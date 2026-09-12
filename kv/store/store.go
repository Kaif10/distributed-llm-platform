// Package store is a durable, single-node key-value store: an in-memory map
// whose every mutation is first written to a write-ahead log.
//
// # The core pattern (remember this, it is Raft's state machine too)
//
//  1. Serialize the command (a LogEntry).
//  2. Append it to the log and wait until it is durable.
//  3. Apply it to the in-memory state.
//  4. Reply to the client.
//
// On restart: replay the log, applying every entry in order, and you are back
// exactly where you were. For that to be true, apply must be deterministic:
// the same entry applied to the same state must always produce the same
// result. No wall-clock reads, no randomness, no map iteration order inside
// apply.
//
// In Phase 2 the only change is that step 2 becomes "get a majority of
// replicas to durably log it" instead of "fsync locally".
//
// The package is split along that seam. Machine (machine.go) is step 3 on
// its own: the deterministic in-memory state plus snapshot/restore, with no
// locks or I/O. Store (this file) is steps 1, 2 and 4 around it: the WAL,
// the committer, and the locking. Raft replaces Store and keeps Machine.
//
// # Group commit
//
// Step 2 is expensive (an fsync is on the order of a millisecond) and step 3
// is cheap (a map write). If every writer did both under one lock, throughput
// would be capped at roughly one write per fsync. Instead, writers hand their
// entries to a single committer goroutine. It takes everything that has
// queued up, writes it with ONE fsync, then applies the entries in order.
//
// Batches form naturally: while one fsync is in flight, new requests queue
// behind it and become the next batch. Under low load a batch is one entry
// and latency is unchanged; under high load batches grow and throughput
// climbs toward the disk's bandwidth rather than its fsync rate.
//
// The single committer also gives us the invariant we need for free:
//
//	log order == apply order,
//
// because the same goroutine does both, in the same sequence. No lock has to
// be held across the disk write any more, so readers are never blocked by
// I/O either.
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/wal"
)

var (
	// ErrClosed is returned for writes after Close.
	ErrClosed = errors.New("store: closed")
	// ErrStaleRequest is returned for a request id lower than one this client
	// has already had applied. Under the one-outstanding-request assumption
	// that can only be a client bug, so we refuse rather than guess.
	ErrStaleRequest = errors.New("store: request id is older than the last applied for this client")
)

// maxBatch caps how many entries one fsync covers, bounding both memory and
// the latency of the first entry in a batch.
const maxBatch = 512

// Options configures the store.
type Options struct {
	// NoSync passes through to the WAL. Tests only.
	NoSync bool
}

// Stats are cumulative counters, useful for seeing group commit at work.
type Stats struct {
	Batches uint64 // number of fsyncs
	Entries uint64 // number of entries written across all batches
}

// Store is safe for concurrent use.
type Store struct {
	// mu guards m. It is held only for in-memory work: reads, the dedup
	// fast path, and apply. Never across an fsync.
	mu sync.RWMutex
	m  *Machine

	log *wal.WAL

	// Write pipeline. qmu makes "check closed, then enqueue" atomic with
	// respect to Close, so we never send on a closed channel.
	qmu    sync.RWMutex
	closed bool
	queue  chan *pending
	wg     sync.WaitGroup

	batches atomic.Uint64
	entries atomic.Uint64
}

// pending is one write waiting for the committer.
type pending struct {
	entry   *kvv1.LogEntry
	payload []byte
	done    chan outcome
}

type outcome struct {
	r   Result
	err error
}

// Open opens or creates a store in dir, replaying its log.
func Open(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w, err := wal.Open(filepath.Join(dir, "kv.wal"), wal.Options{NoSync: opts.NoSync})
	if err != nil {
		return nil, err
	}
	s := &Store{
		m:     NewMachine(),
		log:   w,
		queue: make(chan *pending, maxBatch),
	}

	// Recovery is just "apply every logged command again". Nobody else has
	// a reference to s yet, so no lock is needed around Apply here.
	err = w.Replay(func(payload []byte) error {
		var e kvv1.LogEntry
		if err := proto.Unmarshal(payload, &e); err != nil {
			return fmt.Errorf("store: decode log entry: %w", err)
		}
		s.m.Apply(&e)
		return nil
	})
	if err != nil {
		_ = w.Close()
		return nil, err
	}

	s.wg.Add(1)
	go s.committer()
	return s, nil
}

// Close stops accepting writes, flushes everything already queued, and
// closes the log. Writes that were accepted before Close still complete.
func (s *Store) Close() error {
	s.qmu.Lock()
	if s.closed {
		s.qmu.Unlock()
		return nil
	}
	s.closed = true
	close(s.queue)
	s.qmu.Unlock()

	s.wg.Wait() // committer drains the queue, then exits
	return s.log.Close()
}

// Stats returns cumulative batching counters.
func (s *Store) Stats() Stats {
	return Stats{Batches: s.batches.Load(), Entries: s.entries.Load()}
}

// Get returns the value for key. Reads never touch the log.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m.Get(key)
}

// Put durably sets key to value.
func (s *Store) Put(key string, value []byte, meta *kvv1.RequestMeta) error {
	_, err := s.commit(&kvv1.LogEntry{Op: kvv1.Op_OP_PUT, Key: key, Value: value, Meta: meta})
	return err
}

// Delete durably removes key and reports whether it existed.
func (s *Store) Delete(key string, meta *kvv1.RequestMeta) (existed bool, err error) {
	r, err := s.commit(&kvv1.LogEntry{Op: kvv1.Op_OP_DELETE, Key: key, Meta: meta})
	if err != nil {
		return false, err
	}
	return r.Existed, nil
}

// CompareAndSwap sets key to value if its current value equals expected
// (or if the key is absent and expectAbsent is set). It reports whether the
// swap happened and the value that was current at decision time.
//
// Note that the comparison is NOT done here. We log the attempt and let
// apply decide, for two reasons:
//
//  1. apply runs under the lock, in log order, so the decision is made
//     against the state that actually existed when the entry took its place
//     in the log. Deciding earlier would open a window where another writer
//     sneaks in between "I compared" and "I logged", which is exactly the
//     race CAS exists to prevent.
//  2. A failed CAS still needs to be remembered for idempotent retries, and
//     the only durable memory we have is the log.
//
// The cost is that a failed CAS occupies a log record. That is a fine trade.
func (s *Store) CompareAndSwap(key string, expected []byte, expectAbsent bool, value []byte, meta *kvv1.RequestMeta) (swapped bool, current []byte, err error) {
	r, err := s.commit(&kvv1.LogEntry{
		Op:           kvv1.Op_OP_CAS,
		Key:          key,
		Expected:     expected,
		ExpectAbsent: expectAbsent,
		Value:        value,
		Meta:         meta,
	})
	if err != nil {
		return false, nil, err
	}
	return r.Swapped, r.Current, nil
}

// commit is the write path: hand the entry to the committer and wait.
func (s *Store) commit(e *kvv1.LogEntry) (Result, error) {
	payload, err := proto.Marshal(e)
	if err != nil {
		return Result{}, err
	}

	// Fast path: a retry of something already applied never touches the
	// disk. Apply would catch it too; this just saves the write.
	s.mu.RLock()
	r, done, err := s.m.Dedup(e.Meta)
	s.mu.RUnlock()
	if done || err != nil {
		return r, err
	}

	p := &pending{entry: e, payload: payload, done: make(chan outcome, 1)}

	s.qmu.RLock()
	if s.closed {
		s.qmu.RUnlock()
		return Result{}, ErrClosed
	}
	s.queue <- p // may block briefly if the committer is behind; that is backpressure
	s.qmu.RUnlock()

	o := <-p.done
	return o.r, o.err
}

// committer is the only goroutine that writes to the log or calls Apply on
// the live path. It runs until Close closes the queue and the queue drains.
func (s *Store) committer() {
	defer s.wg.Done()
	batch := make([]*pending, 0, maxBatch)
	payloads := make([][]byte, 0, maxBatch)

	for first := range s.queue {
		batch = append(batch[:0], first)
		payloads = append(payloads[:0], first.payload)

		// Take everything else already waiting, without blocking. This is
		// the whole batching policy: no timers, no tuning. The batch is
		// however much arrived while the previous fsync was running.
	gather:
		for len(batch) < maxBatch {
			select {
			case p, ok := <-s.queue:
				if !ok {
					break gather
				}
				batch = append(batch, p)
				payloads = append(payloads, p.payload)
			default:
				break gather
			}
		}

		// One fsync for the whole batch.
		err := s.log.AppendBatch(payloads)
		s.batches.Add(1)
		s.entries.Add(uint64(len(batch)))

		// Apply in log order, then release each waiter. Holding mu here is
		// cheap: it is pure memory work.
		s.mu.Lock()
		for _, p := range batch {
			if err != nil {
				p.done <- outcome{err: err}
				continue
			}
			p.done <- outcome{r: s.m.Apply(p.entry)}
		}
		s.mu.Unlock()
	}
}

// Len returns the number of live keys. Test helper.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m.Len()
}
