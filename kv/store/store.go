// Package store is a durable, single-node key-value store: an in-memory map
// whose every mutation is first written to a write-ahead log.
//
// # The core pattern (remember this, it is Raft's state machine too)
//
//	1. Serialize the command (a LogEntry).
//	2. Append it to the log and wait until it is durable.
//	3. Apply it to the in-memory state.
//	4. Reply to the client.
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
	"bytes"
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
	// mu guards data and sessions. It is held only for in-memory work:
	// reads, the dedup fast path, and apply. Never across an fsync.
	mu   sync.RWMutex
	data map[string][]byte

	// sessions is the deduplication table: for each client, the highest
	// request id applied so far and the result it produced. A retry with the
	// same id gets that result back without being applied again.
	//
	// It lives in memory only, and that is enough: it is updated inside
	// apply, and apply runs on replay, so a restart rebuilds it from the log
	// exactly like it rebuilds data.
	//
	// Assumption: one outstanding request per client at a time, so ids
	// arrive in increasing order and remembering only the latest suffices.
	// This is the same assumption the MIT 6.5840 labs make. Relaxing it
	// means remembering a window of ids per client, and bounding it.
	sessions map[string]session

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

// session is what we remember per client.
type session struct {
	lastID     uint64
	lastResult result
}

// pending is one write waiting for the committer.
type pending struct {
	entry   *kvv1.LogEntry
	payload []byte
	done    chan outcome
}

type outcome struct {
	r   result
	err error
}

// result is what applying an entry produces. It is returned to the caller
// and remembered per client so a retried request gets the same answer.
type result struct {
	existed bool   // Delete
	swapped bool   // CAS
	current []byte // CAS
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
		data:     make(map[string][]byte),
		sessions: make(map[string]session),
		log:      w,
		queue:    make(chan *pending, maxBatch),
	}

	// Recovery is just "apply every logged command again".
	err = w.Replay(func(payload []byte) error {
		var e kvv1.LogEntry
		if err := proto.Unmarshal(payload, &e); err != nil {
			return fmt.Errorf("store: decode log entry: %w", err)
		}
		s.apply(&e)
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
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	// Return a copy so callers cannot mutate our state behind the lock.
	return append([]byte(nil), v...), true
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
	return r.existed, nil
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
	return r.swapped, r.current, nil
}

// commit is the write path: hand the entry to the committer and wait.
func (s *Store) commit(e *kvv1.LogEntry) (result, error) {
	payload, err := proto.Marshal(e)
	if err != nil {
		return result{}, err
	}

	// Fast path: a retry of something already applied never touches the
	// disk. apply would catch it too; this just saves the write.
	s.mu.RLock()
	r, done, err := s.dedup(e.Meta)
	s.mu.RUnlock()
	if done || err != nil {
		return r, err
	}

	p := &pending{entry: e, payload: payload, done: make(chan outcome, 1)}

	s.qmu.RLock()
	if s.closed {
		s.qmu.RUnlock()
		return result{}, ErrClosed
	}
	s.queue <- p // may block briefly if the committer is behind; that is backpressure
	s.qmu.RUnlock()

	o := <-p.done
	return o.r, o.err
}

// committer is the only goroutine that writes to the log or calls apply on
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
			p.done <- outcome{r: s.apply(p.entry)}
		}
		s.mu.Unlock()
	}
}

// dedup reports whether the request described by meta has already been
// applied (done=true, with its original result) or is stale (err != nil).
// Requests without meta are always applied; that is the caller opting out
// of exactly-once. Caller holds mu (read or write).
func (s *Store) dedup(meta *kvv1.RequestMeta) (r result, done bool, err error) {
	if meta == nil || meta.ClientId == "" {
		return result{}, false, nil
	}
	sess, ok := s.sessions[meta.ClientId]
	if !ok {
		return result{}, false, nil
	}
	switch {
	case meta.RequestId == sess.lastID:
		return sess.lastResult, true, nil
	case meta.RequestId < sess.lastID:
		return result{}, false, ErrStaleRequest
	}
	return result{}, false, nil
}

// apply mutates in-memory state. Caller holds mu. Must be deterministic.
//
// apply is itself idempotent with respect to RequestMeta: applying the same
// (client, request) twice mutates once. That makes the log tolerant of
// duplicate entries, which matters because a duplicate can land in the same
// batch as its original (both passed the fast path before either was
// applied), and again in Raft, where a leader change can cause a client's
// retry to be logged a second time.
func (s *Store) apply(e *kvv1.LogEntry) result {
	if r, done, err := s.dedup(e.Meta); done || err != nil {
		// Stale ids are also skipped here: on replay there is nobody to
		// return an error to, and skipping is the only deterministic choice.
		return r
	}
	r := s.applyOp(e)
	if e.Meta != nil && e.Meta.ClientId != "" {
		s.sessions[e.Meta.ClientId] = session{lastID: e.Meta.RequestId, lastResult: r}
	}
	return r
}

// applyOp performs the state change for one entry. Caller holds mu.
func (s *Store) applyOp(e *kvv1.LogEntry) result {
	switch e.Op {
	case kvv1.Op_OP_PUT:
		s.data[e.Key] = e.Value
		return result{}
	case kvv1.Op_OP_DELETE:
		_, existed := s.data[e.Key]
		delete(s.data, e.Key)
		return result{existed: existed}
	case kvv1.Op_OP_CAS:
		// This runs both live and on replay, and must reach the same decision
		// both times. It does, because the decision depends only on
		// (current state, entry), and replay reproduces both in order.
		cur, present := s.data[e.Key]
		var matches bool
		if e.ExpectAbsent {
			matches = !present
		} else {
			matches = present && bytes.Equal(cur, e.Expected)
		}
		// Report what was there at decision time, copied so the caller
		// cannot alias our map. On a failed CAS this lets a client retry
		// with the right expectation instead of doing a separate Get.
		res := result{}
		if present {
			res.current = append([]byte(nil), cur...)
		}
		if matches {
			s.data[e.Key] = e.Value
			res.swapped = true
		}
		return res
	default:
		// An unknown op in the log means a newer binary wrote it. Crashing
		// loudly is better than silently skipping a write.
		panic(fmt.Sprintf("store: unknown op %v in log", e.Op))
	}
}

// Len returns the number of live keys. Test helper.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
