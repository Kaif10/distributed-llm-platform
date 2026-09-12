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
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/wal"
)

// ErrNotImplemented marks the parts of Phase 1 left as exercises.
var ErrNotImplemented = errors.New("store: not implemented yet (see docs/phase1.md exercises)")

// Options configures the store.
type Options struct {
	// NoSync passes through to the WAL. Tests only.
	NoSync bool
}

// Store is safe for concurrent use.
type Store struct {
	// mu guards everything below. It is held across the WAL append on the
	// write path, which serializes writers. That is deliberate:
	//
	//   log order MUST equal apply order,
	//
	// otherwise a replay after a crash could apply Put(k,1),Put(k,2) in a
	// different order than the live server did and diverge. Holding one lock
	// across both steps is the simplest way to guarantee it. The cost is
	// that throughput is bounded by fsync latency (roughly 1 write per
	// fsync). Group commit fixes that; it is the stretch exercise.
	mu   sync.RWMutex
	data map[string][]byte
	log  *wal.WAL
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
	s := &Store{data: make(map[string][]byte), log: w}

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
	return s, nil
}

// Close flushes and closes the log.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
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
// EXERCISE 1: implement this. See docs/phase1.md.
func (s *Store) CompareAndSwap(key string, expected []byte, expectAbsent bool, value []byte, meta *kvv1.RequestMeta) (swapped bool, current []byte, err error) {
	return false, nil, ErrNotImplemented
}

// result is what applying an entry produces. It is returned to the caller
// and, once you implement deduplication, remembered per client so a retried
// request gets the same answer without being applied again.
type result struct {
	existed bool   // Delete
	swapped bool   // CAS
	current []byte // CAS
}

// commit is the write path: log, then apply, under one lock.
func (s *Store) commit(e *kvv1.LogEntry) (result, error) {
	payload, err := proto.Marshal(e)
	if err != nil {
		return result{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// EXERCISE 2 goes here: if e.Meta identifies a request this client has
	// already had applied, return the remembered result instead of logging
	// and applying again.

	if err := s.log.Append(payload); err != nil {
		return result{}, err
	}
	return s.apply(e), nil
}

// apply mutates in-memory state. Caller holds mu. Must be deterministic.
func (s *Store) apply(e *kvv1.LogEntry) result {
	switch e.Op {
	case kvv1.Op_OP_PUT:
		s.data[e.Key] = e.Value
		return result{}
	case kvv1.Op_OP_DELETE:
		_, existed := s.data[e.Key]
		delete(s.data, e.Key)
		return result{existed: existed}
	case kvv1.Op_OP_CAS:
		// EXERCISE 1: apply the CAS here. Remember: this runs on replay too,
		// and must reach the same decision it reached the first time.
		return result{}
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
