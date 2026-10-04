// Package raftkv is the replicated key-value service: the Phase 1 state
// machine (store.Machine) driven by Raft instead of a local write-ahead log.
//
// # The shape
//
//	client ──gRPC──▶ Server.Put ──▶ rf.Start(entry) ──▶ ... Raft replicates ...
//	                                                             │
//	                        applyLoop ◀── applyCh ◀── committed ─┘
//	                            │
//	                     m.Apply(entry) ──▶ wake the waiting Put ──▶ reply
//
// Compare with Phase 1's store.commit: there the entry went to a committer
// goroutine that fsynced it and then applied it. Here the entry goes to the
// Raft leader, which replicates it to a majority and then delivers it back
// on applyCh, and only THEN do we apply it and answer the client. The
// "log then apply" discipline is identical; the log is just shared.
//
// # Why every request, including reads, goes through the log
//
// A follower's state machine can lag. If Get read local state, a client
// could write on the leader, then read on a follower and not see its own
// write. Logging the Get orders it against every write in the single log,
// which is what makes it linearizable. The cost is a round of consensus per
// read. The paper's section 8 describes cheaper options (ReadIndex, leader
// leases); they are the Phase 2 stretch exercise.
//
// # Why the waiter checks the term
//
// Start returns an index, but a leader that loses leadership may have its
// uncommitted entries overwritten by the new leader's entries at the same
// indices. So when index i is applied we compare the applied entry's term
// with the term Start gave us. Same index, different term: our entry is
// gone and the client must retry (with the same RequestMeta, so the retry
// is deduplicated if the original did in fact commit somewhere).
package raftkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/store"
	"dsys/raft"
)

var (
	// ErrNotLeader means this replica cannot accept writes. The client should
	// try another replica.
	ErrNotLeader = errors.New("raftkv: not leader")
	// ErrLeaderChanged means the entry was proposed but leadership changed
	// before it committed; it may or may not have been applied. Safe to
	// retry with the same RequestMeta.
	ErrLeaderChanged = errors.New("raftkv: leader changed before commit")
	// ErrTimeout means the entry did not commit within the wait budget.
	// Usually a partition. Safe to retry with the same RequestMeta.
	ErrTimeout = errors.New("raftkv: timed out waiting for commit")
	// ErrShutdown means the server is stopping.
	ErrShutdown = errors.New("raftkv: shutting down")
)

// Config for a replicated KV server.
type Config struct {
	// MaxRaftState triggers a snapshot when Raft's persisted state exceeds
	// this many bytes. <= 0 disables snapshots.
	MaxRaftState int
	// CommitTimeout bounds how long a request waits to commit before the
	// client is told to retry. Defaults to 3s.
	CommitTimeout time.Duration
	// Addrs, if set, maps peer id to a client-reachable address, used for
	// leader hints in NotLeader errors.
	Addrs []string
	Raft  raft.Config
}

// Server is one replica. It implements kvv1.KVServer.
type Server struct {
	kvv1.UnimplementedKVServer

	me        int
	rf        *raft.Raft
	persister raft.Persister
	cfg       Config
	applyCh   chan raft.ApplyMsg
	done      chan struct{}

	mu          sync.Mutex
	m           *store.Machine
	lastApplied uint64
	waiters     map[uint64]*waiter // by log index
	appliedCond *sync.Cond         // on mu; broadcast whenever lastApplied moves
}

type waiter struct {
	term uint64 // the term Start reported; must match the applied entry
	ch   chan outcome
}

type outcome struct {
	r   store.Result
	err error
}

// New creates a replica and its Raft peer. peers[me] is unused.
func New(peers []raft.Peer, me int, persister raft.Persister, cfg Config) *Server {
	if cfg.CommitTimeout <= 0 {
		cfg.CommitTimeout = 3 * time.Second
	}
	s := &Server{
		me:        me,
		persister: persister,
		cfg:       cfg,
		applyCh:   make(chan raft.ApplyMsg, 64),
		done:      make(chan struct{}),
		m:         store.NewMachine(),
		waiters:   make(map[uint64]*waiter),
	}
	s.appliedCond = sync.NewCond(&s.mu)
	s.rf = raft.New(peers, me, persister, s.applyCh, cfg.Raft)
	go s.applyLoop()
	return s
}

// Raft exposes the underlying peer (tests, admin).
func (s *Server) Raft() *raft.Raft { return s.rf }

// Kill stops the replica.
func (s *Server) Kill() {
	s.rf.Kill()
	s.mu.Lock()
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	for idx, w := range s.waiters {
		w.ch <- outcome{err: ErrShutdown}
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

// LocalGet reads this replica's own state machine without consensus. It is
// NOT linearizable (the replica may be behind) and exists for tests,
// debugging and metrics. Client reads go through Get.
func (s *Server) LocalGet(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.Get(key)
}

// LastApplied returns the highest log index applied to this replica.
func (s *Server) LastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

// IsLeader reports whether this replica currently believes it leads.
func (s *Server) IsLeader() bool {
	_, isLeader := s.rf.GetState()
	return isLeader
}

// ---------------------------------------------------------------------------
// The apply loop: the only goroutine that touches the Machine's state.
// ---------------------------------------------------------------------------

func (s *Server) applyLoop() {
	for {
		var msg raft.ApplyMsg
		select {
		case <-s.done:
			return
		case msg = <-s.applyCh:
		}

		s.mu.Lock()
		switch {
		case msg.SnapshotValid:
			// Raft says: your state is now this snapshot, as of SnapshotIndex.
			// Everything we applied so far is superseded.
			if msg.SnapshotIndex > s.lastApplied {
				if err := s.m.Restore(msg.Snapshot); err != nil {
					panic(fmt.Sprintf("raftkv: restore snapshot: %v", err))
				}
				s.lastApplied = msg.SnapshotIndex
				s.appliedCond.Broadcast()
			}

		case msg.CommandValid:
			if msg.CommandIndex <= s.lastApplied {
				break // duplicate delivery after a snapshot; already applied
			}
			var e kvv1.LogEntry
			if err := proto.Unmarshal(msg.Command, &e); err != nil {
				panic(fmt.Sprintf("raftkv: bad log entry at %d: %v", msg.CommandIndex, err))
			}
			// Machine.Apply is idempotent per RequestMeta, so a client's
			// retry that got logged twice (leader change mid-flight) is
			// applied once. Phase 1 built that property for exactly this.
			// ApplyChecked: a stale entry is skipped, and its waiter is
			// told so instead of being handed an empty "success".
			r, applyErr := s.m.ApplyChecked(&e)
			s.lastApplied = msg.CommandIndex
			s.appliedCond.Broadcast()

			if w, ok := s.waiters[msg.CommandIndex]; ok {
				delete(s.waiters, msg.CommandIndex)
				if w.term == msg.CommandTerm {
					w.ch <- outcome{r: r, err: applyErr}
				} else {
					w.ch <- outcome{err: ErrLeaderChanged}
				}
			}
			s.maybeSnapshot(msg.CommandIndex)
		}
		s.mu.Unlock()
	}
}

// maybeSnapshot compacts when Raft's log has grown past the threshold.
// Caller holds s.mu, so the machine is quiescent while we serialise it.
func (s *Server) maybeSnapshot(index uint64) {
	if s.cfg.MaxRaftState <= 0 || s.persister.StateSize() < s.cfg.MaxRaftState {
		return
	}
	snap, err := s.m.Snapshot()
	if err != nil {
		panic(fmt.Sprintf("raftkv: snapshot: %v", err))
	}
	// Raft may take its own lock here; it never calls back into us while
	// holding it, so holding s.mu across this call is safe.
	s.rf.Snapshot(index, snap)
}

// ---------------------------------------------------------------------------
// Proposing
// ---------------------------------------------------------------------------

// propose runs one entry through consensus and returns its result.
func (s *Server) propose(ctx context.Context, e *kvv1.LogEntry) (store.Result, error) {
	// Fast path: an already-applied request is answered from memory on any
	// replica; the answer is immutable once applied.
	s.mu.Lock()
	if r, done, err := s.m.Dedup(e.Meta); done || err != nil {
		s.mu.Unlock()
		return r, err
	}
	s.mu.Unlock()

	payload, err := proto.Marshal(e)
	if err != nil {
		return store.Result{}, err
	}
	index, term, isLeader := s.rf.Start(payload)
	if !isLeader {
		return store.Result{}, ErrNotLeader
	}

	w := &waiter{term: term, ch: make(chan outcome, 1)}
	s.mu.Lock()
	if old, ok := s.waiters[index]; ok {
		// Someone else was waiting on this index from an earlier term of
		// leadership. Their entry has been superseded by ours.
		old.ch <- outcome{err: ErrLeaderChanged}
	}
	s.waiters[index] = w
	s.mu.Unlock()

	timer := time.NewTimer(s.cfg.CommitTimeout)
	defer timer.Stop()
	select {
	case o := <-w.ch:
		return o.r, o.err
	case <-timer.C:
		s.abandon(index, w)
		return store.Result{}, ErrTimeout
	case <-ctx.Done():
		s.abandon(index, w)
		return store.Result{}, ctx.Err()
	case <-s.done:
		return store.Result{}, ErrShutdown
	}
}

func (s *Server) abandon(index uint64, w *waiter) {
	s.mu.Lock()
	if s.waiters[index] == w {
		delete(s.waiters, index)
	}
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// gRPC surface
// ---------------------------------------------------------------------------

func (s *Server) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	_, err := s.propose(ctx, &kvv1.LogEntry{Op: kvv1.Op_OP_PUT, Key: req.Key, Value: req.Value, Meta: req.Meta})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.PutResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	v, found, err := s.readIndexGet(ctx, req.Key)
	if errors.Is(err, raft.ErrReadIndexNotReady) {
		// New leader, nothing committed in its term yet: read through the
		// log, which is always correct (see raft/readindex.go step 2).
		r, err := s.propose(ctx, &kvv1.LogEntry{Op: kvv1.Op_OP_GET, Key: req.Key})
		if err != nil {
			return nil, s.toStatus(err)
		}
		return &kvv1.GetResponse{Value: r.Value, Found: r.Found}, nil
	}
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.GetResponse{Value: v, Found: found}, nil
}

// readIndexGet serves a linearizable read without a log entry: confirm
// leadership and a read index with Raft, wait until this replica has applied
// that index, then read the local state machine (Raft thesis §6.4).
func (s *Server) readIndexGet(ctx context.Context, key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CommitTimeout)
	defer cancel()
	idx, err := s.rf.ReadIndex(ctx)
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return nil, false, ErrNotLeader
	case errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil:
		return nil, false, ErrTimeout
	case err != nil:
		return nil, false, err
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() { // wake the wait below if ctx ends first
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.appliedCond.Broadcast()
			s.mu.Unlock()
		case <-stop:
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.lastApplied < idx {
		if ctx.Err() != nil {
			return nil, false, ErrTimeout
		}
		s.appliedCond.Wait()
	}
	v, found := s.m.Get(key)
	return bytes.Clone(v), found, nil // the caller reads it after we unlock
}

func (s *Server) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	r, err := s.propose(ctx, &kvv1.LogEntry{Op: kvv1.Op_OP_DELETE, Key: req.Key, Meta: req.Meta})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.DeleteResponse{Existed: r.Existed}, nil
}

func (s *Server) CompareAndSwap(ctx context.Context, req *kvv1.CASRequest) (*kvv1.CASResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	r, err := s.propose(ctx, &kvv1.LogEntry{
		Op: kvv1.Op_OP_CAS, Key: req.Key, Expected: req.Expected,
		ExpectAbsent: req.ExpectAbsent, Value: req.Value, Meta: req.Meta,
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.CASResponse{Swapped: r.Swapped, Current: r.Current}, nil
}

// toStatus maps errors to gRPC codes. Unavailable means "retry, possibly
// elsewhere, with the same RequestMeta". The leader hint lets a client jump
// straight to the right replica instead of probing.
func (s *Server) toStatus(err error) error {
	switch {
	case errors.Is(err, ErrNotLeader):
		if hint := s.rf.LeaderHint(); hint >= 0 && hint < len(s.cfg.Addrs) && hint != s.me {
			return status.Errorf(codes.Unavailable, "not leader leader=%s", s.cfg.Addrs[hint])
		}
		return status.Error(codes.Unavailable, "not leader")
	case errors.Is(err, ErrLeaderChanged), errors.Is(err, ErrTimeout), errors.Is(err, ErrShutdown):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, store.ErrStaleRequest):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
