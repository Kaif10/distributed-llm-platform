// Server wires Machine to Raft, the same "propose an op, wait for it to
// come back on the apply channel, wake the waiter keyed by log index"
// pattern kv/raftkv.Server uses for the data path (see that file's doc
// comment for the full rationale: log-then-apply, the term check on the
// waiter, and why even Query goes through consensus). shardctrl is small
// enough that this file is a close copy of raftkv/server.go with KV's four
// ops swapped for Join/Leave/Move/Query.
package shardctrl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/raft"
)

var (
	// ErrNotLeader means this replica cannot accept requests. The client
	// should try another replica.
	ErrNotLeader = errors.New("shardctrl: not leader")
	// ErrLeaderChanged means the entry was proposed but leadership changed
	// before it committed; it may or may not have been applied. Safe to
	// retry with the same RequestMeta.
	ErrLeaderChanged = errors.New("shardctrl: leader changed before commit")
	// ErrTimeout means the entry did not commit within the wait budget.
	// Usually a partition. Safe to retry with the same RequestMeta.
	ErrTimeout = errors.New("shardctrl: timed out waiting for commit")
	// ErrShutdown means the server is stopping.
	ErrShutdown = errors.New("shardctrl: shutting down")
)

// Config for a shard controller replica.
type Config struct {
	// CommitTimeout bounds how long a request waits to commit before the
	// client is told to retry. Defaults to 3s.
	CommitTimeout time.Duration
	// Addrs, if set, maps peer id to a client-reachable address, used for
	// leader hints in NotLeader errors.
	Addrs []string
	// MaxRaftState is the persisted Raft state size, in bytes, past which
	// the replica snapshots and compacts its log. 0 means
	// DefaultMaxRaftState; negative disables snapshots (tests only: the log
	// then grows with every Query, forever).
	MaxRaftState int
	Raft         raft.Config
}

// DefaultMaxRaftState is the compaction threshold when Config.MaxRaftState
// is 0. The same order as shardkv's default: large enough that compaction
// is rare, small enough that re-persisting the log stays cheap.
const DefaultMaxRaftState = 1 << 20

// Server is one shard-controller replica. It implements
// shardctrlv1.ShardCtrlServer.
//
// # On snapshotting
//
// Server compacts its Raft log the same way raftkv does: after applying an
// entry, if the persisted Raft state has grown past Config.MaxRaftState, it
// hands Raft a Machine snapshot and lets it discard the log prefix.
//
// An earlier version skipped this on the theory that the controller's log
// only grows with rare administrative operations. That theory is wrong
// because Query is logged too (it must be, to be linearizable), and every
// shardkv leader issues one per poll interval for as long as it runs. With
// no compaction the log grew with uptime, and since Raft re-encodes its
// whole log on every persist, every operation, Query included, got slower
// with it until pollers' deadlines could no longer be met and groups
// stopped seeing new configurations. The snapshot itself still carries the
// full config history (see Machine.Snapshot), so it grows only with
// Join/Leave/Move; what compaction bounds is the per-Query log growth.
type Server struct {
	shardctrlv1.UnimplementedShardCtrlServer

	me        int
	rf        *raft.Raft
	persister raft.Persister
	cfg       Config
	applyCh   chan raft.ApplyMsg
	done      chan struct{}

	mu          sync.Mutex
	m           *Machine
	lastApplied uint64
	waiters     map[uint64]*waiter // by log index
}

type waiter struct {
	term uint64 // the term Start reported; must match the applied entry
	ch   chan outcome
}

type outcome struct {
	cfg *shardctrlv1.Config
	err error
}

// New creates a replica and its Raft peer. peers[me] is unused.
func New(peers []raft.Peer, me int, persister raft.Persister, cfg Config) *Server {
	if cfg.CommitTimeout <= 0 {
		cfg.CommitTimeout = 3 * time.Second
	}
	if cfg.MaxRaftState == 0 {
		cfg.MaxRaftState = DefaultMaxRaftState
	}
	s := &Server{
		me:        me,
		persister: persister,
		cfg:       cfg,
		applyCh:   make(chan raft.ApplyMsg, 64),
		done:      make(chan struct{}),
		m:         NewMachine(),
		waiters:   make(map[uint64]*waiter),
	}
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
			if msg.SnapshotIndex > s.lastApplied {
				if err := s.m.Restore(msg.Snapshot); err != nil {
					panic(fmt.Sprintf("shardctrl: restore snapshot: %v", err))
				}
				s.lastApplied = msg.SnapshotIndex
			}

		case msg.CommandValid:
			if msg.CommandIndex <= s.lastApplied {
				break // duplicate delivery after a snapshot; already applied
			}
			var e shardctrlv1.LogEntry
			if err := proto.Unmarshal(msg.Command, &e); err != nil {
				panic(fmt.Sprintf("shardctrl: bad log entry at %d: %v", msg.CommandIndex, err))
			}
			cfg, err := s.m.Apply(commandFromProto(&e))
			s.lastApplied = msg.CommandIndex

			if w, ok := s.waiters[msg.CommandIndex]; ok {
				delete(s.waiters, msg.CommandIndex)
				if w.term == msg.CommandTerm {
					w.ch <- outcome{cfg: cfg, err: err}
				} else {
					w.ch <- outcome{err: ErrLeaderChanged}
				}
			}
			s.maybeSnapshot(msg.CommandIndex)
		}
		s.mu.Unlock()
	}
}

// maybeSnapshot compacts when Raft's persisted state has grown past the
// threshold. Caller holds s.mu.
func (s *Server) maybeSnapshot(index uint64) {
	if s.cfg.MaxRaftState <= 0 || s.persister.StateSize() < s.cfg.MaxRaftState {
		return
	}
	snap, err := s.m.Snapshot()
	if err != nil {
		panic(fmt.Sprintf("shardctrl: snapshot: %v", err))
	}
	// Raft may take its own lock here; it never calls back into us while
	// holding it, so holding s.mu across this call is safe (as in raftkv).
	s.rf.Snapshot(index, snap)
}

// ---------------------------------------------------------------------------
// Proposing
// ---------------------------------------------------------------------------

// propose runs one entry through consensus and returns its result.
func (s *Server) propose(ctx context.Context, e *shardctrlv1.LogEntry) (*shardctrlv1.Config, error) {
	// Fast path: an already-applied request is answered from memory on any
	// replica; the answer is immutable once applied.
	s.mu.Lock()
	if cfg, done, err := s.m.Dedup(e.Meta); done || err != nil {
		s.mu.Unlock()
		return cfg, err
	}
	s.mu.Unlock()

	payload, err := proto.Marshal(e)
	if err != nil {
		return nil, err
	}
	index, term, isLeader := s.rf.Start(payload)
	if !isLeader {
		return nil, ErrNotLeader
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
		return o.cfg, o.err
	case <-timer.C:
		s.abandon(index, w)
		return nil, ErrTimeout
	case <-ctx.Done():
		s.abandon(index, w)
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrShutdown
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

func (s *Server) Join(ctx context.Context, req *shardctrlv1.JoinRequest) (*shardctrlv1.JoinResponse, error) {
	_, err := s.propose(ctx, &shardctrlv1.LogEntry{Op: shardctrlv1.LogOp_LOG_OP_JOIN, Meta: req.Meta, Groups: req.Groups})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &shardctrlv1.JoinResponse{}, nil
}

func (s *Server) Leave(ctx context.Context, req *shardctrlv1.LeaveRequest) (*shardctrlv1.LeaveResponse, error) {
	_, err := s.propose(ctx, &shardctrlv1.LogEntry{Op: shardctrlv1.LogOp_LOG_OP_LEAVE, Meta: req.Meta, Gids: req.Gids})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &shardctrlv1.LeaveResponse{}, nil
}

func (s *Server) Move(ctx context.Context, req *shardctrlv1.MoveRequest) (*shardctrlv1.MoveResponse, error) {
	_, err := s.propose(ctx, &shardctrlv1.LogEntry{
		Op: shardctrlv1.LogOp_LOG_OP_MOVE, Meta: req.Meta, Shard: req.Shard, Gid: req.Gid,
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &shardctrlv1.MoveResponse{}, nil
}

func (s *Server) Query(ctx context.Context, req *shardctrlv1.QueryRequest) (*shardctrlv1.QueryResponse, error) {
	cfg, err := s.propose(ctx, &shardctrlv1.LogEntry{Op: shardctrlv1.LogOp_LOG_OP_QUERY, QueryNum: req.Num})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &shardctrlv1.QueryResponse{Config: cfg}, nil
}

// commandFromProto converts the wire LogEntry into the Machine's internal
// Command. Kept separate from Machine so Machine has no proto-wire-format
// dependency beyond the Config/Group types it already returns.
func commandFromProto(e *shardctrlv1.LogEntry) *Command {
	c := &Command{Meta: e.Meta, Groups: e.Groups, Gids: e.Gids, Shard: e.Shard, Gid: e.Gid, QueryNum: e.QueryNum}
	switch e.Op {
	case shardctrlv1.LogOp_LOG_OP_JOIN:
		c.Op = OpJoin
	case shardctrlv1.LogOp_LOG_OP_LEAVE:
		c.Op = OpLeave
	case shardctrlv1.LogOp_LOG_OP_MOVE:
		c.Op = OpMove
	case shardctrlv1.LogOp_LOG_OP_QUERY:
		c.Op = OpQuery
	default:
		panic(fmt.Sprintf("shardctrl: unknown wire op %v in log", e.Op))
	}
	return c
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
	case errors.Is(err, ErrStaleRequest):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrUnknownGroup), errors.Is(err, ErrInvalidShard):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
