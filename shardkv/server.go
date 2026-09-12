// Server is one replica of one shard-owning Raft group. See the package doc
// in api.go for the overall design; this file is the implementation.
//
// # Reading guide
//
// This is raftkv.Server (Phase 2) with two things added: a notion of which
// shards this group currently owns, and a background loop that keeps that
// notion in sync with the controller and pulls in shard data when ownership
// changes. Everything about proposing an op, waiting for it to commit, and
// checking the term still matches on delivery is unchanged from Phase 2 —
// re-read kv/raftkv/server.go's package doc first if any of that is fuzzy.
//
// The new correctness property, on top of everything Phase 2 gave you, is:
//
//	a client operation on a key is applied by AT MOST ONE group at a time,
//	and only by the group the controller says currently owns that key's
//	shard AND that has finished receiving that shard's data.
//
// That property is why config changes and migrations go through the SAME
// Raft log as client operations, in the same total order, instead of being
// handled out of band: "was this write accepted before or after the shard
// moved" needs one unambiguous answer, and the log is what provides it.
package shardkv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/store"
	"dsys/raft"
	"dsys/shard"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNotLeader     = errors.New("shardkv: not leader")
	ErrLeaderChanged = errors.New("shardkv: leader changed before commit")
	ErrTimeout       = errors.New("shardkv: timed out waiting for commit")
	ErrShutdown      = errors.New("shardkv: shutting down")
	// ErrWrongGroup means this group does not own the key's shard under the
	// configuration that was current when the operation was (or would have
	// been) applied. The client must re-query the controller and retry
	// against whichever group owns the shard now.
	ErrWrongGroup = errors.New("shardkv: wrong group for this key's shard")
	// ErrNotReady means this group owns the shard under the current
	// configuration but has not finished receiving its data yet. The client
	// should retry shortly; no other group has been told to serve it either,
	// so the write is not lost, just temporarily undeliverable.
	ErrNotReady = errors.New("shardkv: shard not yet migrated in, retry shortly")
)

// Options configures a Server. Not to be confused with Config, which is a
// shard *assignment* (see api.go) — Options is construction-time plumbing.
type Options struct {
	GID           int64
	Addrs         []string // this group's own replica addresses, for leader hints
	Ctrl          Controller
	Fetcher       ShardFetcher
	PollInterval  time.Duration // default 100ms
	MaxRaftState  int           // <=0 disables snapshots
	CommitTimeout time.Duration
	Raft          raft.Config
}

// neededShard records that we are supposed to own a shard under the current
// config but have not yet received its data.
type neededShard struct {
	FromGID   int64
	FromAddrs []string
	AtConfig  int64 // the config number to ask the source group for
}

type waiter struct {
	term uint64
	ch   chan outcome
}

type outcome struct {
	r   store.Result
	err error
}

// Server is one replica.
type Server struct {
	kvv1.UnimplementedKVServer

	me   int
	gid  int64
	rf   *raft.Raft
	opts Options

	persister raft.Persister
	applyCh   chan raft.ApplyMsg
	done      chan struct{}

	mu          sync.Mutex
	cur         Config
	machines    map[int]*store.Machine   // shards we currently own and hold data for
	needed      map[int]neededShard      // shards we own under cur but have not received
	outgoing    map[int]map[int64][]byte // shard -> config num it left us -> frozen snapshot, for PullShard
	lastApplied uint64
	waiters     map[uint64]*waiter
}

// New creates a replica and its Raft peer. peers[me] is unused.
func New(peers []raft.Peer, me int, persister raft.Persister, opts Options) *Server {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 100 * time.Millisecond
	}
	if opts.CommitTimeout <= 0 {
		opts.CommitTimeout = 3 * time.Second
	}
	s := &Server{
		me:        me,
		gid:       opts.GID,
		opts:      opts,
		persister: persister,
		applyCh:   make(chan raft.ApplyMsg, 64),
		done:      make(chan struct{}),
		machines:  make(map[int]*store.Machine),
		needed:    make(map[int]neededShard),
		outgoing:  make(map[int]map[int64][]byte),
		waiters:   make(map[uint64]*waiter),
	}
	s.rf = raft.New(peers, me, persister, s.applyCh, opts.Raft)
	go s.applyLoop()
	go s.pollLoop()
	return s
}

// Raft exposes the underlying peer (tests, admin).
func (s *Server) Raft() *raft.Raft { return s.rf }

// GID returns this replica's group id.
func (s *Server) GID() int64 { return s.gid }

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

// IsLeader reports whether this replica currently believes it leads its group.
func (s *Server) IsLeader() bool {
	_, isLeader := s.rf.GetState()
	return isLeader
}

// CurrentConfig returns the last configuration this replica has applied.
// Test/debug helper.
func (s *Server) CurrentConfig() Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// ---------------------------------------------------------------------------
// The apply loop
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
				if err := s.restoreSnapshot(msg.Snapshot); err != nil {
					panic(fmt.Sprintf("shardkv: restore snapshot: %v", err))
				}
				s.lastApplied = msg.SnapshotIndex
			}

		case msg.CommandValid:
			if msg.CommandIndex <= s.lastApplied {
				break // duplicate delivery after a snapshot; already applied
			}
			cmd, err := decodeCommand(msg.Command)
			if err != nil {
				panic(fmt.Sprintf("shardkv: bad command at %d: %v", msg.CommandIndex, err))
			}
			var o outcome
			switch cmd.Kind {
			case opKV:
				o = s.applyKV(cmd)
			case opConfig:
				s.applyConfig(cmd.Config)
			case opMigration:
				s.applyMigration(cmd)
			}
			s.lastApplied = msg.CommandIndex

			if w, ok := s.waiters[msg.CommandIndex]; ok {
				delete(s.waiters, msg.CommandIndex)
				if w.term == msg.CommandTerm {
					w.ch <- o
				} else {
					w.ch <- outcome{err: ErrLeaderChanged}
				}
			}
			s.maybeSnapshot(msg.CommandIndex)
		}
		s.mu.Unlock()
	}
}

// applyKV applies a client operation, or rejects it if this group no longer
// (or not yet) actually serves the key's shard. Caller holds s.mu.
//
// This check is what makes ownership itself linearizable: it happens at the
// same point in the log, under the same lock, as the config transitions
// that change ownership, so every replica reaches the same accept/reject
// decision for the same log position.
func (s *Server) applyKV(cmd *command) outcome {
	if s.cur.Owner(cmd.Shard) != s.gid {
		return outcome{err: ErrWrongGroup}
	}
	if _, pending := s.needed[cmd.Shard]; pending {
		return outcome{err: ErrNotReady}
	}
	m := s.machines[cmd.Shard]
	if m == nil {
		// Should not happen if cur/needed bookkeeping is right, but fail
		// safely rather than panic on a client-visible path.
		return outcome{err: ErrNotReady}
	}
	var e kvv1.LogEntry
	if err := proto.Unmarshal(cmd.KVBytes, &e); err != nil {
		panic(fmt.Sprintf("shardkv: bad kv entry: %v", err))
	}
	return outcome{r: m.Apply(&e)}
}

// applyConfig moves cur forward by exactly one configuration, recomputing
// which shards are gained or lost as a pure function of (cur, next, gid).
// It is idempotent: a config entry for a number we have already passed is
// ignored, so a duplicate delivery (e.g. after a leadership change re-logs
// a leader's own proposal) is harmless, the same lesson Phase 1 taught
// about client retries applied to the group's own internal operations.
// Caller holds s.mu.
func (s *Server) applyConfig(next Config) {
	if next.Num != s.cur.Num+1 {
		return
	}
	for i := 0; i < shard.NShards; i++ {
		oldOwner, newOwner := s.cur.Owner(i), next.Owner(i)
		switch {
		case oldOwner == s.gid && newOwner != s.gid:
			// Losing shard i. Freeze its state for whoever asks, keyed by
			// the config number it left us under: that is exactly the
			// config number the new owner will request it "as of".
			snap, err := s.machines[i].Snapshot()
			if err != nil {
				panic(fmt.Sprintf("shardkv: snapshot outgoing shard %d: %v", i, err))
			}
			if s.outgoing[i] == nil {
				s.outgoing[i] = make(map[int64][]byte)
			}
			s.outgoing[i][next.Num] = snap
			delete(s.machines, i)

		case oldOwner != s.gid && newOwner == s.gid:
			// Gaining shard i. If nobody owned it before (a fresh shard
			// under the very first real config), there is nothing to pull;
			// otherwise it must be fetched from whoever had it, using THAT
			// group's address as recorded in the OLD config, since the new
			// config's Groups map is the only thing that might not even
			// list a departing group any more.
			if oldOwner == 0 {
				s.machines[i] = store.NewMachine()
			} else {
				s.needed[i] = neededShard{FromGID: oldOwner, FromAddrs: s.cur.Groups[oldOwner], AtConfig: next.Num}
			}
		}
	}
	s.cur = next
}

// applyMigration installs a shard pulled from its previous owner. It is
// idempotent for the same reason applyConfig is: only accepted if the shard
// is still marked needed at exactly this config number; a duplicate (or a
// migration for a shard we no longer need because a later config already
// moved it elsewhere) is a no-op. Caller holds s.mu.
func (s *Server) applyMigration(cmd *command) {
	need, ok := s.needed[cmd.MigShard]
	if !ok || need.AtConfig != cmd.MigConfigNum {
		return
	}
	m := store.NewMachine()
	if err := m.Restore(cmd.MigMachineSnapshot); err != nil {
		panic(fmt.Sprintf("shardkv: restore migrated shard %d: %v", cmd.MigShard, err))
	}
	s.machines[cmd.MigShard] = m
	delete(s.needed, cmd.MigShard)
}

// maybeSnapshot compacts when Raft's log has grown past the threshold.
// Caller holds s.mu.
func (s *Server) maybeSnapshot(index uint64) {
	if s.opts.MaxRaftState <= 0 || s.persister.StateSize() < s.opts.MaxRaftState {
		return
	}
	snap, err := s.snapshotLocked()
	if err != nil {
		panic(fmt.Sprintf("shardkv: snapshot: %v", err))
	}
	s.rf.Snapshot(index, snap)
}

// ---------------------------------------------------------------------------
// Proposing
// ---------------------------------------------------------------------------

// propose runs one entry through consensus and returns its result.
func (s *Server) propose(ctx context.Context, cmd *command) (store.Result, error) {
	if cmd.Kind == opKV {
		// Cheap rejections that never touch the log: if we already know we
		// do not serve this shard, or already have this exact request's
		// answer cached, there is no reason to pay for a round of
		// consensus. Both checks are re-done, authoritatively, at apply
		// time, because the world can change between here and there.
		s.mu.Lock()
		if s.cur.Owner(cmd.Shard) != s.gid {
			s.mu.Unlock()
			return store.Result{}, ErrWrongGroup
		}
		if _, pending := s.needed[cmd.Shard]; pending {
			s.mu.Unlock()
			return store.Result{}, ErrNotReady
		}
		var e kvv1.LogEntry
		_ = proto.Unmarshal(cmd.KVBytes, &e)
		if r, done, err := s.machines[cmd.Shard].Dedup(e.Meta); done || err != nil {
			s.mu.Unlock()
			return r, err
		}
		s.mu.Unlock()
	}

	index, term, isLeader := s.rf.Start(encodeCommand(cmd))
	if !isLeader {
		return store.Result{}, ErrNotLeader
	}

	w := &waiter{term: term, ch: make(chan outcome, 1)}
	s.mu.Lock()
	if old, ok := s.waiters[index]; ok {
		old.ch <- outcome{err: ErrLeaderChanged}
	}
	s.waiters[index] = w
	s.mu.Unlock()

	timer := time.NewTimer(s.opts.CommitTimeout)
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

// proposeInternal is used by the poll loop for opConfig/opMigration entries.
// It does not wait for commit: the poll loop re-derives what is still
// needed on its next tick and simply retries, which is safe because both
// apply paths are idempotent. This mirrors the group-commit lesson from
// Phase 1 (retry with the same logical request; let apply-time dedupe) and
// avoids a slow or lost commit blocking the whole poll loop.
func (s *Server) proposeInternal(cmd *command) {
	s.rf.Start(encodeCommand(cmd))
}

// ---------------------------------------------------------------------------
// The poll loop: only the leader acts. It keeps this group's applied
// configuration moving forward, one step at a time, never requesting
// configuration N+1 from the controller until every shard config N handed
// this group has actually arrived. That ordering is what guarantees a
// group's log always contains, for each config number, a config entry
// followed eventually by every migration entry it depends on, before the
// next config entry — the same "one thing at a time, in order" discipline
// that makes the rest of Raft's log reasoning apply here too.
// ---------------------------------------------------------------------------

func (s *Server) pollLoop() {
	t := time.NewTicker(s.opts.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		if !s.IsLeader() {
			continue
		}

		s.mu.Lock()
		needed := make(map[int]neededShard, len(s.needed))
		for k, v := range s.needed {
			needed[k] = v
		}
		curNum := s.cur.Num
		s.mu.Unlock()

		if len(needed) > 0 {
			s.pullNeeded(needed)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.opts.PollInterval)
		next, err := s.opts.Ctrl.Query(ctx, curNum+1)
		cancel()
		if err != nil || next.Num != curNum+1 {
			continue // controller unreachable, or no newer config yet
		}
		s.proposeInternal(&command{Kind: opConfig, Config: next})
	}
}

func (s *Server) pullNeeded(needed map[int]neededShard) {
	for shardID, need := range needed {
		ctx, cancel := context.WithTimeout(context.Background(), s.opts.PollInterval)
		snap, ok, err := s.opts.Fetcher.PullShard(ctx, need.FromAddrs, need.AtConfig, shardID)
		cancel()
		if err != nil || !ok {
			continue // source not ready or unreachable; next tick retries
		}
		s.proposeInternal(&command{
			Kind:               opMigration,
			MigShard:           shardID,
			MigConfigNum:       need.AtConfig,
			MigMachineSnapshot: snap,
		})
	}
}

// ---------------------------------------------------------------------------
// Migration service: the read side other groups pull from.
// ---------------------------------------------------------------------------

// HandlePullShard answers another group asking for a shard we used to own.
// Only a leader that has itself applied the transition can answer: it is
// the frozen snapshot taken exactly at that transition, keyed by the config
// number the caller is moving into, that makes this safe to hand out
// without any further coordination.
func (s *Server) HandlePullShard(configNum int64, shardID int) (snapshot []byte, ok bool) {
	if !s.IsLeader() {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	byNum, exists := s.outgoing[shardID]
	if !exists {
		return nil, false
	}
	snap, ok := byNum[configNum]
	return snap, ok
}

// ---------------------------------------------------------------------------
// Whole-server snapshot: cur config, every owned shard's machine, and the
// bookkeeping for in-flight migrations, so a restart or InstallSnapshot
// leaves a replica exactly where it was, including mid-migration.
// ---------------------------------------------------------------------------

type serverSnapshot struct {
	Cur      Config
	Machines map[int][]byte
	Needed   map[int]neededShard
	Outgoing map[int]map[int64][]byte
}

func (s *Server) snapshotLocked() ([]byte, error) {
	ss := serverSnapshot{Cur: s.cur, Needed: s.needed, Outgoing: s.outgoing, Machines: make(map[int][]byte, len(s.machines))}
	for shardID, m := range s.machines {
		b, err := m.Snapshot()
		if err != nil {
			return nil, err
		}
		ss.Machines[shardID] = b
	}
	return encodeSnapshot(&ss)
}

func (s *Server) restoreSnapshot(b []byte) error {
	ss, err := decodeSnapshot(b)
	if err != nil {
		return err
	}
	machines := make(map[int]*store.Machine, len(ss.Machines))
	for shardID, mb := range ss.Machines {
		m := store.NewMachine()
		if err := m.Restore(mb); err != nil {
			return err
		}
		machines[shardID] = m
	}
	s.cur = ss.Cur
	s.machines = machines
	s.needed = ss.Needed
	if s.needed == nil {
		s.needed = make(map[int]neededShard)
	}
	s.outgoing = ss.Outgoing
	if s.outgoing == nil {
		s.outgoing = make(map[int]map[int64][]byte)
	}
	return nil
}

// ---------------------------------------------------------------------------
// gRPC surface: same kvv1.KVServer any raftkv client already speaks.
// ---------------------------------------------------------------------------

func (s *Server) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	_, err := s.proposeKV(ctx, req.Key, &kvv1.LogEntry{Op: kvv1.Op_OP_PUT, Key: req.Key, Value: req.Value, Meta: req.Meta})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.PutResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	r, err := s.proposeKV(ctx, req.Key, &kvv1.LogEntry{Op: kvv1.Op_OP_GET, Key: req.Key})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.GetResponse{Value: r.Value, Found: r.Found}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	r, err := s.proposeKV(ctx, req.Key, &kvv1.LogEntry{Op: kvv1.Op_OP_DELETE, Key: req.Key, Meta: req.Meta})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.DeleteResponse{Existed: r.Existed}, nil
}

func (s *Server) CompareAndSwap(ctx context.Context, req *kvv1.CASRequest) (*kvv1.CASResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	r, err := s.proposeKV(ctx, req.Key, &kvv1.LogEntry{
		Op: kvv1.Op_OP_CAS, Key: req.Key, Expected: req.Expected,
		ExpectAbsent: req.ExpectAbsent, Value: req.Value, Meta: req.Meta,
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &kvv1.CASResponse{Swapped: r.Swapped, Current: r.Current}, nil
}

func (s *Server) proposeKV(ctx context.Context, key string, e *kvv1.LogEntry) (store.Result, error) {
	payload, err := proto.Marshal(e)
	if err != nil {
		return store.Result{}, err
	}
	return s.propose(ctx, &command{Kind: opKV, KVBytes: payload, Shard: shard.Key2Shard(key)})
}

// toStatus maps errors to gRPC codes. Every code here means "retry, either
// elsewhere (ErrNotLeader/ErrWrongGroup: re-resolve who owns this) or here
// shortly (ErrNotReady/ErrTimeout/ErrLeaderChanged: same owner, not yet)".
func (s *Server) toStatus(err error) error {
	switch {
	case errors.Is(err, ErrNotLeader):
		if hint := s.rf.LeaderHint(); hint >= 0 && hint < len(s.opts.Addrs) && hint != s.me {
			return status.Errorf(codes.Unavailable, "not leader leader=%s", s.opts.Addrs[hint])
		}
		return status.Error(codes.Unavailable, "not leader")
	case errors.Is(err, ErrWrongGroup):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrLeaderChanged), errors.Is(err, ErrTimeout), errors.Is(err, ErrShutdown), errors.Is(err, ErrNotReady):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, store.ErrStaleRequest):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
