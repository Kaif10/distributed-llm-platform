package raft

// This file holds the Raft struct, its lifecycle, log bookkeeping,
// persistence, and the applier. Elections are in election.go, log
// replication in replication.go, snapshots in snapshot.go.
//
// # Reading guide
//
// Everything in Raft hinges on one idea: a single, totally ordered log that
// a majority agrees on. Terms are a logical clock; each term has at most one
// leader; only the leader appends; followers copy. Every rule in Figure 2 of
// the paper exists to protect one of two properties:
//
//   - Election Safety: at most one leader per term.
//   - State Machine Safety: if a server has applied entry i, no other server
//     will ever apply a different entry i.
//
// # Concurrency model
//
// One mutex, rf.mu, protects all fields. The rules:
//
//   1. Never hold rf.mu while doing I/O you do not control: an RPC to a peer,
//      or a send on applyCh. Both can block indefinitely, and the other side
//      may need rf.mu to make progress. Deadlock.
//   2. After every unlock/relock around an RPC, re-check that the world is
//      still the one you sent the RPC in: same term, same role. Replies from
//      a past life are ignored.
//   3. Persist before replying to any RPC that changed durable state, and
//      before returning from Start. If we crash after replying but before
//      persisting, we may have promised something (a vote, an entry) that we
//      then forget, and the safety proofs no longer hold.

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	default:
		return "leader"
	}
}

// Raft is one peer in a Raft cluster.
type Raft struct {
	mu        sync.Mutex
	peers     []Peer
	persister Persister
	me        int
	cfg       Config
	applyCh   chan<- ApplyMsg
	dead      atomic.Bool
	done      chan struct{} // closed by Kill

	// ---- Persistent state (Figure 2). Updated on stable storage before
	// responding to RPCs. ----
	currentTerm uint64
	votedFor    int // -1 = none
	// log[0] is a sentinel standing in for the last entry covered by the
	// snapshot: its Term is lastIncludedTerm and it sits at absolute index
	// lastIncludedIndex. Real entries follow. Absolute index i lives at
	// log[i - lastIncludedIndex]. With no snapshot, the sentinel is index 0
	// term 0, which is exactly the paper's "log starts at index 1".
	log               []Entry
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshot          []byte

	// ---- Volatile state on all servers ----
	role             role
	leaderID         int
	commitIndex      uint64
	lastApplied      uint64
	electionDeadline time.Time

	// ---- Volatile state on leaders, reinitialised after election ----
	nextIndex  []uint64
	matchIndex []uint64

	// applyCond wakes the applier when commitIndex advances or a snapshot
	// is waiting. pendingSnapshot is a snapshot that must be delivered on
	// applyCh before any further commands.
	applyCond       *sync.Cond
	pendingSnapshot *ApplyMsg

	// trigger[i] nudges peer i's replicator to send now rather than at the
	// next heartbeat. Buffered(1) so many nudges coalesce into one send.
	trigger []chan struct{}
}

// New creates a Raft peer. peers[me] is unused. The peer starts as a
// follower, restores any persisted state, and starts its background
// goroutines. Committed entries are delivered on applyCh.
func New(peers []Peer, me int, persister Persister, applyCh chan<- ApplyMsg, cfg Config) *Raft {
	def := DefaultConfig()
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = def.HeartbeatInterval
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = def.ElectionTimeoutMin
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin * 2
	}

	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		cfg:        cfg,
		applyCh:    applyCh,
		done:       make(chan struct{}),
		votedFor:   -1,
		leaderID:   -1,
		log:        []Entry{{Term: 0}}, // sentinel at index 0
		role:       follower,
		nextIndex:  make([]uint64, len(peers)),
		matchIndex: make([]uint64, len(peers)),
		trigger:    make([]chan struct{}, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	for i := range rf.trigger {
		rf.trigger[i] = make(chan struct{}, 1)
	}

	rf.readPersist(persister.ReadState(), persister.ReadSnapshot())
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.applier()
	for i := range peers {
		if i != me {
			go rf.replicator(i)
		}
	}
	return rf
}

// GetState returns the current term and whether this peer believes it is
// the leader. "Believes" matters: a partitioned leader keeps believing until
// it hears a higher term.
func (rf *Raft) GetState() (uint64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// LeaderHint returns the id of the peer this node last heard from as leader,
// or -1 if unknown. It is a hint for redirecting clients, not a guarantee:
// the information may be stale the moment it is returned.
func (rf *Raft) LeaderHint() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
}

// Start proposes a command. If this peer is the leader it appends the
// command to its log, begins replicating it, and returns the index it will
// have if committed, the current term, and true. Otherwise it returns false
// and the caller must find the leader.
//
// The command is not committed when Start returns. Watch applyCh.
func (rf *Raft) Start(command []byte) (uint64, uint64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader || rf.killed() {
		return 0, rf.currentTerm, false
	}
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	index := rf.lastIndex()
	rf.matchIndex[rf.me] = index
	rf.persist()
	rf.logf("start idx=%d term=%d", index, rf.currentTerm)
	// Replicate immediately instead of waiting for the next heartbeat.
	// Commit latency becomes one round trip instead of up to one heartbeat
	// interval plus one round trip.
	rf.broadcast()
	return index, rf.currentTerm, true
}

// Kill stops the peer. Background goroutines exit; RPC handlers still
// respond (a killed peer is indistinguishable from a slow one to others,
// and tests rely on being able to restart from the persister).
func (rf *Raft) Kill() {
	if rf.dead.CompareAndSwap(false, true) {
		close(rf.done)
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	}
}

// Killed reports whether Kill has been called.
func (rf *Raft) Killed() bool { return rf.dead.Load() }

func (rf *Raft) killed() bool { return rf.dead.Load() }

func (rf *Raft) logf(format string, args ...any) {
	if rf.cfg.Logf != nil {
		rf.cfg.Logf("[n%d t%d %s] "+format, append([]any{rf.me, rf.currentTerm, rf.role}, args...)...)
	}
}

// ---------------------------------------------------------------------------
// Log accessors. All take absolute indices and require rf.mu.
// ---------------------------------------------------------------------------

func (rf *Raft) lastIndex() uint64 { return rf.lastIncludedIndex + uint64(len(rf.log)-1) }

func (rf *Raft) lastTerm() uint64 { return rf.log[len(rf.log)-1].Term }

// termAt returns the term of the entry at absolute index i, which must be in
// [lastIncludedIndex, lastIndex].
func (rf *Raft) termAt(i uint64) uint64 { return rf.log[i-rf.lastIncludedIndex].Term }

// entriesFrom returns a copy of entries with absolute index >= i.
func (rf *Raft) entriesFrom(i uint64) []Entry {
	src := rf.log[i-rf.lastIncludedIndex:]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// truncateFrom drops every entry with absolute index >= i.
func (rf *Raft) truncateFrom(i uint64) {
	rf.log = rf.log[:i-rf.lastIncludedIndex]
}

// ---------------------------------------------------------------------------
// Role transitions. Require rf.mu.
// ---------------------------------------------------------------------------

// becomeFollower is called whenever we learn of a term at least as new as
// ours from someone with authority (a leader's AppendEntries, or any RPC
// carrying a higher term). Rule from Figure 2, "All Servers": if RPC request
// or response contains term T > currentTerm, set currentTerm = T and convert
// to follower.
func (rf *Raft) becomeFollower(term uint64) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.leaderID = -1
	}
	if rf.role != follower {
		rf.logf("-> follower (term %d)", term)
	}
	rf.role = follower
}

func (rf *Raft) becomeLeader() {
	rf.role = leader
	rf.leaderID = rf.me
	// Figure 2, "Volatile state on leaders": nextIndex starts optimistic at
	// lastIndex+1 and is walked back by rejections; matchIndex starts
	// pessimistic at 0 and is only advanced by confirmed replication.
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastIndex()
	rf.logf("-> LEADER")
	// Announce ourselves at once so followers stop timing out.
	rf.broadcast()
}

// broadcast nudges every replicator to send now.
func (rf *Raft) broadcast() {
	for i := range rf.peers {
		if i != rf.me {
			rf.signal(i)
		}
	}
}

func (rf *Raft) signal(peer int) {
	select {
	case rf.trigger[peer] <- struct{}{}:
	default: // already pending
	}
}

// ---------------------------------------------------------------------------
// Election timer
// ---------------------------------------------------------------------------

// resetElectionTimer picks a fresh random deadline. Randomisation is what
// breaks split votes (section 5.2): if all timeouts were equal, candidates
// would keep tying forever.
func (rf *Raft) resetElectionTimer() {
	span := rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin
	rf.electionDeadline = time.Now().Add(rf.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(span))))
}

// ticker fires elections. A leader never times out: it is the one sending
// heartbeats, not waiting for them.
func (rf *Raft) ticker() {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-t.C:
		}
		rf.mu.Lock()
		if rf.role != leader && time.Now().After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

// persistentState is what survives a crash. Compare with Figure 2: exactly
// currentTerm, votedFor, log[], plus the snapshot boundary. commitIndex and
// lastApplied are deliberately NOT here: they are re-derived after restart
// from the leader's commit index, and the service re-applies from the
// snapshot. Persisting them would be an optimisation, not a requirement.
type persistentState struct {
	CurrentTerm       uint64
	VotedFor          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Log               []Entry
}

// persist saves durable state. Requires rf.mu. Called after every change to
// currentTerm, votedFor, or the log, before any RPC reply or Start return
// that reveals the change. The snapshot is saved alongside so the pair is
// always consistent.
func (rf *Raft) persist() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Log:               rf.log,
	}); err != nil {
		panic("raft: encode state: " + err.Error())
	}
	if err := rf.persister.Save(buf.Bytes(), rf.snapshot); err != nil {
		// Losing the ability to persist means we can no longer make
		// promises. Crashing is the safe choice.
		panic("raft: persist: " + err.Error())
	}
}

func (rf *Raft) readPersist(state, snapshot []byte) {
	if len(state) == 0 {
		return
	}
	var ps persistentState
	if err := gob.NewDecoder(bytes.NewReader(state)).Decode(&ps); err != nil {
		panic("raft: decode persisted state: " + err.Error())
	}
	rf.currentTerm = ps.CurrentTerm
	rf.votedFor = ps.VotedFor
	rf.lastIncludedIndex = ps.LastIncludedIndex
	rf.lastIncludedTerm = ps.LastIncludedTerm
	rf.log = ps.Log
	rf.snapshot = snapshot
	// Everything in the snapshot is by definition committed and applied.
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	if len(snapshot) > 0 {
		// The service restarted with empty state; hand it the snapshot
		// before any commands.
		rf.pendingSnapshot = &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotIndex: rf.lastIncludedIndex,
			SnapshotTerm:  rf.lastIncludedTerm,
		}
	}
}

// ---------------------------------------------------------------------------
// Applier
// ---------------------------------------------------------------------------

// applier is the only goroutine that sends on applyCh. It delivers a pending
// snapshot first, then committed-but-unapplied entries in index order. It
// never holds rf.mu while sending: the service may call back into Raft
// (Snapshot, Start) from its apply loop.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.pendingSnapshot == nil && rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if snap := rf.pendingSnapshot; snap != nil {
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			select {
			case rf.applyCh <- *snap:
			case <-rf.done:
				return
			}
			continue
		}

		// Batch everything committed so far. lastApplied is advanced before
		// we release the lock so a concurrent commit does not re-deliver.
		start := rf.lastApplied + 1
		end := rf.commitIndex
		if start <= rf.lastIncludedIndex {
			// A snapshot overtook us; those entries are in it.
			start = rf.lastIncludedIndex + 1
		}
		var msgs []ApplyMsg
		for i := start; i <= end; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastIncludedIndex].Command,
				CommandIndex: i,
				CommandTerm:  rf.termAt(i),
			})
		}
		rf.lastApplied = end
		rf.mu.Unlock()

		for _, m := range msgs {
			select {
			case rf.applyCh <- m:
			case <-rf.done:
				return
			}
		}
	}
}
