// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014).
//
// This file is the public surface: the types a transport, a persister, and a
// state machine need. The algorithm lives in raft.go and its siblings.
//
// # How a service uses it
//
//	applyCh := make(chan raft.ApplyMsg)
//	rf := raft.New(peers, me, persister, applyCh, raft.DefaultConfig())
//	idx, term, ok := rf.Start(cmd)   // propose; ok=false means "not leader"
//	for m := range applyCh {         // committed commands arrive here, in order
//	    if m.CommandValid { stateMachine.Apply(m.Command) }
//	    if m.SnapshotValid { stateMachine.Restore(m.Snapshot) }
//	}
//
// Start returning ok=true does NOT mean the command is committed. It means
// the leader has appended it and will try. The command is committed only
// when it arrives on applyCh. A leader that loses leadership may never
// commit it, and the service must be prepared to see a different command at
// that index.
package raft

import (
	"log"
	"sync"
	"time"
)

// ApplyMsg is delivered on the apply channel. Exactly one of CommandValid or
// SnapshotValid is set. Messages arrive in log order; after a snapshot
// message, the next command message has index SnapshotIndex+1.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Entry is one log entry. Its index is implicit from its position.
type Entry struct {
	Term    uint64
	Command []byte
}

// RequestVoteArgs: Figure 2 of the paper.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply: Figure 2.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs: Figure 2. Empty Entries is a heartbeat.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesReply: Figure 2 plus the accelerated log backtracking hints
// from section 5.3 ("the protocol can be optimized to reduce the number of
// rejected AppendEntries RPCs"). On Success=false:
//
//   - ConflictTerm is the term of the follower's entry at PrevLogIndex, or 0
//     if the follower has no entry there.
//   - ConflictIndex is the first index the follower holds for ConflictTerm,
//     or, when ConflictTerm is 0, the follower's log length + 1.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs: Figure 13, without chunking (snapshots are sent whole).
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotReply: Figure 13.
type InstallSnapshotReply struct {
	Term uint64
}

// Peer is the outbound half of a transport: how this node sends an RPC to
// one specific other node. Each call blocks until a reply arrives or the
// transport gives up. ok=false means no reply was received; Raft treats
// that as a lost message and will retry on its own schedule. A transport
// must never return ok=true with a reply it did not receive.
//
// Raft holds no lock while calling these, and may call them concurrently.
type Peer interface {
	RequestVote(args *RequestVoteArgs) (reply *RequestVoteReply, ok bool)
	AppendEntries(args *AppendEntriesArgs) (reply *AppendEntriesReply, ok bool)
	InstallSnapshot(args *InstallSnapshotArgs) (reply *InstallSnapshotReply, ok bool)
}

// Handler is the inbound half: what a transport calls when an RPC arrives
// for this node. *Raft implements Handler. Handlers are safe to call
// concurrently and always return a reply.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}

// Persister stores Raft's durable state (currentTerm, votedFor, log) and the
// service's latest snapshot. Save must be atomic: after a crash, ReadState
// and ReadSnapshot return either both old values or both new values, never
// a mix, and never a torn write. Raft calls Save before replying to any RPC
// that changed durable state; that is what makes the paper's guarantees hold
// across crashes.
type Persister interface {
	Save(state, snapshot []byte) error
	ReadState() []byte
	ReadSnapshot() []byte
	StateSize() int
}

// Config tunes timing. The paper's requirement is
//
//	broadcastTime << electionTimeout << MTBF
//
// Heartbeats must arrive several times per election timeout or followers
// will start needless elections. The defaults suit tests on one machine;
// production over a real network would use longer values.
type Config struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// Logf receives debug output. nil disables logging.
	Logf func(format string, args ...any)
}

// DefaultConfig returns timing suitable for local testing.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 500 * time.Millisecond,
	}
}

// StdLogger is a Logf that writes to the standard logger.
func StdLogger(format string, args ...any) { log.Printf(format, args...) }

// ---------------------------------------------------------------------------
// MemPersister: in-memory Persister for tests. Copy() lets a test "restart"
// a node with exactly the bytes it had saved, and nothing it had not.
// ---------------------------------------------------------------------------

// MemPersister is an in-memory Persister.
type MemPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

// NewMemPersister returns an empty MemPersister.
func NewMemPersister() *MemPersister { return &MemPersister{} }

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

// Save stores both values atomically.
func (p *MemPersister) Save(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = clone(state)
	p.snapshot = clone(snapshot)
	return nil
}

// ReadState returns a copy of the saved state.
func (p *MemPersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.state)
}

// ReadSnapshot returns a copy of the saved snapshot.
func (p *MemPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

// StateSize returns the size of the saved state in bytes.
func (p *MemPersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

// Copy returns an independent persister with the same contents.
func (p *MemPersister) Copy() *MemPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemPersister{state: clone(p.state), snapshot: clone(p.snapshot)}
}
