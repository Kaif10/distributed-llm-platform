package raft

// ReadIndex: linearizable reads without writing to the log (Raft thesis
// §6.4). Until this existed every Get was proposed as a log entry and paid a
// full replicated, fsynced round, which is what made reads cost the same as
// writes on the gateway's hot path.
//
// The protocol, and why each step is needed:
//
//  1. The leader records readIndex = commitIndex. Anything a client could
//     have observed as written is at or below it.
//  2. That is only safe if the leader has committed an entry from its
//     CURRENT term: a freshly elected leader knows its log holds every
//     committed entry (Leader Completeness) but does not yet know which of
//     them are committed. Real implementations commit a no-op at the start
//     of each term to settle this; this one instead reports
//     ErrReadIndexNotReady and lets the caller fall back to a log read,
//     which is always correct and only matters for the first moments of a
//     term.
//  3. The leader confirms it is STILL the leader by hearing from a majority
//     in a heartbeat round that began after the read arrived. Without this,
//     a deposed leader in a minority partition would serve stale data while
//     a new leader accepts writes elsewhere.
//  4. The caller waits until its state machine has applied readIndex, then
//     reads locally.

import (
	"context"
	"errors"
)

// ErrNotLeader means this peer cannot serve a ReadIndex read: it is not the
// leader, or it lost leadership while confirming.
var ErrNotLeader = errors.New("raft: not leader")

// ErrReadIndexNotReady means the leader has not yet committed an entry in its
// current term, so it cannot know the commit index is final (step 2 above).
// Fall back to a read through the log.
var ErrReadIndexNotReady = errors.New("raft: no entry committed in this term yet")

// ReadIndex returns an index such that, once the caller's state machine has
// applied it, a local read is linearizable. It blocks for one heartbeat
// round trip to a majority, or until ctx ends.
func (rf *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader || rf.killed() {
		return 0, ErrNotLeader
	}
	if rf.termAt(rf.commitIndex) != rf.currentTerm {
		return 0, ErrReadIndexNotReady
	}
	readIndex, term := rf.commitIndex, rf.currentTerm

	// Step 3: start a fresh heartbeat round and wait for a majority of
	// replies from it. Any reply in our term proves that peer has not moved
	// to a newer term, success or not (a log mismatch still acknowledges
	// our leadership).
	rf.hbRound++
	round := rf.hbRound
	rf.broadcast()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			rf.mu.Lock()
			rf.readCond.Broadcast()
			rf.mu.Unlock()
		case <-stop:
		}
	}()

	for {
		if rf.role != leader || rf.currentTerm != term || rf.killed() {
			return 0, ErrNotLeader
		}
		acks := 1 // ourselves
		for i := range rf.peers {
			if i != rf.me && rf.ackRound[i] >= round {
				acks++
			}
		}
		if acks > len(rf.peers)/2 {
			return readIndex, nil
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		rf.readCond.Wait()
	}
}

// noteAck records that peer answered an AppendEntries sent during heartbeat
// round `round` of the current term. Caller holds rf.mu.
func (rf *Raft) noteAck(peer int, round uint64) {
	if round > rf.ackRound[peer] {
		rf.ackRound[peer] = round
		rf.readCond.Broadcast()
	}
}
