package raft

// Log replication: section 5.3, the commit rule from 5.4.2, and the
// accelerated backtracking hinted at in 5.3.

import "time"

// replicator is a per-peer goroutine. It sends to its peer whenever nudged
// (new entry, new leadership, a rejection that moved nextIndex) and at every
// heartbeat interval otherwise. One goroutine per peer means a slow peer
// never delays a fast one.
func (rf *Raft) replicator(peer int) {
	timer := time.NewTimer(rf.cfg.HeartbeatInterval)
	defer timer.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-rf.trigger[peer]:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
		timer.Reset(rf.cfg.HeartbeatInterval)

		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			continue
		}
		needSnapshot := rf.nextIndex[peer] <= rf.lastIncludedIndex
		rf.mu.Unlock()

		// Fire and forget. The replicator must NOT wait for the reply: on a
		// network that delays replies by seconds, a blocked replicator would
		// stop heartbeating to this peer, the peer would time out and start
		// an election, and leadership would churn even though nothing is
		// actually down. Several RPCs to one peer may be in flight at once;
		// the reply handlers are written to tolerate arriving out of order.
		if needSnapshot {
			// The entries this peer needs are gone from our log; they are
			// inside the snapshot. Send that instead (section 7).
			go rf.sendInstallSnapshot(peer)
		} else {
			go rf.sendAppendEntries(peer)
		}
	}
}

// sendAppendEntries builds and sends one AppendEntries to peer and handles
// the reply. It takes rf.mu itself and never holds it across the RPC.
func (rf *Raft) sendAppendEntries(peer int) {
	rf.mu.Lock()
	if rf.role != leader || rf.nextIndex[peer] <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	prev := rf.nextIndex[peer] - 1
	round := rf.hbRound // for ReadIndex: which heartbeat round this answers
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		Entries:      rf.appendBatch(prev + 1), // empty on a pure heartbeat; capped otherwise
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply, ok := rf.peers[peer].AppendEntries(args)
	if !ok {
		return // lost; the next heartbeat retries
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
		rf.resetElectionTimer()
		return
	}
	// Ignore replies from a previous term of leadership. We may have lost
	// and regained leadership meanwhile with a different log.
	if rf.role != leader || rf.currentTerm != args.Term {
		return
	}
	// Any reply in our term, success or not, shows this peer still
	// recognises us as leader (ReadIndex step 3).
	rf.noteAck(peer, round)

	if reply.Success {
		// Advance only if this reply is newer than what we already know.
		// Replies can arrive out of order; a stale success must not move
		// matchIndex backwards.
		newMatch := args.PrevLogIndex + uint64(len(args.Entries))
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		if newMatch+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newMatch + 1
		}
		rf.advanceCommitIndex()
		// Batches are capped (appendBatch): if this peer is still behind,
		// send the next batch now rather than at the next heartbeat.
		if rf.nextIndex[peer] <= rf.lastIndex() {
			rf.signal(peer)
		}
		return
	}

	// Rejected on log consistency. With several RPCs in flight, only the
	// reply to the probe we most recently issued should move nextIndex; an
	// older rejection would undo progress a newer reply already made.
	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}
	// The naive fix is nextIndex--, one entry
	// per round trip; with a follower thousands of entries behind that is
	// thousands of round trips. Use the follower's hints to jump:
	//
	//   ConflictTerm == 0: follower's log is too short; jump to its end.
	//   Otherwise: if we have entries of ConflictTerm, jump to just past our
	//   last one (the follower's entries of that term up to there might
	//   match). If we have none, the whole run of that term on the follower
	//   is garbage; jump to where it starts.
	if reply.ConflictTerm == 0 {
		rf.nextIndex[peer] = max(reply.ConflictIndex, 1)
	} else {
		next := reply.ConflictIndex
		for i := rf.lastIndex(); i > rf.lastIncludedIndex; i-- {
			if rf.termAt(i) == reply.ConflictTerm {
				next = i + 1
				break
			}
			if rf.termAt(i) < reply.ConflictTerm {
				break // terms are monotonic in the log; it is not here
			}
		}
		rf.nextIndex[peer] = max(next, 1)
	}
	// Retry right away instead of waiting a heartbeat.
	rf.signal(peer)
}

// advanceCommitIndex applies the leader commit rule. Requires rf.mu.
//
// Figure 2, "Leaders": if there exists an N such that N > commitIndex, a
// majority of matchIndex[i] >= N, and log[N].term == currentTerm, set
// commitIndex = N.
//
// The "log[N].term == currentTerm" clause is the subtle one (section 5.4.2,
// Figure 8). A leader must not conclude an entry from an OLDER term is
// committed just because it is on a majority: that entry could still be
// overwritten by a future leader that never saw it. Entries from older
// terms become committed indirectly, when an entry from the current term
// that follows them is committed (Log Matching then guarantees they are on
// every server that has the newer entry).
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			// Anything older than this is also not of the current term.
			return
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.logf("commit -> %d", n)
			rf.applyCond.Signal()
			// Let followers learn the new commit index promptly.
			rf.broadcast()
			return
		}
	}
}

// HandleAppendEntries is the follower side. Figure 2, "AppendEntries RPC":
//
//  1. Reply false if term < currentTerm.
//  2. Reply false if log doesn't contain an entry at prevLogIndex whose
//     term matches prevLogTerm.
//  3. If an existing entry conflicts with a new one (same index, different
//     terms), delete the existing entry and all that follow it.
//  4. Append any new entries not already in the log.
//  5. If leaderCommit > commitIndex, set
//     commitIndex = min(leaderCommit, index of last new entry).
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &AppendEntriesReply{Term: rf.currentTerm}

	// (1)
	if args.Term < rf.currentTerm {
		return reply
	}
	// Any AppendEntries with our term or newer comes from the legitimate
	// leader of that term (Election Safety). Follow it, and reset the timer:
	// hearing from a live leader is exactly what the timer waits for.
	termChanged := args.Term > rf.currentTerm
	rf.becomeFollower(args.Term)
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	logChanged := false
	defer func() {
		switch {
		case termChanged:
			// A new term is a promise (we will not vote again in it): save
			// synchronously, as before. This also covers any log change.
			rf.persist()
		case logChanged:
			rf.persistLater()
		}
		// Success tells the leader these entries are DURABLE here, and
		// that is what it counts toward commit. Wait for the save that
		// covers them, whichever request started it: entries we merely
		// matched may have been appended by a concurrent AppendEntries
		// whose save has not landed yet. waitDurable releases rf.mu, so
		// concurrent appends coalesce into the same fsync.
		if !reply.Success {
			return
		}
		if !rf.waitDurable(rf.persistSeq) {
			reply.Success = false // killed before the save landed
			return
		}
		// rf.mu was released while waiting. A newer term may have begun and
		// its leader may have truncated the very entries we are about to
		// acknowledge (the old code could not see this: it saved while
		// holding the lock). Acknowledge only if we are still in the same
		// term and still hold the last entry this request covered.
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if rf.currentTerm != args.Term {
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
		if len(args.Entries) > 0 && lastNew > rf.lastIncludedIndex &&
			(lastNew > rf.lastIndex() || rf.termAt(lastNew) != args.Entries[len(args.Entries)-1].Term) {
			reply.Success = false
		}
	}()

	// If the leader's prevLogIndex is inside our snapshot, everything up to
	// lastIncludedIndex is committed and therefore matches the leader's log
	// (Leader Completeness). Skip that part and check from the snapshot
	// boundary instead.
	if args.PrevLogIndex < rf.lastIncludedIndex {
		skip := rf.lastIncludedIndex - args.PrevLogIndex
		if uint64(len(args.Entries)) <= skip {
			reply.Success = true // nothing new for us
			return reply
		}
		args.Entries = args.Entries[skip:]
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
	}

	// (2) with backtracking hints.
	if args.PrevLogIndex > rf.lastIndex() {
		reply.ConflictTerm = 0
		reply.ConflictIndex = rf.lastIndex() + 1
		return reply
	}
	if t := rf.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
		reply.ConflictTerm = t
		// First index of that term in our log.
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex+1 && rf.termAt(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// (3) and (4). Walk the new entries; the first one that disagrees with
	// what we have truncates from there. Entries that already match are
	// kept untouched. This matters: an old, delayed AppendEntries with a
	// short entry list must not chop off newer entries we already hold
	// (the paper's "if an existing entry conflicts", not "if it differs in
	// length").
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx > rf.lastIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
		if rf.termAt(idx) != e.Term {
			rf.truncateFrom(idx)
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
	}

	// (5). "Index of last new entry" is prevLogIndex+len(entries), NOT our
	// lastIndex(): we may hold extra entries beyond what this RPC covered
	// that the leader has not vouched for.
	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		rf.commitIndex = min(args.LeaderCommit, lastNew)
		rf.applyCond.Signal()
	}
	reply.Success = true
	return reply
}
