package raft

// Log compaction: section 7 of the paper.
//
// Without compaction the log grows forever and a restart replays everything
// since the beginning of time. The service periodically hands Raft a
// snapshot of its state "as of index N"; Raft discards entries <= N. A
// follower that has fallen behind the compaction point can no longer be
// caught up entry by entry, so the leader ships it the snapshot instead.

// Snapshot is called by the service: "my state now reflects every entry
// through index; here it is serialised". Raft drops the log prefix. The
// service must only snapshot indices it has actually applied.
func (rf *Raft) Snapshot(index uint64, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Ignore anything not newer than what we already have compacted, and
	// anything beyond what is committed (the service cannot legitimately
	// have applied it).
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}
	rf.compactTo(index, rf.termAt(index), snapshot)
	rf.persist()
	rf.logf("snapshot through %d, log now %d entries", index, len(rf.log)-1)
}

// compactTo discards log entries <= index, keeping a sentinel for index, and
// installs snapshot as the new base. Requires rf.mu and index in
// (lastIncludedIndex, lastIndex].
func (rf *Raft) compactTo(index, term uint64, snapshot []byte) {
	// Copy the retained suffix into a fresh slice so the old backing array,
	// and every command in the discarded prefix, can be garbage collected.
	suffix := rf.log[index-rf.lastIncludedIndex+1:]
	newLog := make([]Entry, 1, len(suffix)+1)
	newLog[0] = Entry{Term: term} // sentinel
	newLog = append(newLog, suffix...)
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = snapshot
}

// sendInstallSnapshot ships our snapshot to a peer whose nextIndex is
// behind our compaction point. It takes rf.mu itself and never holds it
// across the RPC.
func (rf *Raft) sendInstallSnapshot(peer int) {
	rf.mu.Lock()
	if rf.role != leader {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	reply, ok := rf.peers[peer].InstallSnapshot(args)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
		rf.resetElectionTimer()
		return
	}
	if rf.role != leader || rf.currentTerm != args.Term {
		return
	}
	// The peer now holds everything through LastIncludedIndex.
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	rf.signal(peer) // continue with entries after the snapshot
}

// HandleInstallSnapshot is the follower side. Figure 13, simplified to
// whole snapshots:
//
//  1. Reply immediately if term < currentTerm.
//  6. If existing log entry has same index and term as snapshot's last
//     included entry, retain log entries following it and reply.
//  7. Discard the entire log.
//  8. Reset state machine using snapshot contents.
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &InstallSnapshotReply{Term: rf.currentTerm}

	if args.Term < rf.currentTerm {
		return reply
	}
	termChanged := args.Term > rf.currentTerm
	rf.becomeFollower(args.Term)
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// A snapshot that does not go past our commit index teaches us nothing:
	// we already hold (and may have applied) everything in it. Installing it
	// anyway could move the service's state backwards relative to what it
	// has applied. Ignore it.
	if args.LastIncludedIndex <= rf.commitIndex {
		if termChanged {
			rf.persist()
		}
		return reply
	}

	if args.LastIncludedIndex <= rf.lastIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		// (6) Our log extends past the snapshot and agrees with it at the
		// boundary; keep the tail.
		rf.compactTo(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	} else {
		// (7) Our log is useless: either too short or diverges. Replace it
		// with just the sentinel.
		rf.log = []Entry{{Term: args.LastIncludedTerm}}
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.snapshot = args.Data
	}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()

	// (8) Queue the snapshot for the service. The applier delivers it
	// before any later commands, and the service replaces its state.
	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.applyCond.Signal()
	rf.logf("installed snapshot through %d", args.LastIncludedIndex)
	return reply
}
