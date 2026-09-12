package raft

// Leader election: section 5.2 of the paper, plus the log-comparison rule
// from 5.4.1 that makes elections safe.

// startElection converts to candidate and solicits votes. Requires rf.mu.
//
// Figure 2, "Candidates": on conversion to candidate, start election:
// increment currentTerm, vote for self, reset election timer, send
// RequestVote RPCs to all other servers.
func (rf *Raft) startElection() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.persist()
	rf.resetElectionTimer()
	rf.logf("-> candidate")

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	votes := 1 // our own
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply, ok := rf.peers[peer].RequestVote(args)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Rule: anything with a higher term demotes us at once.
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.persist()
				return
			}
			// Stale reply: the election it belongs to is over. Without this
			// check, votes from term 5 could elect us in term 7.
			if rf.role != candidate || rf.currentTerm != args.Term {
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes++
			// A majority of the full cluster, counting ourselves. With n=3
			// that is 2; with n=5 it is 3. Two majorities always overlap in
			// at least one server, which is why two leaders in one term are
			// impossible: that server voted at most once.
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}(i)
	}
}

// HandleRequestVote answers a candidate. Figure 2, "RequestVote RPC":
//
//  1. Reply false if term < currentTerm.
//  2. If votedFor is null or candidateId, and candidate's log is at least
//     as up-to-date as receiver's log, grant vote.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &RequestVoteReply{}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return reply
	}
	if args.Term > rf.currentTerm {
		// A newer term exists. Whoever we were (even leader), we are now a
		// follower with no vote cast in this new term.
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	reply.Term = rf.currentTerm

	// Section 5.4.1, election restriction: we only vote for a candidate
	// whose log is at least as up-to-date as ours. "Up-to-date" compares
	// the last entries: higher term wins; equal terms, longer log wins.
	// This is the rule that guarantees a new leader already holds every
	// committed entry, so leaders never have to "catch up" from followers.
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist() // the vote is a promise; it must survive a crash
		// Granting a vote resets our timer: we have just endorsed someone,
		// give them time to win before we run ourselves. Note we do NOT
		// reset when refusing: a candidate with a stale log should not be
		// able to suppress our own election indefinitely.
		rf.resetElectionTimer()
		reply.VoteGranted = true
		rf.logf("vote -> n%d", args.CandidateID)
	}
	return reply
}
