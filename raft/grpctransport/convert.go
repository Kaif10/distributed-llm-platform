package grpctransport

import (
	raftv1 "dsys/gen/raft/v1"
	"dsys/raft"
)

// Conversions between the Go structs in package raft and the generated
// protobuf messages. All functions are nil-safe: a nil input yields a nil
// output. Byte slices are passed through (protobuf marshalling copies them on
// the wire anyway); the Entries slice is always freshly allocated so the two
// representations never alias each other's backing array.

func entriesToProto(in []raft.Entry) []*raftv1.Entry {
	if in == nil {
		return nil
	}
	out := make([]*raftv1.Entry, len(in))
	for i := range in {
		out[i] = &raftv1.Entry{Term: in[i].Term, Command: in[i].Command}
	}
	return out
}

func entriesFromProto(in []*raftv1.Entry) []raft.Entry {
	if in == nil {
		return nil
	}
	out := make([]raft.Entry, len(in))
	for i, e := range in {
		if e == nil {
			continue
		}
		out[i] = raft.Entry{Term: e.GetTerm(), Command: e.GetCommand()}
	}
	return out
}

func requestVoteArgsToProto(a *raft.RequestVoteArgs) *raftv1.RequestVoteRequest {
	if a == nil {
		return nil
	}
	return &raftv1.RequestVoteRequest{
		Term:         a.Term,
		CandidateId:  int32(a.CandidateID),
		LastLogIndex: a.LastLogIndex,
		LastLogTerm:  a.LastLogTerm,
	}
}

func requestVoteArgsFromProto(p *raftv1.RequestVoteRequest) *raft.RequestVoteArgs {
	if p == nil {
		return nil
	}
	return &raft.RequestVoteArgs{
		Term:         p.GetTerm(),
		CandidateID:  int(p.GetCandidateId()),
		LastLogIndex: p.GetLastLogIndex(),
		LastLogTerm:  p.GetLastLogTerm(),
	}
}

func requestVoteReplyToProto(r *raft.RequestVoteReply) *raftv1.RequestVoteResponse {
	if r == nil {
		return nil
	}
	return &raftv1.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}
}

func requestVoteReplyFromProto(p *raftv1.RequestVoteResponse) *raft.RequestVoteReply {
	if p == nil {
		return nil
	}
	return &raft.RequestVoteReply{Term: p.GetTerm(), VoteGranted: p.GetVoteGranted()}
}

func appendEntriesArgsToProto(a *raft.AppendEntriesArgs) *raftv1.AppendEntriesRequest {
	if a == nil {
		return nil
	}
	return &raftv1.AppendEntriesRequest{
		Term:         a.Term,
		LeaderId:     int32(a.LeaderID),
		PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm:  a.PrevLogTerm,
		Entries:      entriesToProto(a.Entries),
		LeaderCommit: a.LeaderCommit,
	}
}

func appendEntriesArgsFromProto(p *raftv1.AppendEntriesRequest) *raft.AppendEntriesArgs {
	if p == nil {
		return nil
	}
	return &raft.AppendEntriesArgs{
		Term:         p.GetTerm(),
		LeaderID:     int(p.GetLeaderId()),
		PrevLogIndex: p.GetPrevLogIndex(),
		PrevLogTerm:  p.GetPrevLogTerm(),
		Entries:      entriesFromProto(p.GetEntries()),
		LeaderCommit: p.GetLeaderCommit(),
	}
}

func appendEntriesReplyToProto(r *raft.AppendEntriesReply) *raftv1.AppendEntriesResponse {
	if r == nil {
		return nil
	}
	return &raftv1.AppendEntriesResponse{
		Term:          r.Term,
		Success:       r.Success,
		ConflictTerm:  r.ConflictTerm,
		ConflictIndex: r.ConflictIndex,
	}
}

func appendEntriesReplyFromProto(p *raftv1.AppendEntriesResponse) *raft.AppendEntriesReply {
	if p == nil {
		return nil
	}
	return &raft.AppendEntriesReply{
		Term:          p.GetTerm(),
		Success:       p.GetSuccess(),
		ConflictTerm:  p.GetConflictTerm(),
		ConflictIndex: p.GetConflictIndex(),
	}
}

func installSnapshotArgsToProto(a *raft.InstallSnapshotArgs) *raftv1.InstallSnapshotRequest {
	if a == nil {
		return nil
	}
	return &raftv1.InstallSnapshotRequest{
		Term:              a.Term,
		LeaderId:          int32(a.LeaderID),
		LastIncludedIndex: a.LastIncludedIndex,
		LastIncludedTerm:  a.LastIncludedTerm,
		Data:              a.Data,
	}
}

func installSnapshotArgsFromProto(p *raftv1.InstallSnapshotRequest) *raft.InstallSnapshotArgs {
	if p == nil {
		return nil
	}
	return &raft.InstallSnapshotArgs{
		Term:              p.GetTerm(),
		LeaderID:          int(p.GetLeaderId()),
		LastIncludedIndex: p.GetLastIncludedIndex(),
		LastIncludedTerm:  p.GetLastIncludedTerm(),
		Data:              p.GetData(),
	}
}

func installSnapshotReplyToProto(r *raft.InstallSnapshotReply) *raftv1.InstallSnapshotResponse {
	if r == nil {
		return nil
	}
	return &raftv1.InstallSnapshotResponse{Term: r.Term}
}

func installSnapshotReplyFromProto(p *raftv1.InstallSnapshotResponse) *raft.InstallSnapshotReply {
	if p == nil {
		return nil
	}
	return &raft.InstallSnapshotReply{Term: p.GetTerm()}
}
