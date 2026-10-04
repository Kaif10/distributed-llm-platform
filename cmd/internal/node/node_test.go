package node

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftv1 "dsys/gen/raft/v1"
	"dsys/raft"
	"dsys/raft/grpctransport"
)

func TestCheckLayout(t *testing.T) {
	p := []string{"a:1", "b:1", "c:1"}
	c := []string{"a:2", "b:2", "c:2"}
	if err := CheckLayout(1, p, c); err != nil {
		t.Fatalf("valid layout rejected: %v", err)
	}
	for name, tc := range map[string]struct {
		id      int
		peers   []string
		clients []string
	}{
		"length mismatch":   {0, p, c[:2]},
		"id out of range":   {3, p, c},
		"shared address":    {0, p, []string{"a:2", "b:1", "c:2"}},
		"old one-list form": {0, p, p},
		"empty":             {0, nil, nil},
	} {
		if err := CheckLayout(tc.id, tc.peers, tc.clients); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

type nopHandler struct{}

func (nopHandler) HandleRequestVote(a *raft.RequestVoteArgs) *raft.RequestVoteReply {
	return &raft.RequestVoteReply{Term: a.Term, VoteGranted: true}
}
func (nopHandler) HandleAppendEntries(a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return &raft.AppendEntriesReply{Term: a.Term, Success: true}
}
func (nopHandler) HandleInstallSnapshot(a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return &raft.InstallSnapshotReply{Term: a.Term}
}

// The point of the split: the Raft service answers on the peer port and is
// Unimplemented on the client port.
func TestRaftOnlyOnPeerListener(t *testing.T) {
	peerLis, clientLis, err := Listeners("127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peerGS, clientGS := grpc.NewServer(), grpc.NewServer()
	grpctransport.Register(peerGS, nopHandler{})
	done := make(chan error, 1)
	go func() { done <- Serve(peerGS, peerLis, clientGS, clientLis) }()
	t.Cleanup(func() { clientGS.Stop(); peerGS.Stop(); <-done })

	vote := func(addr string) error {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err = raftv1.NewRaftClient(conn).RequestVote(ctx, &raftv1.RequestVoteRequest{Term: 1 << 40})
		return err
	}
	if err := vote(peerLis.Addr().String()); err != nil {
		t.Fatalf("peer port: %v", err)
	}
	if err := vote(clientLis.Addr().String()); status.Code(err) != codes.Unimplemented {
		t.Fatalf("client port: err = %v, want Unimplemented", err)
	}
}
