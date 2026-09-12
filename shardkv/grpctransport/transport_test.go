package grpctransport_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"dsys/shardkv/grpctransport"
)

// fakeHandler is a minimal stand-in for shardkv.Server's HandlePullShard,
// exercising Register/PullShard without needing a real shardkv.Server.
type fakeHandler struct {
	haveIt   bool
	snapshot []byte
}

func (f *fakeHandler) HandlePullShard(_ int64, _ int) ([]byte, bool) {
	if !f.haveIt {
		return nil, false
	}
	return f.snapshot, true
}

func startServer(t *testing.T, h grpctransport.PullShardHandler) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
	grpctransport.Register(gs, h)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func TestPullShardRoundTripsLargeSnapshot(t *testing.T) {
	snap := make([]byte, 2<<20) // ~2MB
	for i := range snap {
		snap[i] = byte(i)
	}
	addr := startServer(t, &fakeHandler{haveIt: true, snapshot: snap})

	f := grpctransport.NewFetcher()
	t.Cleanup(func() { _ = f.Close() })

	got, ok, err := f.PullShard(context.Background(), []string{addr}, 7, 3)
	if err != nil {
		t.Fatalf("PullShard: unexpected err %v", err)
	}
	if !ok {
		t.Fatalf("PullShard: ok=false, want true")
	}
	if !bytes.Equal(got, snap) {
		t.Fatalf("PullShard: snapshot mismatch, got %d bytes want %d bytes", len(got), len(snap))
	}
}

func TestPullShardMovesToSecondAddressWhenFirstLacksIt(t *testing.T) {
	snap := []byte("shard data from the real leader")
	addr1 := startServer(t, &fakeHandler{haveIt: false}) // e.g. a follower
	addr2 := startServer(t, &fakeHandler{haveIt: true, snapshot: snap})

	f := grpctransport.NewFetcher()
	t.Cleanup(func() { _ = f.Close() })

	got, ok, err := f.PullShard(context.Background(), []string{addr1, addr2}, 1, 0)
	if err != nil {
		t.Fatalf("PullShard: unexpected err %v", err)
	}
	if !ok {
		t.Fatalf("PullShard: ok=false, want true (should have moved to second address)")
	}
	if !bytes.Equal(got, snap) {
		t.Fatalf("PullShard: snapshot mismatch, got %q want %q", got, snap)
	}
}

func TestPullShardAllUnreachableGivesOkFalseNilErr(t *testing.T) {
	const timeout = 50 * time.Millisecond
	f := grpctransport.NewFetcher(grpctransport.WithTimeout(timeout))
	t.Cleanup(func() { _ = f.Close() })

	// Nothing listens on these; dialing lazily either fails fast or the RPC
	// itself is bounded by the per-address timeout.
	addrs := []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}

	start := time.Now()
	snap, ok, err := f.PullShard(context.Background(), addrs, 1, 0)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("PullShard: expected nil err (this is the normal not-ready case), got %v", err)
	}
	if ok {
		t.Fatalf("PullShard: ok=true, want false")
	}
	if snap != nil {
		t.Fatalf("PullShard: expected nil snapshot, got %d bytes", len(snap))
	}
	// Generous upper bound: N addresses * timeout, plus slack for scheduling.
	maxElapsed := time.Duration(len(addrs))*timeout + 2*time.Second
	if elapsed > maxElapsed {
		t.Fatalf("PullShard took %v, want at most roughly %v", elapsed, maxElapsed)
	}
}
