package grpctransport

import (
	"bytes"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"dsys/raft"
)

// fakeHandler records the last args of each RPC and replies with term+1 so a
// test can tell the reply really came from the handler.
type fakeHandler struct {
	mu   sync.Mutex
	rv   *raft.RequestVoteArgs
	ae   *raft.AppendEntriesArgs
	is   *raft.InstallSnapshotArgs
	slow time.Duration // if set, every handler sleeps this long first
}

func (f *fakeHandler) HandleRequestVote(a *raft.RequestVoteArgs) *raft.RequestVoteReply {
	time.Sleep(f.slow)
	f.mu.Lock()
	f.rv = a
	f.mu.Unlock()
	return &raft.RequestVoteReply{Term: a.Term + 1, VoteGranted: true}
}

func (f *fakeHandler) HandleAppendEntries(a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	time.Sleep(f.slow)
	f.mu.Lock()
	f.ae = a
	f.mu.Unlock()
	return &raft.AppendEntriesReply{Term: a.Term + 1, Success: false, ConflictTerm: 7, ConflictIndex: 42}
}

func (f *fakeHandler) HandleInstallSnapshot(a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	time.Sleep(f.slow)
	f.mu.Lock()
	f.is = a
	f.mu.Unlock()
	return &raft.InstallSnapshotReply{Term: a.Term + 1}
}

// startServer runs a real gRPC server on a loopback port and returns its address.
func startServer(t *testing.T, h raft.Handler) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(MaxMessageSize))
	Register(gs, h)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func TestRoundTrip(t *testing.T) {
	h := &fakeHandler{}
	addr := startServer(t, h)
	p := NewPeer(addr, WithTimeout(2*time.Second))
	t.Cleanup(func() { p.(*peer).Close() })

	t.Run("RequestVote", func(t *testing.T) {
		args := &raft.RequestVoteArgs{Term: 5, CandidateID: 2, LastLogIndex: 99, LastLogTerm: 4}
		reply, ok := p.RequestVote(args)
		if !ok {
			t.Fatal("ok=false")
		}
		if reply.Term != 6 || !reply.VoteGranted {
			t.Fatalf("reply = %+v, want Term=6 VoteGranted=true", reply)
		}
		h.mu.Lock()
		got := h.rv
		h.mu.Unlock()
		if !reflect.DeepEqual(got, args) {
			t.Fatalf("handler saw %+v, want %+v", got, args)
		}
	})

	t.Run("AppendEntries", func(t *testing.T) {
		args := &raft.AppendEntriesArgs{
			Term:         10,
			LeaderID:     1,
			PrevLogIndex: 20,
			PrevLogTerm:  9,
			Entries: []raft.Entry{
				{Term: 9, Command: []byte("set a=1")},
				{Term: 10, Command: []byte("set b=2")},
				{Term: 10, Command: []byte{0, 1, 2, 255}},
			},
			LeaderCommit: 19,
		}
		reply, ok := p.AppendEntries(args)
		if !ok {
			t.Fatal("ok=false")
		}
		want := &raft.AppendEntriesReply{Term: 11, Success: false, ConflictTerm: 7, ConflictIndex: 42}
		if !reflect.DeepEqual(reply, want) {
			t.Fatalf("reply = %+v, want %+v", reply, want)
		}
		h.mu.Lock()
		got := h.ae
		h.mu.Unlock()
		if !reflect.DeepEqual(got, args) {
			t.Fatalf("handler saw %+v, want %+v", got, args)
		}
		if len(got.Entries) != 3 {
			t.Fatalf("got %d entries, want 3", len(got.Entries))
		}
		for i := range args.Entries {
			if !bytes.Equal(got.Entries[i].Command, args.Entries[i].Command) {
				t.Fatalf("entry %d command mismatch: %q vs %q", i, got.Entries[i].Command, args.Entries[i].Command)
			}
		}
	})

	t.Run("Heartbeat", func(t *testing.T) {
		// Empty entries must survive as a zero-length (not nil-vs-non-nil
		// significant) list; Raft only checks len.
		args := &raft.AppendEntriesArgs{Term: 12, LeaderID: 1, PrevLogIndex: 23, PrevLogTerm: 10, LeaderCommit: 23}
		if _, ok := p.AppendEntries(args); !ok {
			t.Fatal("ok=false")
		}
		h.mu.Lock()
		got := h.ae
		h.mu.Unlock()
		if len(got.Entries) != 0 {
			t.Fatalf("heartbeat carried %d entries", len(got.Entries))
		}
		if got.Term != 12 || got.PrevLogIndex != 23 || got.LeaderCommit != 23 {
			t.Fatalf("handler saw %+v", got)
		}
	})

	t.Run("InstallSnapshot", func(t *testing.T) {
		data := bytes.Repeat([]byte("snapshot-bytes-"), 1<<16) // ~1 MiB
		args := &raft.InstallSnapshotArgs{Term: 30, LeaderID: 0, LastIncludedIndex: 500, LastIncludedTerm: 29, Data: data}
		reply, ok := p.InstallSnapshot(args)
		if !ok {
			t.Fatal("ok=false")
		}
		if reply.Term != 31 {
			t.Fatalf("reply.Term = %d, want 31", reply.Term)
		}
		h.mu.Lock()
		got := h.is
		h.mu.Unlock()
		if got.Term != 30 || got.LeaderID != 0 || got.LastIncludedIndex != 500 || got.LastIncludedTerm != 29 {
			t.Fatalf("handler saw %+v", got)
		}
		if !bytes.Equal(got.Data, data) {
			t.Fatalf("snapshot data mismatch (%d vs %d bytes)", len(got.Data), len(data))
		}
	})
}

func TestLargeSnapshot(t *testing.T) {
	h := &fakeHandler{}
	addr := startServer(t, h)
	p := NewPeer(addr, WithTimeout(2*time.Second))
	t.Cleanup(func() { p.(*peer).Close() })

	// Bigger than gRPC's 4 MiB default; must go through thanks to MaxMessageSize.
	data := make([]byte, 8<<20)
	for i := range data {
		data[i] = byte(i)
	}
	reply, ok := p.InstallSnapshot(&raft.InstallSnapshotArgs{Term: 1, LastIncludedIndex: 1, LastIncludedTerm: 1, Data: data})
	if !ok || reply.Term != 2 {
		t.Fatalf("ok=%v reply=%+v", ok, reply)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !bytes.Equal(h.is.Data, data) {
		t.Fatal("8 MiB snapshot did not round-trip")
	}
}

func TestClosedPort(t *testing.T) {
	// Grab a free port and release it so nothing is listening there.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	const timeout = 200 * time.Millisecond
	p := NewPeer(addr, WithTimeout(timeout))
	t.Cleanup(func() { p.(*peer).Close() })

	start := time.Now()
	reply, ok := p.RequestVote(&raft.RequestVoteArgs{Term: 1})
	elapsed := time.Since(start)
	if ok {
		t.Fatalf("ok=true against closed port, reply=%+v", reply)
	}
	if reply != nil {
		t.Fatalf("reply should be nil on failure, got %+v", reply)
	}
	if elapsed > 2*timeout {
		t.Fatalf("RequestVote took %v, want <= %v", elapsed, 2*timeout)
	}

	start = time.Now()
	if _, ok := p.AppendEntries(&raft.AppendEntriesArgs{Term: 1}); ok {
		t.Fatal("AppendEntries ok=true against closed port")
	}
	if elapsed := time.Since(start); elapsed > 2*timeout {
		t.Fatalf("AppendEntries took %v, want <= %v", elapsed, 2*timeout)
	}
}

func TestSlowHandlerTimesOut(t *testing.T) {
	h := &fakeHandler{slow: 500 * time.Millisecond}
	addr := startServer(t, h)
	const timeout = 100 * time.Millisecond
	p := NewPeer(addr, WithTimeout(timeout))
	t.Cleanup(func() { p.(*peer).Close() })

	start := time.Now()
	_, ok := p.RequestVote(&raft.RequestVoteArgs{Term: 1})
	if ok {
		t.Fatal("ok=true from a handler slower than the timeout")
	}
	if elapsed := time.Since(start); elapsed > 3*timeout {
		t.Fatalf("took %v, want about %v", elapsed, timeout)
	}
}

func TestConcurrentCalls(t *testing.T) {
	h := &fakeHandler{}
	addr := startServer(t, h)
	p := NewPeer(addr, WithTimeout(2*time.Second))
	t.Cleanup(func() { p.(*peer).Close() })

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			r, ok := p.RequestVote(&raft.RequestVoteArgs{Term: i})
			if !ok || r.Term != i+1 {
				errs <- "bad RequestVote"
			}
			a, ok := p.AppendEntries(&raft.AppendEntriesArgs{Term: i, Entries: []raft.Entry{{Term: i, Command: []byte{byte(i)}}}})
			if !ok || a.Term != i+1 {
				errs <- "bad AppendEntries"
			}
		}(uint64(i))
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

func TestConvertNilSafe(t *testing.T) {
	if requestVoteArgsToProto(nil) != nil || requestVoteArgsFromProto(nil) != nil ||
		requestVoteReplyToProto(nil) != nil || requestVoteReplyFromProto(nil) != nil ||
		appendEntriesArgsToProto(nil) != nil || appendEntriesArgsFromProto(nil) != nil ||
		appendEntriesReplyToProto(nil) != nil || appendEntriesReplyFromProto(nil) != nil ||
		installSnapshotArgsToProto(nil) != nil || installSnapshotArgsFromProto(nil) != nil ||
		installSnapshotReplyToProto(nil) != nil || installSnapshotReplyFromProto(nil) != nil {
		t.Fatal("nil input must yield nil output")
	}
	if entriesToProto(nil) != nil || entriesFromProto(nil) != nil {
		t.Fatal("nil entries must stay nil")
	}
}

func TestEntriesCopied(t *testing.T) {
	in := []raft.Entry{{Term: 1, Command: []byte("a")}, {Term: 2, Command: []byte("b")}}
	out := entriesFromProto(entriesToProto(in))
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip: %+v vs %+v", in, out)
	}
	if &in[0] == &out[0] {
		t.Fatal("entries slice aliases input")
	}
}
