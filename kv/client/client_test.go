package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kvv1 "dsys/gen/kv/v1"
	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/shard"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// metaKey is a RequestMeta flattened for recording and comparison.
type metaKey struct {
	cid string
	rid uint64
}

// fakeKV is an in-memory kvv1.KVServer that records the RequestMeta of every
// Put attempt it sees and can be told to reject attempts.
type fakeKV struct {
	kvv1.UnimplementedKVServer
	mu    sync.Mutex
	data  map[string][]byte
	metas []metaKey
	puts  int
	// reject, if set, is consulted with the 1-based attempt number before a
	// Put is applied; a non-nil error is returned instead.
	reject func(attempt int) error
}

func newFakeKV() *fakeKV { return &fakeKV{data: make(map[string][]byte)} }

func (f *fakeKV) Put(_ context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	f.metas = append(f.metas, metaKey{req.GetMeta().GetClientId(), req.GetMeta().GetRequestId()})
	if f.reject != nil {
		if err := f.reject(f.puts); err != nil {
			return nil, err
		}
	}
	f.data[req.GetKey()] = req.GetValue()
	return &kvv1.PutResponse{}, nil
}

func (f *fakeKV) Get(_ context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[req.GetKey()]
	return &kvv1.GetResponse{Value: v, Found: ok}, nil
}

func (f *fakeKV) snapshot() (puts int, metas []metaKey, data map[string][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data = make(map[string][]byte, len(f.data))
	for k, v := range f.data {
		data[k] = v
	}
	return f.puts, append([]metaKey(nil), f.metas...), data
}

// fakeCtrl serves Query from a swappable Config and counts queries.
type fakeCtrl struct {
	shardctrlv1.UnimplementedShardCtrlServer
	mu      sync.Mutex
	cfg     *shardctrlv1.Config
	queries atomic.Int64
}

func (f *fakeCtrl) Query(context.Context, *shardctrlv1.QueryRequest) (*shardctrlv1.QueryResponse, error) {
	f.queries.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return &shardctrlv1.QueryResponse{Config: f.cfg}, nil
}

func (f *fakeCtrl) set(cfg *shardctrlv1.Config) {
	f.mu.Lock()
	f.cfg = cfg
	f.mu.Unlock()
}

// serve starts a gRPC server on a loopback port; register wires the fake in.
func serve(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func serveKV(t *testing.T, f *fakeKV) string {
	return serve(t, func(gs *grpc.Server) { kvv1.RegisterKVServer(gs, f) })
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// A retried mutation must carry the SAME RequestMeta on every attempt;
// that is the whole basis of server-side dedup.
func TestRetryReusesMeta(t *testing.T) {
	f := newFakeKV()
	f.reject = func(attempt int) error {
		if attempt == 1 {
			return status.Error(codes.Unavailable, "not leader")
		}
		return nil
	}
	addr := serveKV(t, f)
	c := NewFlat([]string{addr})
	defer c.Close()

	if err := c.Put(testCtx(t), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	puts, metas, data := f.snapshot()
	if puts != 2 {
		t.Fatalf("server saw %d attempts, want 2", puts)
	}
	if metas[0] != metas[1] {
		t.Fatalf("retry changed meta: first %+v, second %+v", metas[0], metas[1])
	}
	if metas[0].cid != c.ClientID() || metas[0].rid != 1 {
		t.Fatalf("meta = %+v, want {%s 1}", metas[0], c.ClientID())
	}
	if string(data["k"]) != "v" {
		t.Fatalf("value not stored: %q", data["k"])
	}
	if got := c.Retries(); got != 1 {
		t.Fatalf("Retries() = %d, want 1", got)
	}
	// The next mutation gets the next id, not a reused one.
	if err := c.Put(testCtx(t), "k2", []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, metas, _ = f.snapshot()
	if last := metas[len(metas)-1]; last.rid != 2 {
		t.Fatalf("second logical put has request id %d, want 2", last.rid)
	}
}

// A follower that names the leader is followed immediately, and the leader
// is remembered for the next request (no round trip through the follower).
func TestLeaderHintJump(t *testing.T) {
	leader := newFakeKV()
	leaderAddr := serveKV(t, leader)

	follower := newFakeKV()
	follower.reject = func(int) error {
		return status.Errorf(codes.Unavailable, "not leader leader=%s", leaderAddr)
	}
	followerAddr := serveKV(t, follower)

	// Only the follower is configured; the leader is learned from the hint.
	c := NewFlat([]string{followerAddr})
	defer c.Close()

	for i := 0; i < 3; i++ {
		if err := c.Put(testCtx(t), fmt.Sprint("k", i), []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	fp, _, _ := follower.snapshot()
	lp, _, ldata := leader.snapshot()
	if fp != 1 {
		t.Fatalf("follower saw %d attempts, want exactly 1 (the leader should be remembered)", fp)
	}
	if lp != 3 || len(ldata) != 3 {
		t.Fatalf("leader saw %d attempts / %d keys, want 3 / 3", lp, len(ldata))
	}
	if got := c.Retries(); got != 1 {
		t.Fatalf("Retries() = %d, want 1", got)
	}
}

// allTo returns a config assigning every shard to gid.
func allTo(num int64, gid int64, groups map[int64]*shardctrlv1.Group) *shardctrlv1.Config {
	shards := make([]int64, shard.NShards)
	for i := range shards {
		shards[i] = gid
	}
	return &shardctrlv1.Config{Num: num, Shards: shards, Groups: groups}
}

// When a group answers "wrong group", the client must re-ask the controller
// (even though its cached config is fresh) and retry against the new owner
// with the same request.
func TestShardedWrongGroupReresolves(t *testing.T) {
	g1, g2 := newFakeKV(), newFakeKV()
	g1Addr, g2Addr := serveKV(t, g1), serveKV(t, g2)
	groups := map[int64]*shardctrlv1.Group{
		1: {Addrs: []string{g1Addr}},
		2: {Addrs: []string{g2Addr}},
	}
	ctrl := &fakeCtrl{cfg: allTo(1, 1, groups)}
	ctrlAddr := serve(t, func(gs *grpc.Server) { shardctrlv1.RegisterShardCtrlServer(gs, ctrl) })

	// A very long TTL guarantees the second Put uses the CACHED config, so
	// the only way it can reach g2 is via wrong-group invalidation.
	c := NewSharded([]string{ctrlAddr}, WithConfigCacheTTL(time.Hour))
	defer c.Close()

	if err := c.Put(testCtx(t), "a", []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if _, _, d := g1.snapshot(); string(d["a"]) != "1" {
		t.Fatalf("first put did not land on group 1: %v", d)
	}
	if q := ctrl.queries.Load(); q != 1 {
		t.Fatalf("controller queried %d times after first put, want 1", q)
	}

	// Move every shard to group 2; group 1 now refuses like shardkv does.
	ctrl.set(allTo(2, 2, groups))
	g1.mu.Lock()
	g1.reject = func(int) error {
		return status.Error(codes.FailedPrecondition, "shardkv: wrong group for this key's shard")
	}
	g1.mu.Unlock()

	if err := c.Put(testCtx(t), "b", []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	p1, m1, _ := g1.snapshot()
	p2, m2, d2 := g2.snapshot()
	if p1 != 2 {
		t.Fatalf("group 1 saw %d attempts, want 2 (cached config must have sent the second put there first)", p1)
	}
	if p2 != 1 || string(d2["b"]) != "2" {
		t.Fatalf("group 2 saw %d attempts, data %v; want the retried put to land there", p2, d2)
	}
	if m1[1] != m2[0] {
		t.Fatalf("retry against the new owner changed meta: %+v vs %+v", m1[1], m2[0])
	}
	if q := ctrl.queries.Load(); q != 2 {
		t.Fatalf("controller queried %d times, want 2 (one forced re-resolution)", q)
	}
	if got := c.Retries(); got != 1 {
		t.Fatalf("Retries() = %d, want 1", got)
	}
}

// A non-retryable answer is returned as-is, not retried.
func TestNonRetryableNotRetried(t *testing.T) {
	f := newFakeKV()
	f.reject = func(int) error { return status.Error(codes.InvalidArgument, "bad key") }
	c := NewFlat([]string{serveKV(t, f)})
	defer c.Close()
	err := c.Put(testCtx(t), "k", nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	if p, _, _ := f.snapshot(); p != 1 {
		t.Fatalf("server saw %d attempts, want 1", p)
	}
}

// Giving up after the context expires yields an AttemptError wrapping the
// last transient status.
func TestGiveUpReturnsAttemptError(t *testing.T) {
	f := newFakeKV()
	f.reject = func(int) error { return status.Error(codes.Unavailable, "not leader") }
	addr := serveKV(t, f)
	c := NewFlat([]string{addr}, WithTimeout(150*time.Millisecond))
	defer c.Close()
	err := c.Put(context.Background(), "k", nil)
	var ae *AttemptError
	if !errors.As(err, &ae) || ae.Addr != addr {
		t.Fatalf("err = %#v, want *AttemptError from %s", err, addr)
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("status.Code(err) = %v, want Unavailable via Unwrap", status.Code(err))
	}
	if c.Retries() == 0 {
		t.Fatal("expected at least one retry before giving up")
	}
}

// Sessions get their own identity and their own request counter, and share
// the parent's retry counter.
func TestSessionIdentitiesDiffer(t *testing.T) {
	f := newFakeKV()
	c := NewFlat([]string{serveKV(t, f)})
	defer c.Close()
	s1, s2 := c.Session(), c.Session()
	ids := map[string]bool{c.ClientID(): true, s1.ClientID(): true, s2.ClientID(): true}
	if len(ids) != 3 {
		t.Fatalf("identities collide: root=%s s1=%s s2=%s", c.ClientID(), s1.ClientID(), s2.ClientID())
	}
	ctx := testCtx(t)
	for _, cl := range []*Client{c, s1, s2, s1} {
		if err := cl.Put(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	_, metas, _ := f.snapshot()
	want := []metaKey{{c.ClientID(), 1}, {s1.ClientID(), 1}, {s2.ClientID(), 1}, {s1.ClientID(), 2}}
	for i := range want {
		if metas[i] != want[i] {
			t.Fatalf("attempt %d meta = %+v, want %+v", i, metas[i], want[i])
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Session.Close: %v", err)
	}
	// Closing a session must not have torn down the shared connections.
	if err := s2.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put after sibling Close: %v", err)
	}
}
