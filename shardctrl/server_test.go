package shardctrl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/raft"
	"dsys/raft/simnet"
)

// ---------------------------------------------------------------------------
// A cluster of shardctrl servers over the simulated network, exactly like
// kv/raftkv's test cluster: direct Go calls to the servers' gRPC methods,
// no real gRPC, so the test is about consensus and rebalancing rather than
// sockets.
// ---------------------------------------------------------------------------

type cluster struct {
	t          *testing.T
	n          int
	net        *simnet.Net
	mu         sync.Mutex
	servers    []*Server
	persisters []*raft.MemPersister
	cfg        Config
	lastLeader atomic.Int32
}

func newCluster(t *testing.T, n int, opts ...func(*Config)) *cluster {
	c := &cluster{
		t:          t,
		n:          n,
		net:        simnet.New(n),
		servers:    make([]*Server, n),
		persisters: make([]*raft.MemPersister, n),
		cfg: Config{
			CommitTimeout: time.Second,
			Raft: raft.Config{
				HeartbeatInterval:  50 * time.Millisecond,
				ElectionTimeoutMin: 250 * time.Millisecond,
				ElectionTimeoutMax: 500 * time.Millisecond,
			},
		},
	}
	for _, o := range opts {
		o(&c.cfg)
	}
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewMemPersister()
		c.start(i)
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			if s != nil {
				s.Kill()
			}
		}
	})
	return c
}

// start (re)creates server i from its persister.
func (c *cluster) start(i int) {
	peers := make([]raft.Peer, c.n)
	for j := 0; j < c.n; j++ {
		if j != i {
			peers[j] = c.net.Peer(i, j)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := New(peers, i, c.persisters[i], c.cfg)
	c.servers[i] = s
	c.net.Bind(i, s.Raft())
	c.net.Connect(i)
}

// crash kills server i and keeps only what it had persisted.
func (c *cluster) crash(i int) {
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
	c.net.Disconnect(i)
	if s != nil {
		s.Kill()
	}
}

func (c *cluster) server(i int) *Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

// do runs fn against servers until one succeeds, following leader changes.
// The op (including its RequestMeta) is fixed by the caller, so every retry
// is the same logical request and the server deduplicates it.
func (c *cluster) do(ctx context.Context, fn func(s *Server) error) error {
	i := int(c.lastLeader.Load())
	for {
		if s := c.server(i); s != nil {
			err := fn(s)
			if err == nil {
				c.lastLeader.Store(int32(i))
				return nil
			}
			switch status.Code(err) {
			case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
			default:
				return err
			}
		}
		i = (i + 1) % c.n
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// client is one logical client with its own request ids.
type client struct {
	c   *cluster
	id  string
	seq uint64
}

func (c *cluster) client(name string) *client { return &client{c: c, id: name} }

func (cl *client) meta() *shardctrlv1.RequestMeta {
	cl.seq++
	return &shardctrlv1.RequestMeta{ClientId: cl.id, RequestId: cl.seq}
}

func (cl *client) join(ctx context.Context, gids ...int64) error {
	groups := make(map[int64]*shardctrlv1.Group, len(gids))
	for _, gid := range gids {
		groups[gid] = &shardctrlv1.Group{Addrs: []string{fmt.Sprintf("addr-%d", gid)}}
	}
	req := &shardctrlv1.JoinRequest{Meta: cl.meta(), Groups: groups}
	return cl.c.do(ctx, func(s *Server) error { _, err := s.Join(ctx, req); return err })
}

func (cl *client) leave(ctx context.Context, gids ...int64) error {
	req := &shardctrlv1.LeaveRequest{Meta: cl.meta(), Gids: gids}
	return cl.c.do(ctx, func(s *Server) error { _, err := s.Leave(ctx, req); return err })
}

func (cl *client) move(ctx context.Context, shardID, gid int64) error {
	req := &shardctrlv1.MoveRequest{Meta: cl.meta(), Shard: shardID, Gid: gid}
	return cl.c.do(ctx, func(s *Server) error { _, err := s.Move(ctx, req); return err })
}

func (cl *client) query(ctx context.Context, num int64) (*shardctrlv1.Config, error) {
	req := &shardctrlv1.QueryRequest{Num: num}
	var resp *shardctrlv1.QueryResponse
	err := cl.c.do(ctx, func(s *Server) error {
		var err error
		resp, err = s.Query(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.Config, nil
}

func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBasicJoinQuery(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client("c1")
	ctx, cancel := ctxT(20 * time.Second)
	defer cancel()

	cfg0, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if cfg0.Num != 0 || len(cfg0.Groups) != 0 {
		t.Fatalf("initial config = %+v", cfg0)
	}

	if err := cl.join(ctx, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
	cfg1, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if cfg1.Num != 1 || len(cfg1.Groups) != 3 {
		t.Fatalf("after join config = %+v", cfg1)
	}
	for _, gid := range cfg1.Shards {
		if gid == 0 {
			t.Fatal("some shard is unassigned after join")
		}
	}

	// Querying a past config still works.
	if got, err := cl.query(ctx, 0); err != nil || got.Num != 0 {
		t.Fatalf("query(0) = %+v, %v", got, err)
	}
}

func TestServerLeaveRedistributes(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client("c1")
	ctx, cancel := ctxT(20 * time.Second)
	defer cancel()

	if err := cl.join(ctx, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
	if err := cl.leave(ctx, 2); err != nil {
		t.Fatal(err)
	}
	cfg, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Groups[2]; ok {
		t.Fatal("left group 2 still present")
	}
	for _, gid := range cfg.Shards {
		if gid == 2 {
			t.Fatal("shard still assigned to group that left")
		}
		if gid != 1 && gid != 3 {
			t.Fatalf("shard owned by unexpected group %d", gid)
		}
	}
}

func TestMoveOverrideThenJoinRespectsIt(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client("c1")
	ctx, cancel := ctxT(20 * time.Second)
	defer cancel()

	if err := cl.join(ctx, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
	before, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	// Force shard 0 onto whichever group does not already own it.
	forced := before.Shards[0]
	for _, gid := range []int64{1, 2, 3} {
		if gid != before.Shards[0] {
			forced = gid
			break
		}
	}
	if err := cl.move(ctx, 0, forced); err != nil {
		t.Fatal(err)
	}
	moved, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Shards[0] != forced {
		t.Fatalf("shard 0 = %d, want %d", moved.Shards[0], forced)
	}

	// A later Join must rebalance from the post-Move state, not revert it.
	if err := cl.join(ctx, 4); err != nil {
		t.Fatal(err)
	}
	after, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int64]int{}
	for _, gid := range after.Shards {
		counts[gid]++
	}
	if len(counts) != 4 {
		t.Fatalf("after join with 4 groups, counts=%v", counts)
	}
	min, max := 10, 0
	for _, n := range counts {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	if max-min > 1 {
		t.Fatalf("unbalanced after join: %v", counts)
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client("c1")
	ctx, cancel := ctxT(30 * time.Second)
	defer cancel()

	if err := cl.join(ctx, 1, 2); err != nil {
		t.Fatal(err)
	}
	leader := int(c.lastLeader.Load())
	c.net.Disconnect(leader)
	t.Logf("disconnected leader %d", leader)

	start := time.Now()
	if err := cl.join(ctx, 3); err != nil {
		t.Fatal(err)
	}
	t.Logf("join after leader loss took %s", time.Since(start).Round(time.Millisecond))

	cfg, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Groups) != 3 {
		t.Fatalf("groups = %v, want 3", cfg.Groups)
	}

	// The old leader rejoins and must catch up, not impose its stale view.
	c.net.Connect(leader)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if s := c.server(leader); s != nil && s.LastApplied() >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old leader did not catch up")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A retried request across a leader change must apply exactly once: the
// config must not gain an extra Num for one logical Join. Mirrors
// raftkv's TestRetryAcrossLeaderChangeAppliesOnce.
func TestRetryAcrossLeaderChangeAppliesOnce(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client("c1")
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()

	if err := cl.join(ctx, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
	base, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}

	req := &shardctrlv1.JoinRequest{Meta: cl.meta(), Groups: map[int64]*shardctrlv1.Group{
		4: {Addrs: []string{"addr-4"}},
	}}
	leader := int(c.lastLeader.Load())
	c.net.Disconnect(leader)

	err = c.do(ctx, func(s *Server) error { _, err := s.Join(ctx, req); return err })
	c.net.Connect(leader)
	if err != nil {
		t.Fatal(err)
	}

	after, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if after.Num != base.Num+1 {
		t.Fatalf("num = %d, want %d (the retried join must apply exactly once)", after.Num, base.Num+1)
	}
	if len(after.Groups) != 4 {
		t.Fatalf("groups = %v, want 4", after.Groups)
	}

	// Explicitly retry the identical request again: must be deduped, not
	// produce a second new config.
	err = c.do(ctx, func(s *Server) error { _, err := s.Join(ctx, req); return err })
	if err != nil {
		t.Fatal(err)
	}
	final, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if final.Num != after.Num {
		t.Fatalf("num = %d after explicit retry, want unchanged %d", final.Num, after.Num)
	}
}

func TestFullClusterCrashPreservesHistory(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client("c1")
	ctx, cancel := ctxT(30 * time.Second)
	defer cancel()

	if err := cl.join(ctx, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := cl.join(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if err := cl.move(ctx, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := cl.leave(ctx, 2); err != nil {
		t.Fatal(err)
	}
	latestBefore, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}

	ctx2, cancel2 := ctxT(30 * time.Second)
	defer cancel2()
	latestAfter, err := cl.query(ctx2, -1)
	if err != nil {
		t.Fatal(err)
	}
	if latestAfter.Num != latestBefore.Num {
		t.Fatalf("num after restart = %d, want %d", latestAfter.Num, latestBefore.Num)
	}
	for i, gid := range latestBefore.Shards {
		if latestAfter.Shards[i] != gid {
			t.Fatalf("shard %d = %d after restart, want %d", i, latestAfter.Shards[i], gid)
		}
	}

	// Earlier configs survived too.
	for num := int64(0); num <= latestBefore.Num; num++ {
		if _, err := cl.query(ctx2, num); err != nil {
			t.Fatalf("query(%d) after restart: %v", num, err)
		}
	}

	// Dedup state survived: a stale retry is still recognised.
	stale := &shardctrlv1.LeaveRequest{Meta: &shardctrlv1.RequestMeta{ClientId: "c1", RequestId: 1}, Gids: []int64{99}}
	err = c.do(ctx2, func(s *Server) error { _, err := s.Leave(ctx2, stale); return err })
	if err == nil {
		t.Fatal("expected an error retrying a stale request id, got nil")
	}
}
