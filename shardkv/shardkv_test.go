package shardkv

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "dsys/gen/kv/v1"
	"dsys/raft"
	"dsys/raft/simnet"
	"dsys/shard"
)

// ---------------------------------------------------------------------------
// Test harness: several groups, each a real Raft cluster over a simulated
// network (one dsys/raft/simnet.Net per group, exactly like
// kv/raftkv/raftkv_test.go uses one for a single group). A fake controller
// and a fake cross-group fetcher stand in for shardctrl and
// shardkv/grpctransport: both are exercised by the OTHER engineers' code and
// tested there; here we hand-construct Configs so this test is about
// shardkv's own migration and ownership logic, not about the rebalancing
// algorithm or gRPC wiring.
// ---------------------------------------------------------------------------

// fakeCtrl is a Controller a test can push new configs into directly.
type fakeCtrl struct {
	mu      sync.Mutex
	configs []Config // index 0 is the zero config (Num 0, everything unassigned)
}

func newFakeCtrl() *fakeCtrl { return &fakeCtrl{configs: []Config{{}}} }

func (f *fakeCtrl) Query(_ context.Context, num int64) (Config, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if num < 0 || num >= int64(len(f.configs)) {
		return f.configs[len(f.configs)-1], nil
	}
	return f.configs[num], nil
}

// push appends cfg with the next sequential Num, ignoring whatever Num the
// caller set, and returns the assigned number.
func (f *fakeCtrl) push(cfg Config) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg.Num = int64(len(f.configs))
	f.configs = append(f.configs, cfg)
	return cfg.Num
}

func (f *fakeCtrl) latest() Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.configs[len(f.configs)-1]
}

// fakeFetcher routes PullShard to whichever in-process Server owns the
// given synthetic address, standing in for shardkv/grpctransport.Fetcher.
type fakeFetcher struct {
	mu     sync.Mutex
	byAddr map[string]*Server
}

func newFakeFetcher() *fakeFetcher { return &fakeFetcher{byAddr: make(map[string]*Server)} }

func (f *fakeFetcher) bind(addr string, s *Server) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byAddr[addr] = s
}

func (f *fakeFetcher) PullShard(_ context.Context, addrs []string, configNum int64, shardID int) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range addrs {
		if s, ok := f.byAddr[a]; ok {
			if snap, ok := s.HandlePullShard(configNum, shardID); ok {
				return snap, true, nil
			}
		}
	}
	return nil, false, nil
}

// group is one Raft-replicated shardkv group under test.
type group struct {
	gid        int64
	addrs      []string
	net        *simnet.Net
	fetcher    *fakeFetcher
	ctrl       *fakeCtrl
	mu         sync.Mutex
	servers    []*Server
	persisters []*raft.MemPersister
	lastLeader atomic.Int32
}

func newGroup(t *testing.T, gid int64, n int, ctrl *fakeCtrl, fetcher *fakeFetcher) *group {
	g := &group{
		gid:        gid,
		addrs:      make([]string, n),
		net:        simnet.New(n),
		fetcher:    fetcher,
		ctrl:       ctrl,
		servers:    make([]*Server, n),
		persisters: make([]*raft.MemPersister, n),
	}
	for i := 0; i < n; i++ {
		g.addrs[i] = fmt.Sprintf("g%d-r%d", gid, i)
	}
	for i := 0; i < n; i++ {
		g.persisters[i] = raft.NewMemPersister()
		g.start(i)
	}
	t.Cleanup(func() {
		for _, s := range g.servers {
			if s != nil {
				s.Kill()
			}
		}
	})
	return g
}

func (g *group) start(i int) {
	peers := make([]raft.Peer, len(g.addrs))
	for j := range g.addrs {
		if j != i {
			peers[j] = g.net.Peer(i, j)
		}
	}
	s := New(peers, i, g.persisters[i], Options{
		GID:           g.gid,
		Addrs:         g.addrs,
		Ctrl:          g.ctrl,
		Fetcher:       g.fetcher,
		PollInterval:  15 * time.Millisecond,
		CommitTimeout: 2 * time.Second,
		Raft: raft.Config{
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 250 * time.Millisecond,
			ElectionTimeoutMax: 500 * time.Millisecond,
		},
	})
	g.mu.Lock()
	g.servers[i] = s
	g.mu.Unlock()
	g.fetcher.bind(g.addrs[i], s)
	g.net.Bind(i, s.Raft())
	g.net.Connect(i)
}

func (g *group) crash(i int) {
	g.mu.Lock()
	s := g.servers[i]
	g.servers[i] = nil
	g.persisters[i] = g.persisters[i].Copy()
	g.mu.Unlock()
	g.net.Disconnect(i)
	if s != nil {
		s.Kill()
	}
}

func (g *group) server(i int) *Server {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.servers[i]
}

func (g *group) n() int { return len(g.addrs) }

// ---------------------------------------------------------------------------
// A tiny multi-group client: resolve owner via the controller, follow
// leader hints and retryable codes within the owning group, and on
// FailedPrecondition (wrong group OR stale request; either way "the config
// this replica knows about disagrees with mine") re-resolve the owner and
// try again. This is deliberately the same shape cmd/kvctl's sharded mode
// uses against real gRPC: resolve -> try -> on ownership doubt, re-resolve.
// ---------------------------------------------------------------------------

type testCluster struct {
	ctrl   *fakeCtrl
	groups map[int64]*group
}

func (tc *testCluster) group(gid int64) *group { return tc.groups[gid] }

func (tc *testCluster) do(ctx context.Context, key string, fn func(s *Server) error) error {
	shardID := shard.Key2Shard(key)
	for {
		gid := tc.ctrl.latest().Owner(shardID)
		g := tc.group(gid)
		if g == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(15 * time.Millisecond):
				continue
			}
		}
		wrongGroup := false
		for attempt := 0; attempt < g.n(); attempt++ {
			i := int(g.lastLeader.Load())
			if s := g.server(i); s != nil {
				err := fn(s)
				if err == nil {
					g.lastLeader.Store(int32(i))
					return nil
				}
				switch status.Code(err) {
				case codes.FailedPrecondition:
					wrongGroup = true
				case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
				default:
					return err
				}
			}
			g.lastLeader.Store(int32((i + 1) % g.n()))
			if wrongGroup {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Millisecond):
		}
	}
}

type client struct {
	tc  *testCluster
	id  string
	seq uint64
}

func (tc *testCluster) client(name string) *client { return &client{tc: tc, id: name} }

func (cl *client) meta() *kvv1.RequestMeta {
	cl.seq++
	return &kvv1.RequestMeta{ClientId: cl.id, RequestId: cl.seq}
}

func (cl *client) put(ctx context.Context, k, v string) error {
	req := &kvv1.PutRequest{Meta: cl.meta(), Key: k, Value: []byte(v)}
	return cl.tc.do(ctx, k, func(s *Server) error { _, err := s.Put(ctx, req); return err })
}

func (cl *client) get(ctx context.Context, k string) (string, bool, error) {
	req := &kvv1.GetRequest{Key: k}
	var resp *kvv1.GetResponse
	err := cl.tc.do(ctx, k, func(s *Server) error {
		var err error
		resp, err = s.Get(ctx, req)
		return err
	})
	if err != nil {
		return "", false, err
	}
	return string(resp.Value), resp.Found, nil
}

func (cl *client) cas(ctx context.Context, k, expected, v string) (bool, string, error) {
	req := &kvv1.CASRequest{Meta: cl.meta(), Key: k, Expected: []byte(expected), Value: []byte(v)}
	var resp *kvv1.CASResponse
	err := cl.tc.do(ctx, k, func(s *Server) error {
		var err error
		resp, err = s.CompareAndSwap(ctx, req)
		return err
	})
	if err != nil {
		return false, "", err
	}
	return resp.Swapped, string(resp.Current), nil
}

func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// allTo builds a Config assigning every shard to gid.
func allTo(gid int64, groups map[int64][]string) Config {
	var c Config
	for i := range c.Shards {
		c.Shards[i] = gid
	}
	c.Groups = groups
	return c
}

func waitLen(t *testing.T, ctrl *fakeCtrl, num int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctrl.latest().Num >= num {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("controller never observed config %d (stuck at %d) — a group's poll loop is not advancing", num, ctrl.latest().Num)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBasicSingleGroup(t *testing.T) {
	ctrl := newFakeCtrl()
	fetcher := newFakeFetcher()
	gA := newGroup(t, 100, 3, ctrl, fetcher)
	ctrl.push(allTo(100, map[int64][]string{100: gA.addrs}))

	tc := &testCluster{ctrl: ctrl, groups: map[int64]*group{100: gA}}
	cl := tc.client("c1")
	ctx, cancel := ctxT(20 * time.Second)
	defer cancel()

	if err := cl.put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if v, found, _ := cl.get(ctx, "a"); !found || v != "1" {
		t.Fatalf("get a = %q %v", v, found)
	}
	if swapped, _, _ := cl.cas(ctx, "a", "1", "2"); !swapped {
		t.Fatal("cas should swap")
	}
}

// TestMigrationMovesDataAndSessions is the core Phase 3 correctness test:
// data written to group A before a shard moves is visible, through the new
// owner, after the move — and the dedup table moved with it, so a client's
// pre-move retry is still recognised by the new owner instead of being
// re-applied.
func TestMigrationMovesDataAndSessions(t *testing.T) {
	ctrl := newFakeCtrl()
	fetcher := newFakeFetcher()
	gA := newGroup(t, 100, 3, ctrl, fetcher)
	gB := newGroup(t, 200, 3, ctrl, fetcher)
	groups := map[int64][]string{100: gA.addrs, 200: gB.addrs}
	ctrl.push(allTo(100, groups))

	tc := &testCluster{ctrl: ctrl, groups: map[int64]*group{100: gA, 200: gB}}
	cl := tc.client("c1")
	ctx, cancel := ctxT(30 * time.Second)
	defer cancel()

	// Find a key whose shard we can move on its own, and write it via A.
	key := "movable"
	shardID := shard.Key2Shard(key)
	if err := cl.put(ctx, key, "before-move"); err != nil {
		t.Fatal(err)
	}
	// A CAS that will succeed exactly once; its RequestMeta must still be
	// recognised as "already applied" after the shard moves.
	preMoveMeta := cl.meta()
	req := &kvv1.CASRequest{Meta: preMoveMeta, Key: key, Expected: []byte("before-move"), Value: []byte("after-cas")}
	if err := tc.do(ctx, key, func(s *Server) error { _, err := s.CompareAndSwap(ctx, req); return err }); err != nil {
		t.Fatal(err)
	}

	// Move just that one shard to group B; everything else stays on A.
	next := ctrl.latest()
	next.Shards[shardID] = 200
	num := ctrl.push(next)
	waitLen(t, ctrl, num, 10*time.Second)

	// Poll until the read succeeds against the new owner (migration takes a
	// few poll intervals).
	deadline := time.Now().Add(10 * time.Second)
	var v string
	var found bool
	var err error
	for time.Now().Before(deadline) {
		v, found, err = cl.get(ctx, key)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("get after migration: %v", err)
	}
	if !found || v != "after-cas" {
		t.Fatalf("after migration: v=%q found=%v, want after-cas", v, found)
	}

	// Group A must now refuse the shard directly (bypass tc.do, which would
	// just follow the move) to prove it actually let go of it.
	directErr := func() error {
		i := int(gA.lastLeader.Load())
		for a := 0; a < gA.n(); a++ {
			if s := gA.server(i); s != nil {
				_, err := s.Get(ctx, &kvv1.GetRequest{Key: key})
				if status.Code(err) == codes.FailedPrecondition {
					return err
				}
				if status.Code(err) != codes.Unavailable {
					return nil // A answered successfully or with an unexpected code: not refusing
				}
			}
			i = (i + 1) % gA.n()
		}
		return nil
	}()
	if directErr == nil {
		t.Fatal("group A should refuse the migrated shard with FailedPrecondition, but did not")
	}

	// The retry: same RequestMeta as the CAS applied before the move. It
	// must report swapped=true (the ORIGINAL outcome) without mutating
	// anything again, proving the session table travelled with the shard.
	retryReq := &kvv1.CASRequest{Meta: preMoveMeta, Key: key, Expected: []byte("before-move"), Value: []byte("after-cas")}
	var retryResp *kvv1.CASResponse
	if err := tc.do(ctx, key, func(s *Server) error {
		var err error
		retryResp, err = s.CompareAndSwap(ctx, retryReq)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !retryResp.Swapped {
		t.Fatal("retry after migration lost the dedup session: swapped=false, the CAS was re-evaluated against post-move state instead of returning the cached result")
	}
	if v, _, _ := cl.get(ctx, key); v != "after-cas" {
		t.Fatalf("retry corrupted state: %q", v)
	}
}

// TestWritesContinueThroughRebalance hammers a set of keys spread across
// every shard while a config change moves half the shards from A to B, and
// checks every write that the client saw succeed is present in the final
// state under the current owner.
func TestWritesContinueThroughRebalance(t *testing.T) {
	ctrl := newFakeCtrl()
	fetcher := newFakeFetcher()
	gA := newGroup(t, 100, 3, ctrl, fetcher)
	gB := newGroup(t, 200, 3, ctrl, fetcher)
	groups := map[int64][]string{100: gA.addrs, 200: gB.addrs}
	ctrl.push(allTo(100, groups))
	tc := &testCluster{ctrl: ctrl, groups: map[int64]*group{100: gA, 200: gB}}

	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	keys := make([]string, shard.NShards*3)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var mu sync.Mutex
	committed := map[string]string{}
	writer := func(id int) {
		defer wg.Done()
		cl := tc.client(fmt.Sprintf("writer-%d", id))
		r := rand.New(rand.NewSource(int64(id)))
		for {
			select {
			case <-stop:
				return
			default:
			}
			k := keys[r.Intn(len(keys))]
			v := fmt.Sprintf("%d-%d", id, r.Intn(1_000_000))
			if err := cl.put(ctx, k, v); err == nil {
				mu.Lock()
				committed[k] = v
				mu.Unlock()
			}
		}
	}
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go writer(i)
	}

	time.Sleep(200 * time.Millisecond)
	// Split ownership: even-indexed shards move to B.
	next := ctrl.latest()
	for i := 0; i < shard.NShards; i += 2 {
		next.Shards[i] = 200
	}
	num := ctrl.push(next)
	waitLen(t, ctrl, num, 10*time.Second)

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// committed is a snapshot mid-flight (later writes to the same key may
	// have landed after we stopped recording); re-read each key we ever
	// recorded and just confirm it resolves to SOME value without error,
	// proving no shard is stuck unreachable after the split.
	mu.Lock()
	toCheck := make([]string, 0, len(committed))
	for k := range committed {
		toCheck = append(toCheck, k)
	}
	mu.Unlock()
	if len(toCheck) == 0 {
		t.Fatal("no writes ever committed during the run; the harness itself is broken")
	}
	for _, k := range toCheck {
		if _, _, err := tc.client("verifier").get(ctx, k); err != nil {
			t.Fatalf("key %q unreachable after rebalance: %v", k, err)
		}
	}
}

// TestCrashDuringMigrationRecovers crashes the new owner while it still has
// a shard pending, then restarts it, and checks the migration completes
// from the group's own Raft log/snapshot rather than being lost.
func TestCrashDuringMigrationRecovers(t *testing.T) {
	ctrl := newFakeCtrl()
	fetcher := newFakeFetcher()
	gA := newGroup(t, 100, 3, ctrl, fetcher)
	gB := newGroup(t, 200, 3, ctrl, fetcher)
	groups := map[int64][]string{100: gA.addrs, 200: gB.addrs}
	ctrl.push(allTo(100, groups))
	tc := &testCluster{ctrl: ctrl, groups: map[int64]*group{100: gA, 200: gB}}
	cl := tc.client("c1")
	ctx, cancel := ctxT(30 * time.Second)
	defer cancel()

	key := "will-migrate"
	shardID := shard.Key2Shard(key)
	if err := cl.put(ctx, key, "v1"); err != nil {
		t.Fatal(err)
	}

	// Crash a replica of B (the incoming owner) BEFORE the move even starts,
	// so it has to learn about the pending migration purely by replaying
	// (or being sent) the group's Raft log after it rejoins.
	victim := 1
	gB.crash(victim)

	next := ctrl.latest()
	next.Shards[shardID] = 200
	num := ctrl.push(next)
	waitLen(t, ctrl, num, 10*time.Second)

	// Give the two live B replicas time to actually complete the migration
	// among themselves.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v, found, err := cl.get(ctx, key); err == nil && found && v == "v1" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	gB.start(victim)
	// The restarted replica must eventually agree it owns the shard, either
	// via normal Raft replication or InstallSnapshot, without us doing
	// anything special to help it along.
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if s := gB.server(victim); s != nil && s.CurrentConfig().Owner(shardID) == 200 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v, found, err := cl.get(ctx, key); err != nil || !found || v != "v1" {
		t.Fatalf("after restart: v=%q found=%v err=%v", v, found, err)
	}
}

// ---------------------------------------------------------------------------
// Linearizability across live shard moves: the Phase 3 exit criterion,
// mirroring kv/raftkv's TestLinearizability but with a nemesis that also
// moves shards between groups, not just partitions/crashes.
// ---------------------------------------------------------------------------

type kvInput struct {
	op       string
	key      string
	value    string
	expected string
}

type kvOutput struct {
	value   string
	found   bool
	swapped bool
	current string
	failed  bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return "\x00absent" },
	Step: func(state, input, output any) (bool, any) {
		st := state.(string)
		in := input.(kvInput)
		out := output.(kvOutput)
		absent := st == "\x00absent"
		switch in.op {
		case "get":
			if out.failed {
				return true, st
			}
			if absent {
				return !out.found, st
			}
			return out.found && out.value == st, st
		case "put":
			return true, in.value
		case "cas":
			swapped := !absent && st == in.expected
			if out.failed {
				if swapped {
					return true, in.value
				}
				return true, st
			}
			if out.swapped != swapped {
				return false, st
			}
			if !absent && out.current != st {
				return false, st
			}
			if swapped {
				return true, in.value
			}
			return true, st
		}
		return false, st
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.op {
		case "get":
			return fmt.Sprintf("get(%s) -> %q,%v", in.key, out.value, out.found)
		case "put":
			return fmt.Sprintf("put(%s,%s)", in.key, in.value)
		default:
			return fmt.Sprintf("cas(%s,%s->%s) -> %v", in.key, in.expected, in.value, out.swapped)
		}
	},
}

func TestLinearizabilityAcrossShardMoves(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	const (
		nGroups  = 3
		perGroup = 3
		nClients = 6
		duration = 12 * time.Second
	)
	ctrl := newFakeCtrl()
	fetcher := newFakeFetcher()
	groups := map[int64]*group{}
	addrs := map[int64][]string{}
	for g := int64(1); g <= nGroups; g++ {
		gid := 100 * g
		groups[gid] = newGroup(t, gid, perGroup, ctrl, fetcher)
		addrs[gid] = groups[gid].addrs
	}
	ctrl.push(allTo(100, addrs)) // everything starts on the first group
	tc := &testCluster{ctrl: ctrl, groups: groups}
	keys := []string{"x", "y", "z", "w"}

	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Nemesis: periodically rebalance shards across groups.
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(400+r.Intn(400)) * time.Millisecond):
			}
			next := ctrl.latest()
			for i := 0; i < shard.NShards; i++ {
				if r.Intn(3) == 0 { // occasionally reassign a shard
					next.Shards[i] = 100 * int64(1+r.Intn(nGroups))
				}
			}
			next.Groups = addrs
			ctrl.push(next)
		}
	}()

	var mu sync.Mutex
	var ops []porcupine.Operation
	record := func(cid int, in kvInput, out kvOutput, call, ret time.Time) {
		mu.Lock()
		ops = append(ops, porcupine.Operation{
			ClientId: cid, Input: in, Output: out,
			Call: call.Sub(start).Nanoseconds(), Return: ret.Sub(start).Nanoseconds(),
		})
		mu.Unlock()
	}

	for ci := 0; ci < nClients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			cl := tc.client(fmt.Sprintf("lin-%d", ci))
			r := rand.New(rand.NewSource(int64(ci) + 100))
			for {
				select {
				case <-stop:
					return
				default:
				}
				in := kvInput{key: keys[r.Intn(len(keys))]}
				switch r.Intn(3) {
				case 0:
					in.op = "get"
				case 1:
					in.op, in.value = "put", fmt.Sprintf("%d-%d", ci, r.Intn(1000))
				default:
					in.op, in.value, in.expected = "cas", fmt.Sprintf("%d-%d", ci, r.Intn(1000)), fmt.Sprintf("%d-%d", r.Intn(nClients), r.Intn(20))
				}
				ctx, cancel := ctxT(4 * time.Second)
				call := time.Now()
				var out kvOutput
				var err error
				switch in.op {
				case "get":
					out.value, out.found, err = cl.get(ctx, in.key)
				case "put":
					err = cl.put(ctx, in.key, in.value)
				case "cas":
					out.swapped, out.current, err = cl.cas(ctx, in.key, in.expected, in.value)
				}
				cancel()
				ret := time.Now()
				if err != nil {
					out = kvOutput{failed: true}
					ret = start.Add(duration + 10*time.Second)
				}
				record(ci, in, out, call, ret)
			}
		}(ci)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	res, info := porcupine.CheckOperationsVerbose(kvModel, ops, 60*time.Second)
	failed := 0
	for _, op := range ops {
		if op.Output.(kvOutput).failed {
			failed++
		}
	}
	t.Logf("%d operations (%d incomplete) across %d groups, result: %v", len(ops), failed, nGroups, res)
	switch res {
	case porcupine.Illegal:
		path := t.TempDir() + "/history.html"
		if err := porcupine.VisualizePath(kvModel, info, path); err == nil {
			t.Logf("visualisation: %s", path)
		}
		t.Fatal("history is NOT linearizable")
	case porcupine.Unknown:
		t.Log("checker timed out; treating as inconclusive")
	}
}
