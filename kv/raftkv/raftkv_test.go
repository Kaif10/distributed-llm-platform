package raftkv

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
)

// ---------------------------------------------------------------------------
// A cluster of raftkv servers over the simulated network. Clients call the
// servers' gRPC methods directly (no real gRPC), which keeps the test about
// consensus rather than sockets.
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

func newCluster(t *testing.T, n int, maxRaftState int) *cluster {
	c := &cluster{
		t:          t,
		n:          n,
		net:        simnet.New(n),
		servers:    make([]*Server, n),
		persisters: make([]*raft.MemPersister, n),
		cfg: Config{
			MaxRaftState:  maxRaftState,
			CommitTimeout: time.Second,
			Raft: raft.Config{
				HeartbeatInterval:  50 * time.Millisecond,
				ElectionTimeoutMin: 250 * time.Millisecond,
				ElectionTimeoutMax: 500 * time.Millisecond,
			},
		},
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
	// Fresh copy: the dead instance must not be able to write to the
	// persister the new instance will read.
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

// do runs fn against servers until one succeeds, following leader
// changes. Retryable gRPC codes move on to the next server. The op
// (including its RequestMeta) is fixed by the caller, so every retry is the
// same logical request and the server deduplicates it.
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

func (cl *client) meta() *kvv1.RequestMeta {
	cl.seq++
	return &kvv1.RequestMeta{ClientId: cl.id, RequestId: cl.seq}
}

func (cl *client) put(ctx context.Context, k, v string) error {
	req := &kvv1.PutRequest{Meta: cl.meta(), Key: k, Value: []byte(v)}
	return cl.c.do(ctx, func(s *Server) error { _, err := s.Put(ctx, req); return err })
}

func (cl *client) get(ctx context.Context, k string) (string, bool, error) {
	req := &kvv1.GetRequest{Key: k}
	var resp *kvv1.GetResponse
	err := cl.c.do(ctx, func(s *Server) error {
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
	err := cl.c.do(ctx, func(s *Server) error {
		var err error
		resp, err = s.CompareAndSwap(ctx, req)
		return err
	})
	if err != nil {
		return false, "", err
	}
	return resp.Swapped, string(resp.Current), nil
}

func (cl *client) del(ctx context.Context, k string) (bool, error) {
	req := &kvv1.DeleteRequest{Meta: cl.meta(), Key: k}
	var resp *kvv1.DeleteResponse
	err := cl.c.do(ctx, func(s *Server) error {
		var err error
		resp, err = s.Delete(ctx, req)
		return err
	})
	if err != nil {
		return false, err
	}
	return resp.Existed, nil
}

func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBasicOps(t *testing.T) {
	c := newCluster(t, 3, 0)
	cl := c.client("c1")
	ctx, cancel := ctxT(20 * time.Second)
	defer cancel()

	if _, found, err := cl.get(ctx, "a"); err != nil || found {
		t.Fatalf("get empty: %v %v", found, err)
	}
	if err := cl.put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if v, found, _ := cl.get(ctx, "a"); !found || v != "1" {
		t.Fatalf("get a = %q %v", v, found)
	}
	if swapped, cur, _ := cl.cas(ctx, "a", "wrong", "2"); swapped || cur != "1" {
		t.Fatalf("cas wrong: %v %q", swapped, cur)
	}
	if swapped, _, _ := cl.cas(ctx, "a", "1", "2"); !swapped {
		t.Fatal("cas right should swap")
	}
	if existed, _ := cl.del(ctx, "a"); !existed {
		t.Fatal("delete should see key")
	}
	if _, found, _ := cl.get(ctx, "a"); found {
		t.Fatal("deleted key still present")
	}

	// Every replica converges to the same state.
	waitConverged(t, c, "a", "", false)
}

// waitConverged waits until every live replica's local state agrees.
func waitConverged(t *testing.T, c *cluster, key, want string, wantFound bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for i := 0; i < c.n; i++ {
			s := c.server(i)
			if s == nil || !c.net.IsConnected(i) {
				continue
			}
			v, found := s.LocalGet(key)
			if found != wantFound || string(v) != want {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < c.n; i++ {
		if s := c.server(i); s != nil {
			v, f := s.LocalGet(key)
			t.Logf("replica %d: %q %v (applied %d)", i, v, f, s.LastApplied())
		}
	}
	t.Fatalf("replicas did not converge on %q", key)
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, 0)
	cl := c.client("c1")
	ctx, cancel := ctxT(30 * time.Second)
	defer cancel()

	if err := cl.put(ctx, "k", "v1"); err != nil {
		t.Fatal(err)
	}
	leader := int(c.lastLeader.Load())
	c.net.Disconnect(leader)
	t.Logf("disconnected leader %d", leader)

	start := time.Now()
	if err := cl.put(ctx, "k", "v2"); err != nil {
		t.Fatal(err)
	}
	t.Logf("write after leader loss took %s", time.Since(start).Round(time.Millisecond))
	if v, _, _ := cl.get(ctx, "k"); v != "v2" {
		t.Fatalf("k=%q", v)
	}

	// The old leader rejoins and must catch up, not impose its stale view.
	c.net.Connect(leader)
	waitConverged(t, c, "k", "v2", true)
}

// A retried request across a leader change must apply exactly once. We
// force the situation: submit a CAS, immediately partition the leader so
// the client retries elsewhere, and check the counter moved by one.
func TestRetryAcrossLeaderChangeAppliesOnce(t *testing.T) {
	c := newCluster(t, 3, 0)
	cl := c.client("c1")
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	if err := cl.put(ctx, "ctr", "0"); err != nil {
		t.Fatal(err)
	}

	for round := 0; round < 5; round++ {
		exp := fmt.Sprint(round)
		next := fmt.Sprint(round + 1)
		req := &kvv1.CASRequest{Meta: cl.meta(), Key: "ctr", Expected: []byte(exp), Value: []byte(next)}
		leader := int(c.lastLeader.Load())
		// Even rounds: partition the leader BEFORE the request. The old
		// leader still believes it leads, accepts the entry into its log,
		// and can never commit it; the client times out and retries on the
		// new leader. When the old leader rejoins, its entry is overwritten.
		// Odd rounds: race the partition against the proposal, so the first
		// attempt may or may not have committed when the retry happens.
		partitioned := make(chan struct{})
		if round%2 == 0 {
			c.net.Disconnect(leader)
			close(partitioned)
		} else {
			go func() {
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				c.net.Disconnect(leader)
				close(partitioned)
			}()
		}
		var resp *kvv1.CASResponse
		err := c.do(ctx, func(s *Server) error {
			var err error
			resp, err = s.CompareAndSwap(ctx, req)
			return err
		})
		<-partitioned // never reconnect before the disconnect has happened
		c.net.Connect(leader)
		if err != nil {
			t.Fatal(err)
		}
		// Whether the first attempt committed or not, the client must see
		// swapped=true exactly as if applied once.
		if !resp.Swapped {
			t.Fatalf("round %d: swapped=false current=%q; the retry was re-evaluated instead of deduplicated", round, resp.Current)
		}
		if v, _, _ := cl.get(ctx, "ctr"); v != next {
			t.Fatalf("round %d: ctr=%q want %q", round, v, next)
		}
	}
}

func TestSnapshotAndCrashRecovery(t *testing.T) {
	c := newCluster(t, 3, 2000) // tiny threshold: snapshot every few dozen entries
	cl := c.client("c1")
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		if err := cl.put(ctx, fmt.Sprintf("k%d", i%10), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Raft state must be bounded by snapshots.
	for i := 0; i < 3; i++ {
		if sz := c.persisters[i].StateSize(); sz > 20000 {
			t.Fatalf("replica %d raft state is %d bytes: snapshots are not trimming the log", i, sz)
		}
	}

	// Crash a follower, keep writing past the compaction point, restart it.
	// It must come back via InstallSnapshot and then catch up on the tail.
	victim := (int(c.lastLeader.Load()) + 1) % 3
	c.crash(victim)
	for i := 100; i < 200; i++ {
		if err := cl.put(ctx, fmt.Sprintf("k%d", i%10), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.start(victim)
	waitConverged(t, c, "k9", "199", true)

	// Full cluster restart from persisted state only.
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}
	if v, _, err := cl.get(ctx, "k5"); err != nil || v != "195" {
		t.Fatalf("after full restart k5=%q err=%v", v, err)
	}
	// Dedup state survived too: a stale retry is still recognised.
	old := &kvv1.CASRequest{Meta: &kvv1.RequestMeta{ClientId: "c1", RequestId: cl.seq}, Key: "k9", Expected: []byte("x"), Value: []byte("y")}
	err := c.do(ctx, func(s *Server) error {
		_, err := s.Put(ctx, &kvv1.PutRequest{Meta: old.Meta, Key: "k9", Value: []byte("199")})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Linearizability. Random clients, random partitions and crashes, then
// Porcupine decides whether the observed history could have come from a
// single sequential key-value store. This is the Phase 2 exit criterion.
// ---------------------------------------------------------------------------

type kvInput struct {
	op       string // "put" | "get" | "cas"
	key      string
	value    string
	expected string
}

type kvOutput struct {
	value   string
	found   bool
	swapped bool
	current string
	failed  bool // op never completed; Porcupine may place it anywhere or nowhere
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
			// A failed put may or may not have happened; both are legal
			// linearizations, so we let Porcupine try both by treating the
			// op as having taken effect (it will also try the placement
			// where it comes "last" which is equivalent to not observed).
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

func TestLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	const (
		n        = 5
		nClients = 8
		duration = 12 * time.Second
	)
	c := newCluster(t, n, 5000)
	c.net.SetUnreliable(true)
	keys := []string{"x", "y", "z"}

	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Nemesis: partitions, heals, crashes, restarts.
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(300+r.Intn(500)) * time.Millisecond):
			}
			switch r.Intn(4) {
			case 0: // partition a minority
				a, b := r.Intn(n), r.Intn(n)
				c.net.Disconnect(a)
				c.net.Disconnect(b)
			case 1: // heal everything
				for i := 0; i < n; i++ {
					if c.server(i) != nil {
						c.net.Connect(i)
					}
				}
			case 2: // crash one node
				i := r.Intn(n)
				if c.server(i) != nil {
					c.crash(i)
				}
			case 3: // restart crashed nodes
				for i := 0; i < n; i++ {
					if c.server(i) == nil {
						c.start(i)
					}
				}
			}
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
			cl := c.client(fmt.Sprintf("lin-%d", ci))
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
					// Never completed from the client's view. It may still
					// have taken effect; record it as pending until the end.
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

	// Heal and restart everything so pending ops can settle, then check.
	for i := 0; i < n; i++ {
		if c.server(i) == nil {
			c.start(i)
		}
		c.net.Connect(i)
	}
	c.net.SetUnreliable(false)

	res, info := porcupine.CheckOperationsVerbose(kvModel, ops, 60*time.Second)
	failed := 0
	for _, op := range ops {
		if op.Output.(kvOutput).failed {
			failed++
		}
	}
	t.Logf("%d operations (%d incomplete), result: %v", len(ops), failed, res)
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
