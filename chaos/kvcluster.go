package chaos

// kvCluster wraps a real, Raft-replicated raftkv cluster over a seeded
// simnet.Net so it can be crashed, partitioned, and restarted by the
// nemesis, and serves every replica over a real loopback gRPC listener so
// that ALL harness traffic goes through the production client, kv/client
// (its retry loop, leader hints, attempt timeouts, and per-identity request
// numbering), exactly as cmd/sched, cmd/gateway and cmd/kvctl use it:
//
//	workload goroutine ── kv/client Session ──gRPC──▶ nodeProxy[i] ──▶ raftkv.Server[i] ──simnet──▶ peers
//	sched.Queue / gateway ── sched/kvadapter (pool of Sessions) ──┘
//
// The nemesis's network faults stay where they were: on Raft peer traffic
// (simnet). The client<->replica hop is loopback gRPC with one optional,
// harness-injected fault of its own (reply loss, below), because that is
// the fault that forces a mutation to be retried with the SAME RequestMeta
// after it was already applied: the case the store's dedup table exists for.
//
// nodeProxy forwards to whichever raftkv.Server instance currently occupies
// slot i, so a replica's address is stable across crash/restart (a crashed
// slot answers Unavailable, which is what a refused connection looks like to
// kv/client). Every Server is built with Config.Addrs set to those
// addresses, so a follower's "not leader leader=<addr>" hint is a real,
// dialable address and kv/client's leader-jump path runs.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/client"
	"dsys/kv/raftkv"
	"dsys/kvapi"
	"dsys/raft"
	"dsys/raft/simnet"
	"dsys/sched/kvadapter"
)

// kvClusterOptions configures the client-facing side of a kvCluster.
type kvClusterOptions struct {
	// seed seeds the reply-loss coin. Which replies are lost is NOT pinned
	// by it (the draws are taken in goroutine-arrival order), only the
	// stream of coin flips.
	seed int64
	// replyLoss is the probability that a SUCCESSFUL mutation's reply is
	// replaced by Unavailable on its way back to the client. The mutation
	// has been applied; kv/client must retry it with the same RequestMeta
	// and the store must answer from its dedup table rather than apply it
	// a second time.
	replyLoss float64
	// attemptTimeout is kv/client's per-attempt bound (WithAttemptTimeout).
	attemptTimeout time.Duration

	// freshMetaPerDelivery is a TEST-ONLY injected bug, never set by Run's
	// public Scenario: it rewrites every delivered mutation's RequestMeta to
	// a brand-new identity, i.e. it simulates a client that rebuilds its
	// request on each retry (the exact bug kv/client's doOn comment warns
	// about). Combined with replyLoss it turns one Put/CAS into several,
	// and the linearizability check must notice. See
	// TestCheckerCatchesDuplicateApply.
	freshMetaPerDelivery bool
}

type kvCluster struct {
	n          int
	net        *simnet.Net
	cfg        raftkv.Config
	mu         sync.Mutex
	servers    []*raftkv.Server
	persisters []*raft.MemPersister

	partitioned []bool // nemesis bookkeeping, guarded by mu
	crashed     []bool

	addrs []string
	gsrv  []*grpc.Server
	// client is the production root client over every replica's address.
	// Workload goroutines take a Session each; sched and the gateway take a
	// kvadapter pool each, exactly like their cmd/ binaries.
	client *client.Client

	opts    kvClusterOptions
	lossMu  sync.Mutex
	lossRng *rand.Rand

	stats deliveryStats
}

// deliveryStats is what the client-facing proxies observed. It is how the
// harness proves the dedup path actually ran (redeliveries > 0) instead of
// assuming it did.
type deliveryStats struct {
	mu         sync.Mutex
	seen       map[string]struct{} // "clientID/requestID" of every delivered mutation
	identities map[string]struct{}

	mutations    atomic.Int64 // mutation deliveries (attempts that reached a live replica)
	redeliveries atomic.Int64 // deliveries of a (client_id, request_id) already delivered before
	repliesLost  atomic.Int64 // successful replies the harness replaced with Unavailable
	// staleLive counts ErrStaleRequest (FailedPrecondition) answers whose
	// caller was still waiting: a later request id of the same identity was
	// applied while an earlier one was still outstanding. kv/client's
	// one-outstanding-mutation-per-identity rule makes that impossible, so
	// any non-zero value is a violation. (A stale answer to an attempt
	// whose caller already gave up is legitimate and not counted.)
	staleLive atomic.Int64
}

// newKVCluster starts n raftkv replicas over net, each behind its own
// loopback gRPC listener, and a production kv/client over all of them.
func newKVCluster(n int, net *simnet.Net, cfg raftkv.Config, opts kvClusterOptions) (*kvCluster, error) {
	if opts.attemptTimeout <= 0 {
		opts.attemptTimeout = time.Second
	}
	c := &kvCluster{
		n: n, net: net, cfg: cfg,
		servers:     make([]*raftkv.Server, n),
		persisters:  make([]*raft.MemPersister, n),
		partitioned: make([]bool, n),
		crashed:     make([]bool, n),
		opts:        opts,
		lossRng:     rand.New(rand.NewSource(opts.seed)),
	}
	c.stats.seen = map[string]struct{}{}
	c.stats.identities = map[string]struct{}{}
	for i := 0; i < n; i++ {
		lis, err := listenLoopback()
		if err != nil {
			c.killAll()
			return nil, fmt.Errorf("chaos: listen for replica %d: %w", i, err)
		}
		gs := grpc.NewServer()
		kvv1.RegisterKVServer(gs, &nodeProxy{c: c, i: i})
		go func() { _ = gs.Serve(lis) }()
		c.gsrv = append(c.gsrv, gs)
		c.addrs = append(c.addrs, lis.Addr().String())
	}
	c.cfg.Addrs = append([]string(nil), c.addrs...) // real, dialable leader hints
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewMemPersister()
		c.start(i)
	}
	c.client = client.NewFlat(c.addrs, client.WithAttemptTimeout(opts.attemptTimeout))
	return c, nil
}

func listenLoopback() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

// newAdapter returns a sched.KV / kvapi.KV over this cluster's production
// client: the same sched/kvadapter (a bounded pool of kv/client Sessions)
// that cmd/sched and cmd/gateway wrap their client in.
func (c *kvCluster) newAdapter() kvapi.KV { return kvadapter.New(c.client) }

func (c *kvCluster) start(i int) {
	peers := make([]raft.Peer, c.n)
	for j := 0; j < c.n; j++ {
		if j != i {
			peers[j] = c.net.Peer(i, j)
		}
	}
	c.mu.Lock()
	s := raftkv.New(peers, i, c.persisters[i], c.cfg)
	c.servers[i] = s
	c.crashed[i] = false
	c.mu.Unlock()
	c.net.Bind(i, s.Raft())
	if !c.isPartitioned(i) {
		c.net.Connect(i)
	}
}

func (c *kvCluster) crash(i int) {
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	c.persisters[i] = c.persisters[i].Copy()
	c.crashed[i] = true
	c.mu.Unlock()
	c.net.Disconnect(i)
	if s != nil {
		s.Kill()
	}
}

func (c *kvCluster) partition(i int) {
	c.mu.Lock()
	c.partitioned[i] = true
	c.mu.Unlock()
	c.net.Disconnect(i)
}

func (c *kvCluster) heal(i int) {
	c.mu.Lock()
	c.partitioned[i] = false
	crashed := c.crashed[i]
	c.mu.Unlock()
	if !crashed {
		c.net.Connect(i)
	}
}

func (c *kvCluster) isPartitioned(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.partitioned[i]
}

func (c *kvCluster) isCrashed(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed[i]
}

func (c *kvCluster) server(i int) *raftkv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

// killAll stops the client, the listeners, and every replica. Safe to call
// on a partially constructed cluster.
func (c *kvCluster) killAll() {
	if c.client != nil {
		_ = c.client.Close()
	}
	for _, gs := range c.gsrv {
		gs.Stop()
	}
	for i := 0; i < c.n; i++ {
		if s := c.server(i); s != nil {
			s.Kill()
		}
	}
}

// loseReply flips the reply-loss coin.
func (c *kvCluster) loseReply() bool {
	if c.opts.replyLoss <= 0 {
		return false
	}
	c.lossMu.Lock()
	defer c.lossMu.Unlock()
	return c.lossRng.Float64() < c.opts.replyLoss
}

// deliver records one mutation delivery and returns the meta to forward
// (rewritten only under the test-only freshMetaPerDelivery bug).
func (c *kvCluster) deliver(meta *kvv1.RequestMeta) *kvv1.RequestMeta {
	c.stats.mutations.Add(1)
	if meta != nil {
		k := fmt.Sprintf("%s/%d", meta.ClientId, meta.RequestId)
		c.stats.mu.Lock()
		if _, dup := c.stats.seen[k]; dup {
			c.stats.redeliveries.Add(1)
		} else {
			c.stats.seen[k] = struct{}{}
		}
		c.stats.identities[meta.ClientId] = struct{}{}
		c.stats.mu.Unlock()
	}
	if c.opts.freshMetaPerDelivery {
		c.lossMu.Lock()
		id := fmt.Sprintf("injected-%016x", c.lossRng.Uint64())
		c.lossMu.Unlock()
		return &kvv1.RequestMeta{ClientId: id, RequestId: 1}
	}
	return meta
}

// settle post-processes a mutation's outcome: count live stale answers and
// apply reply loss to successes.
func (c *kvCluster) settle(ctx context.Context, err error) error {
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition && ctx.Err() == nil {
			c.stats.staleLive.Add(1)
		}
		return err
	}
	if c.loseReply() {
		c.stats.repliesLost.Add(1)
		return status.Error(codes.Unavailable, "chaos: reply lost after the mutation was applied")
	}
	return nil
}

// nodeProxy is replica i's gRPC face: it forwards to whatever raftkv.Server
// currently occupies slot i.
type nodeProxy struct {
	kvv1.UnimplementedKVServer
	c *kvCluster
	i int
}

var errCrashed = errors.New("replica is crashed")

func (p *nodeProxy) srv() (*raftkv.Server, error) {
	if s := p.c.server(p.i); s != nil {
		return s, nil
	}
	return nil, status.Errorf(codes.Unavailable, "chaos: node %d: %v", p.i, errCrashed)
}

func (p *nodeProxy) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	s, err := p.srv()
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, req) // ReadIndex on the server: no log entry, no meta
}

func (p *nodeProxy) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	s, err := p.srv()
	if err != nil {
		return nil, err
	}
	req.Meta = p.c.deliver(req.Meta)
	resp, err := s.Put(ctx, req)
	if err = p.c.settle(ctx, err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *nodeProxy) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	s, err := p.srv()
	if err != nil {
		return nil, err
	}
	req.Meta = p.c.deliver(req.Meta)
	resp, err := s.Delete(ctx, req)
	if err = p.c.settle(ctx, err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *nodeProxy) CompareAndSwap(ctx context.Context, req *kvv1.CASRequest) (*kvv1.CASResponse, error) {
	s, err := p.srv()
	if err != nil {
		return nil, err
	}
	req.Meta = p.c.deliver(req.Meta)
	resp, err := s.CompareAndSwap(ctx, req)
	if err = p.c.settle(ctx, err); err != nil {
		return nil, err
	}
	return resp, nil
}
