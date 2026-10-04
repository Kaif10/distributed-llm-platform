package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gatewayv1 "dsys/gen/gateway/v1"
	inferv1 "dsys/gen/infer/v1"
	"dsys/infer/mock"
	"dsys/ratelimit"
	"dsys/router"
	"dsys/semcache"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type memKV struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemKV() *memKV { return &memKV{m: map[string][]byte{}} }

func (k *memKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[key]
	return bytes.Clone(v), ok, nil
}

func (k *memKV) Put(_ context.Context, key string, value []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[key] = bytes.Clone(value)
	return nil
}

func (k *memKV) CAS(_ context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.m[key]
	matches := (expectAbsent && !ok) || (!expectAbsent && ok && bytes.Equal(cur, expected))
	if matches {
		k.m[key] = bytes.Clone(value)
		return true, bytes.Clone(cur), nil
	}
	return false, bytes.Clone(cur), nil
}

// fakeCache is a trivial exact-match cache, so gateway tests exercise the
// cache PATH without depending on semcache's similarity behaviour.
type fakeCache struct {
	mu       sync.Mutex
	m        map[string]string
	lookups  atomic.Int64
	stores   atomic.Int64
	failNext atomic.Bool
}

func newFakeCache() *fakeCache { return &fakeCache{m: map[string]string{}} }

func (c *fakeCache) Lookup(_ context.Context, prompt string) (string, bool, error) {
	c.lookups.Add(1)
	if c.failNext.Swap(false) {
		return "", false, fmt.Errorf("cache is down")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[prompt]
	return v, ok, nil
}

func (c *fakeCache) Store(_ context.Context, prompt, text string) error {
	c.stores.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[prompt] = text
	return nil
}

// harness wires N in-process mock workers behind a gateway. Workers are
// served over real loopback gRPC so streaming and cancellation are exercised
// for real, not simulated.
type harness struct {
	t        *testing.T
	kv       *memKV
	reg      *router.Registry
	gw       *Server
	workers  []*mock.Worker
	addrs    []string
	servers  []*grpc.Server
	clientCC *grpc.ClientConn
	client   gatewayv1.GatewayClient
}

func newHarness(t *testing.T, nWorkers int, opts Options, wopts func(i int) mock.Options) *harness {
	t.Helper()
	h := &harness{t: t, kv: newMemKV()}
	h.reg = router.NewRegistry(h.kv, router.Options{CacheTTL: time.Millisecond})

	for i := 0; i < nWorkers; i++ {
		mo := mock.Options{ID: fmt.Sprintf("w%d", i)}
		if wopts != nil {
			mo = wopts(i)
			if mo.ID == "" {
				mo.ID = fmt.Sprintf("w%d", i)
			}
		}
		w := mock.New(mo)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		gs := grpc.NewServer()
		inferv1.RegisterInferenceServer(gs, w)
		go func() { _ = gs.Serve(lis) }()
		h.workers = append(h.workers, w)
		h.addrs = append(h.addrs, lis.Addr().String())
		h.servers = append(h.servers, gs)
		if _, err := h.reg.Register(context.Background(), router.Worker{ID: w.ID(), Addr: lis.Addr().String()}, time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	opts.KV = h.kv
	opts.Registry = h.reg
	h.gw = New(opts)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	gatewayv1.RegisterGatewayServer(gs, h.gw)
	go func() { _ = gs.Serve(lis) }()
	h.servers = append(h.servers, gs)

	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	h.clientCC = cc
	h.client = gatewayv1.NewGatewayClient(cc)

	t.Cleanup(func() {
		_ = cc.Close()
		h.gw.Close()
		for _, s := range h.servers {
			s.Stop()
		}
	})
	return h
}

// collect drains a stream, returning the tokens and the first token's meta.
func collect(t *testing.T, st grpc.ServerStreamingClient[gatewayv1.Token]) (text string, first *gatewayv1.Token, err error) {
	t.Helper()
	var sb strings.Builder
	for {
		tok, rerr := st.Recv()
		if rerr != nil {
			if rerr.Error() == "EOF" {
				return sb.String(), first, nil
			}
			return sb.String(), first, rerr
		}
		if first == nil {
			// Keep the pointer: a protobuf message must not be copied by
			// value (it carries internal state), and the stream hands us a
			// fresh message each Recv anyway.
			first = tok
		}
		sb.WriteString(tok.Text)
		if tok.Done {
			return sb.String(), first, nil
		}
	}
}

func (h *harness) generate(ctx context.Context, prompt, tenant string, maxTokens int32, noCache bool) (string, *gatewayv1.Token, error) {
	st, err := h.client.Generate(ctx, &gatewayv1.GenerateRequest{
		Tenant: tenant, Prompt: prompt, MaxTokens: maxTokens, NoCache: noCache,
	})
	if err != nil {
		return "", nil, err
	}
	return collect(h.t, st)
}

var bg = context.Background()

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestGenerateStreamsTokens(t *testing.T) {
	h := newHarness(t, 1, Options{PrefixRouting: true}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	text, first, err := h.generate(bg, "hello world", "t1", 5, true)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.Worker != "w0" {
		t.Fatalf("first token %+v", first)
	}
	if len(strings.Fields(text)) != 5 {
		t.Fatalf("got %d words: %q", len(strings.Fields(text)), text)
	}
	if first.TtftMs < 0 {
		t.Fatalf("ttft %d", first.TtftMs)
	}
	if got := h.gw.Snapshot().Requests; got != 1 {
		t.Fatalf("requests=%d", got)
	}
}

func TestRateLimitRejectsWithRetryHint(t *testing.T) {
	kvForLimiter := newMemKV()
	lim := ratelimit.New(kvForLimiter, ratelimit.Options{Rate: 1, Burst: 2})
	h := newHarness(t, 1, Options{PrefixRouting: true, Limiter: lim}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	for i := 0; i < 2; i++ {
		if _, _, err := h.generate(bg, "p", "t1", 2, true); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	_, _, err := h.generate(bg, "p", "t1", 2, true)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("want ResourceExhausted, got %v", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "retry_after_ms=") {
		t.Fatalf("missing retry hint: %v", err)
	}
	// A different tenant is unaffected: the budget is per tenant.
	if _, _, err := h.generate(bg, "p", "t2", 2, true); err != nil {
		t.Fatalf("other tenant: %v", err)
	}
	if h.gw.Snapshot().RateLimited != 1 {
		t.Fatalf("rateLimited=%d", h.gw.Snapshot().RateLimited)
	}
}

func TestCacheHitSkipsWorkerEntirely(t *testing.T) {
	c := newFakeCache()
	h := newHarness(t, 1, Options{PrefixRouting: true, Cache: c}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	text1, first1, err := h.generate(bg, "what is raft", "t1", 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if first1.Cached {
		t.Fatal("first request should not be a cache hit")
	}
	before := h.workers[0].Stats().Requests

	text2, first2, err := h.generate(bg, "what is raft", "t1", 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if !first2.Cached {
		t.Fatal("second identical request should be served from cache")
	}
	if text2 != text1 {
		t.Fatalf("cached text %q != original %q", text2, text1)
	}
	if after := h.workers[0].Stats().Requests; after != before {
		t.Fatalf("worker was called on a cache hit: %d -> %d", before, after)
	}
	if h.gw.Snapshot().CacheHits != 1 {
		t.Fatalf("cacheHits=%d", h.gw.Snapshot().CacheHits)
	}

	// no_cache bypasses the cache and reaches the worker.
	if _, first3, err := h.generate(bg, "what is raft", "t1", 4, true); err != nil || first3.Cached {
		t.Fatalf("no_cache should bypass: %+v %v", first3, err)
	}
}

// A cache failure must degrade to a normal generation, never fail the request.
func TestCacheFailureDoesNotFailRequest(t *testing.T) {
	c := newFakeCache()
	h := newHarness(t, 1, Options{PrefixRouting: true, Cache: c}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	c.failNext.Store(true)
	_, first, err := h.generate(bg, "prompt", "t1", 3, false)
	if err != nil {
		t.Fatalf("a cache error must not fail the request: %v", err)
	}
	if first.Cached {
		t.Fatal("should have gone to the worker")
	}
}

// Prefix routing: requests sharing a long prefix must land on ONE worker,
// and that worker's prefix cache must then report hits.
//
// The comparison has to run CONCURRENTLY to be honest. Sequentially, every
// worker is idle when each request arrives, so least-loaded routing keeps
// picking the same worker by tie-break and looks identical to affinity
// routing. It is exactly under concurrent load, when inflight counts
// differ, that least-loaded spreads a shared prefix across workers and
// throws the prefix cache away. That is also when it matters in production.
func TestPrefixRoutingGivesAffinityAndCacheHits(t *testing.T) {
	const nWorkers, nReq = 4, 16
	sys := strings.Repeat("You are a careful assistant. ", 12) // > 256 chars

	run := func(prefixRouting bool) (workers map[string]int, hits int) {
		h := newHarness(t, nWorkers, Options{
			PrefixRouting: prefixRouting, PrefixChars: 256, MaxInflightPerWorker: 100,
		}, func(int) mock.Options {
			return mock.Options{BasePrefillMs: 5, PrefillPerChar: 0.02, TokenMs: 1}
		})
		// Warm one worker's prefix cache so there is something to hit.
		if _, _, err := h.generate(bg, sys+"warmup", "t1", 2, true); err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		workers = map[string]int{}
		var wg sync.WaitGroup
		for i := 0; i < nReq; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, first, err := h.generate(bg, sys+fmt.Sprintf("Question %d?", i), "t1", 2, true)
				if err != nil {
					t.Errorf("request %d: %v", i, err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				workers[first.Worker]++
				if first.PrefixCacheHit {
					hits++
				}
			}(i)
		}
		wg.Wait()
		return workers, hits
	}

	affWorkers, affHits := run(true)
	llWorkers, llHits := run(false)
	t.Logf("prefix routing:  workers=%v prefix-cache hits=%d/%d", affWorkers, affHits, nReq)
	t.Logf("least-loaded:    workers=%v prefix-cache hits=%d/%d", llWorkers, llHits, nReq)

	if len(affWorkers) != 1 {
		t.Fatalf("a shared prefix was spread across %d workers under prefix routing: %v", len(affWorkers), affWorkers)
	}
	if affHits != nReq {
		t.Fatalf("prefix routing gave only %d/%d cache hits", affHits, nReq)
	}
	if len(llWorkers) < 2 {
		t.Fatalf("least-loaded routing did not spread under concurrency: %v", llWorkers)
	}
	if affHits <= llHits {
		t.Fatalf("prefix routing (%d hits) did not beat least-loaded (%d hits)", affHits, llHits)
	}
}

// Hedging: one worker always stalls; with hedging on, p99 should be rescued
// by the second attempt, and the stalled attempt must be cancelled.
func TestHedgingRescuesAStalledWorker(t *testing.T) {
	h := newHarness(t, 2, Options{
		PrefixRouting: false, // least-loaded so we can steer deterministically
		HedgeAfter:    60 * time.Millisecond,
	}, func(i int) mock.Options {
		if i == 0 {
			// w0 always stalls badly.
			return mock.Options{ID: "w0", BasePrefillMs: 400, PrefillPerChar: 0, TokenMs: 1}
		}
		return mock.Options{ID: "w1", BasePrefillMs: 2, PrefillPerChar: 0, TokenMs: 1}
	})

	start := time.Now()
	_, first, err := h.generate(bg, "hedge me", "t1", 3, true)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if !first.Hedged {
		t.Fatalf("expected a hedge to be launched, got %+v", first)
	}
	if first.Worker != "w1" || !first.HedgeWon {
		t.Fatalf("expected the fast hedge (w1) to win, got worker=%s hedgeWon=%v", first.Worker, first.HedgeWon)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("hedged request took %v; the stall was not bypassed", elapsed)
	}
	st := h.gw.Snapshot()
	if st.HedgesLaunched != 1 || st.HedgesWon != 1 {
		t.Fatalf("stats %+v", st)
	}
	// The loser must have been cancelled, not left generating.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.workers[0].Stats().Cancelled >= 1 {
			t.Logf("stalled worker cancelled after losing the hedge race")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the losing attempt was never cancelled: it is still burning compute")
}

// Client cancellation must reach the worker.
func TestClientCancelPropagatesToWorker(t *testing.T) {
	h := newHarness(t, 1, Options{PrefixRouting: true}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 30}
	})
	ctx, cancel := context.WithCancel(bg)
	st, err := h.client.Generate(ctx, &gatewayv1.GenerateRequest{
		Tenant: "t1", Prompt: "long stream", MaxTokens: 200, NoCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := st.Recv(); err != nil {
			t.Fatalf("token %d: %v", i, err)
		}
	}
	cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.workers[0].Stats().Cancelled >= 1 {
			emitted := h.workers[0].Stats().TokensEmitted
			if emitted > 100 {
				t.Fatalf("worker emitted %d tokens after cancel; it kept generating", emitted)
			}
			t.Logf("worker stopped after %d tokens on client cancel", emitted)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker never observed the cancellation")
}

// Every gateway replica must see the same worker set and enforce one budget.
func TestTwoGatewaysShareRegistryAndBudget(t *testing.T) {
	kv := newMemKV()
	reg := router.NewRegistry(kv, router.Options{CacheTTL: time.Millisecond})
	w := mock.New(mock.Options{ID: "shared", BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	inferv1.RegisterInferenceServer(gs, w)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	if _, err := reg.Register(bg, router.Worker{ID: "shared", Addr: lis.Addr().String()}, time.Minute); err != nil {
		t.Fatal(err)
	}

	lim := ratelimit.New(kv, ratelimit.Options{Rate: 0.0001, Burst: 3})
	mk := func() *Server {
		return New(Options{KV: kv, Registry: reg, Limiter: lim, PrefixRouting: true})
	}
	a, b := mk(), mk()
	defer a.Close()
	defer b.Close()

	// Three requests total across the two gateways, then both refuse.
	admitted := 0
	for i := 0; i < 6; i++ {
		srv := a
		if i%2 == 1 {
			srv = b
		}
		err := srv.Generate(&gatewayv1.GenerateRequest{Tenant: "t", Prompt: "p", MaxTokens: 1, NoCache: true}, &nopStream{ctx: bg})
		if err == nil {
			admitted++
		} else if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if admitted != 3 {
		t.Fatalf("two gateways admitted %d requests against a shared burst of 3", admitted)
	}
}

// nopStream is a minimal ServerStreamingServer that discards tokens.
type nopStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (n *nopStream) Send(*gatewayv1.Token) error { return nil }
func (n *nopStream) Context() context.Context    { return n.ctx }

func TestNoWorkersIsUnavailable(t *testing.T) {
	kv := newMemKV()
	reg := router.NewRegistry(kv, router.Options{CacheTTL: time.Millisecond})
	s := New(Options{KV: kv, Registry: reg, PrefixRouting: true})
	defer s.Close()
	err := s.Generate(&gatewayv1.GenerateRequest{Prompt: "p", MaxTokens: 1, NoCache: true}, &nopStream{ctx: bg})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
}

// A dead worker (registered but not listening) must not sink requests: the
// gateway falls back to another worker.
func TestFailoverToAnotherWorker(t *testing.T) {
	h := newHarness(t, 2, Options{PrefixRouting: true}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	// Kill w0's server but leave its registration in place.
	h.servers[0].Stop()

	okCount := 0
	for i := 0; i < 6; i++ {
		if _, first, err := h.generate(bg, fmt.Sprintf("prompt-%d", i), "t1", 2, true); err == nil {
			okCount++
			if first.Worker == "w0" {
				t.Fatalf("request %d claims to have been served by the dead worker", i)
			}
		}
	}
	if okCount != 6 {
		t.Fatalf("only %d/6 requests survived one dead worker", okCount)
	}
}

func TestRegisterWorkerRPC(t *testing.T) {
	h := newHarness(t, 1, Options{PrefixRouting: true}, nil)
	resp, err := h.client.RegisterWorker(bg, &gatewayv1.RegisterWorkerRequest{
		WorkerId: "late", Addr: "127.0.0.1:1", LeaseMs: 5000, Model: "m", Inflight: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.LeaseUntilMs <= time.Now().UnixMilli() {
		t.Fatalf("lease in the past: %d", resp.LeaseUntilMs)
	}
	live, _ := h.reg.Live(bg)
	found := false
	for _, w := range live {
		if w.ID == "late" && w.Inflight == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("registered worker missing: %+v", live)
	}
	if _, err := h.client.RegisterWorker(bg, &gatewayv1.RegisterWorkerRequest{WorkerId: ""}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestStatsRPC(t *testing.T) {
	h := newHarness(t, 1, Options{PrefixRouting: true}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	if _, _, err := h.generate(bg, "p", "t1", 2, true); err != nil {
		t.Fatal(err)
	}
	st, err := h.client.Stats(bg, &gatewayv1.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Requests != 1 || st.LiveWorkers != 1 || st.RoutedTo["w0"] != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// Concurrency: many requests at once, mixed tenants and prefixes, with the
// race detector on. Asserts nothing leaks and every request completes.
func TestConcurrentLoad(t *testing.T) {
	h := newHarness(t, 3, Options{
		PrefixRouting: true, MaxInflightPerWorker: 4, HedgeAfter: 50 * time.Millisecond,
	}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 2, PrefillPerChar: 0.01, TokenMs: 2, StallProb: 0.1, StallMs: 120}
	})
	prefixes := []string{
		strings.Repeat("alpha context. ", 20),
		strings.Repeat("beta context. ", 20),
	}
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(bg, 10*time.Second)
			defer cancel()
			p := prefixes[i%len(prefixes)] + fmt.Sprintf("q%d", i)
			if _, _, err := h.generate(ctx, p, fmt.Sprintf("t%d", i%3), 3, true); err != nil {
				failures.Add(1)
				t.Errorf("request %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d requests failed", failures.Load())
	}
	st := h.gw.Snapshot()
	t.Logf("40 concurrent requests: %+v", st)
	if st.Requests != 40 {
		t.Fatalf("requests=%d", st.Requests)
	}
}

// Cache entries must be scoped by tenant: tenant B must never be served a
// completion that was generated (and paid for, and possibly personalised)
// for tenant A, even for a byte-identical prompt.
func TestCacheIsIsolatedPerTenant(t *testing.T) {
	c := newFakeCache()
	h := newHarness(t, 1, Options{PrefixRouting: true, Cache: c}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	const prompt = "summarise my private notes"
	if _, first, err := h.generate(bg, prompt, "tenant-a", 4, false); err != nil || first.Cached {
		t.Fatalf("tenant A first request: first=%+v err=%v", first, err)
	}
	_, first, err := h.generate(bg, prompt, "tenant-b", 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if first.Cached {
		t.Fatal("tenant B was served tenant A's cached completion for the same prompt")
	}
	if _, first, err := h.generate(bg, prompt, "tenant-a", 4, false); err != nil || !first.Cached {
		t.Fatalf("tenant A repeat should be a cache hit: first=%+v err=%v", first, err)
	}
}

// The same isolation against the real semcache with its near path ON, which
// normalises case/whitespace and accepts keys a few edits apart: tenant ids
// that differ by one character or only by case must still not share.
func TestCacheIsIsolatedPerTenantWithRealNearCache(t *testing.T) {
	kv := newMemKV()
	c := semcache.New(kv, semcache.NewNGramEmbedder(0), semcache.Options{Near: true})
	h := newHarness(t, 1, Options{PrefixRouting: true, Cache: c}, func(int) mock.Options {
		return mock.Options{BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1}
	})
	const prompt = "summarise my private notes"
	if _, _, err := h.generate(bg, prompt, "tenant-a", 4, false); err != nil {
		t.Fatal(err)
	}
	for _, other := range []string{"tenant-b", "Tenant-a", "tenant-a "} {
		_, first, err := h.generate(bg, prompt, other, 4, false)
		if err != nil {
			t.Fatal(err)
		}
		if first.Cached {
			t.Fatalf("tenant %q was served tenant-a's cached completion", other)
		}
	}
	// Within a tenant, a one-character typo is still a near hit.
	if _, first, err := h.generate(bg, "summarize my private notes", "tenant-a", 4, false); err != nil || !first.Cached {
		t.Fatalf("same-tenant near duplicate should hit: first=%+v err=%v", first, err)
	}
}

// Regression: with a worker that produces tokens faster than they are
// forwarded (here: instantly, and the client reads slowly), the forwarding
// loop must deliver every token in order and only then cache the full text.
// It used to select between "more tokens" and "worker finished" with both
// ready, pick "finished" at random, return with tokens still buffered, and
// Store the truncated text.
func TestSlowClientGetsEveryTokenAndCacheGetsFullText(t *testing.T) {
	const nTok, iters = 64, 200
	c := newFakeCache()
	h := newHarness(t, 1, Options{PrefixRouting: true, Cache: c}, func(int) mock.Options {
		return mock.Options{
			BasePrefillMs: 1, PrefillPerChar: 0, TokenMs: 1,
			// No real sleeping: the worker emits all 64 tokens at once.
			Sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		}
	})
	truncated, badCache := 0, 0
	for it := 0; it < iters; it++ {
		prompt := fmt.Sprintf("slow client prompt %d", it)
		st, err := h.client.Generate(bg, &gatewayv1.GenerateRequest{
			Tenant: "t1", Prompt: prompt, MaxTokens: nTok,
		})
		if err != nil {
			t.Fatal(err)
		}
		var sb strings.Builder
		next := int32(0)
		sawDone := false
		for {
			tok, err := st.Recv()
			if err != nil {
				break // EOF (or error): stream over
			}
			if tok.Index != next {
				t.Fatalf("iter %d: got token index %d, want %d", it, tok.Index, next)
			}
			next++
			sb.WriteString(tok.Text)
			if tok.Done {
				sawDone = true
				break
			}
			if next%16 == 0 {
				time.Sleep(time.Millisecond) // a slow reader
			}
		}
		complete := next == nTok && sawDone
		if !complete {
			truncated++
			if truncated <= 3 {
				t.Errorf("iter %d: stream truncated: %d/%d tokens, done=%v", it, next, nTok, sawDone)
			}
		}
		// Store runs after the last Send, so give it a moment to land. A
		// truncated stream must not be cached at all; a complete one must be
		// cached byte for byte.
		key := cacheKey("t1", prompt)
		var cached string
		var ok bool
		wait := time.Second
		if !complete {
			wait = 50 * time.Millisecond
		}
		for deadline := time.Now().Add(wait); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			c.mu.Lock()
			cached, ok = c.m[key]
			c.mu.Unlock()
			if ok {
				break
			}
		}
		want := ""
		if complete {
			want = sb.String()
		}
		if ok != complete || cached != want {
			badCache++
			if badCache <= 3 {
				t.Errorf("iter %d: cache has %d bytes (present=%v); stream had %d bytes (complete=%v)", it, len(cached), ok, sb.Len(), complete)
			}
		}
	}
	if truncated > 0 || badCache > 0 {
		t.Fatalf("%d/%d streams truncated, %d/%d cache entries wrong", truncated, iters, badCache, iters)
	}
}

// scriptedClient is an in-process InferenceClient that replays toks and
// then ends the stream with endErr (io.EOF for a clean close).
type scriptedClient struct {
	toks   []*inferv1.Token
	endErr error
}

func (c *scriptedClient) Generate(context.Context, *inferv1.GenerateRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[inferv1.Token], error) {
	return &scriptedStream{toks: c.toks, endErr: c.endErr}, nil
}

func (c *scriptedClient) Health(context.Context, *inferv1.HealthRequest, ...grpc.CallOption) (*inferv1.HealthResponse, error) {
	return &inferv1.HealthResponse{}, nil
}

type scriptedStream struct {
	grpc.ClientStream
	toks   []*inferv1.Token
	endErr error
}

func (s *scriptedStream) Recv() (*inferv1.Token, error) {
	if len(s.toks) == 0 {
		return nil, s.endErr
	}
	t := s.toks[0]
	s.toks = s.toks[1:]
	return t, nil
}

// A worker stream that ends without a done token (EOF early, or an error
// mid-stream) is not a complete answer: the request must fail and nothing
// may be cached.
func TestIncompleteWorkerStreamIsNotCached(t *testing.T) {
	partial := []*inferv1.Token{{Text: "a ", Index: 0}, {Text: "b ", Index: 1}, {Text: "c ", Index: 2}}
	for name, endErr := range map[string]error{
		"eof-without-done": io.EOF,
		"error-mid-stream": status.Error(codes.Internal, "worker blew up"),
	} {
		t.Run(name, func(t *testing.T) {
			kv := newMemKV()
			reg := router.NewRegistry(kv, router.Options{CacheTTL: time.Millisecond})
			if _, err := reg.Register(bg, router.Worker{ID: "w", Addr: "scripted"}, time.Minute); err != nil {
				t.Fatal(err)
			}
			c := newFakeCache()
			s := New(Options{KV: kv, Registry: reg, Cache: c, PrefixRouting: true,
				Dial: func(string) (inferv1.InferenceClient, error) {
					return &scriptedClient{toks: partial, endErr: endErr}, nil
				}})
			defer s.Close()
			err := s.Generate(&gatewayv1.GenerateRequest{Tenant: "t", Prompt: "p", MaxTokens: 8}, &nopStream{ctx: bg})
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("want Unavailable for an incomplete stream, got %v", err)
			}
			if n := c.stores.Load(); n != 0 {
				t.Fatalf("an incomplete stream was cached (%d stores)", n)
			}
		})
	}
}
