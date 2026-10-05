package gateway

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	gatewayv1 "dsys/gen/gateway/v1"
	"dsys/kv/client"
	"dsys/ratelimit"
	"dsys/router"
	"dsys/sched/kvadapter"
	"dsys/semcache"
)

// blockingKV hangs every operation until its context is done while block is
// set: what the real kv/client does when there is no Raft leader (it retries
// for up to its whole -kv-timeout), as opposed to failingKV, which fails
// instantly and so hid how long a fail-open actually takes.
type blockingKV struct {
	*memKV
	block atomic.Bool
	calls atomic.Int64 // operations that reached the KV while blocked
}

func (k *blockingKV) wait(ctx context.Context) error {
	k.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func (k *blockingKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if k.block.Load() {
		return nil, false, k.wait(ctx)
	}
	return k.memKV.Get(ctx, key)
}

func (k *blockingKV) Put(ctx context.Context, key string, value []byte) error {
	if k.block.Load() {
		return k.wait(ctx)
	}
	return k.memKV.Put(ctx, key, value)
}

func (k *blockingKV) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	if k.block.Load() {
		return false, nil, k.wait(ctx)
	}
	return k.memKV.CAS(ctx, key, expected, expectAbsent, value)
}

// timedGenerate runs one Generate with a generous outer deadline (so a
// regression fails the test instead of hanging it) and reports how long it
// took.
func timedGenerate(s *Server, req *gatewayv1.GenerateRequest, outer time.Duration) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(bg, outer)
	defer cancel()
	start := time.Now()
	err := s.Generate(req, &nopStream{ctx: ctx})
	return time.Since(start), err
}

// A KV that hangs (no leader) must not stall requests: the limiter fails
// open, the cache is skipped and the registry serves its last known set,
// all within a small bound, instead of each waiting out the KV client's
// retry budget.
func TestHangingKVFailsOpenQuickly(t *testing.T) {
	kv := &blockingKV{memKV: newMemKV()}
	reg := router.NewRegistry(kv, router.Options{CacheTTL: time.Millisecond})
	if _, err := reg.Register(bg, router.Worker{ID: "w0", Addr: startMockWorker(t, "w0")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	lim := ratelimit.New(kv, ratelimit.Options{Rate: 100, Burst: 100})
	cache := semcache.New(kv, semcache.NewNGramEmbedder(0), semcache.Options{})
	s := New(Options{KV: kv, Registry: reg, Limiter: lim, Cache: cache, PrefixRouting: true, RateLimitFailOpen: true})
	defer s.Close()
	req := &gatewayv1.GenerateRequest{Tenant: "t", Prompt: "p", MaxTokens: 2}
	if _, err := timedGenerate(s, req, 5*time.Second); err != nil {
		t.Fatalf("KV healthy: %v", err)
	}

	kv.block.Store(true)
	time.Sleep(5 * time.Millisecond) // past the registry's CacheTTL
	for i := 0; i < 3; i++ {
		took, err := timedGenerate(s, req, 5*time.Second)
		if err != nil {
			t.Fatalf("KV hanging: request %d failed after %v: %v", i, took, err)
		}
		// Worst case before the breaker opens: registry read, limiter,
		// lookup and store each spend the 300ms default budget (1.2s; the
		// client has its done token before the store). Once it opens, a
		// request touches the KV not at all.
		if took > 2*time.Second {
			t.Fatalf("KV hanging: request %d took %v, want < 2s", i, took)
		}
	}
	if n := s.Snapshot().RateLimitFailOpen; n != 3 {
		t.Fatalf("RateLimitFailOpen=%d, want 3", n)
	}
}

// The same outage against the REAL kv/client (through kvadapter, exactly as
// cmd/gateway wires it) pointed at an address nothing listens on: the client
// retries for its whole WithTimeout before returning an error, which is the
// stall the per-call budget exists to cut short.
func TestUnreachableRealKVClientFailsOpenQuickly(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := lis.Addr().String()
	_ = lis.Close()
	kvc := client.NewFlat([]string{dead}, client.WithTimeout(10*time.Second))
	defer kvc.Close()
	kv := kvadapter.New(kvc, kvadapter.WithSessions(4))

	// The registry lives on a healthy KV here: this test is about the
	// limiter and the cache, the two KV calls every request makes.
	regKV := newMemKV()
	reg := router.NewRegistry(regKV, router.Options{})
	if _, err := reg.Register(bg, router.Worker{ID: "w0", Addr: startMockWorker(t, "w0")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	lim := ratelimit.New(kv, ratelimit.Options{Rate: 100, Burst: 100})
	cache := semcache.New(kv, semcache.NewNGramEmbedder(0), semcache.Options{})
	s := New(Options{KV: regKV, Registry: reg, Limiter: lim, Cache: cache, PrefixRouting: true, RateLimitFailOpen: true})
	defer s.Close()

	took, err := timedGenerate(s, &gatewayv1.GenerateRequest{Tenant: "t", Prompt: "p", MaxTokens: 2}, 30*time.Second)
	if err != nil {
		t.Fatalf("KV unreachable: request failed after %v: %v", took, err)
	}
	if took > 2*time.Second {
		t.Fatalf("KV unreachable: request took %v, want < 2s", took)
	}
}

// After KVBreakerThreshold consecutive failures the breaker opens and
// requests stop touching the KV at all; once the KV is back, the first
// request after the cool-down probes it and closes the breaker, and the
// cache works again.
func TestKVBreakerOpensAndRecovers(t *testing.T) {
	kv := &blockingKV{memKV: newMemKV()}
	reg := router.NewRegistry(kv, router.Options{CacheTTL: time.Minute})
	if _, err := reg.Register(bg, router.Worker{ID: "w0", Addr: startMockWorker(t, "w0")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	lim := ratelimit.New(kv, ratelimit.Options{Rate: 100, Burst: 100})
	const cooldown = 200 * time.Millisecond
	s := New(Options{
		KV: kv, Registry: reg, Limiter: lim, Cache: &kvCache{kv: kv}, PrefixRouting: true, RateLimitFailOpen: true,
		KVCallTimeout: 50 * time.Millisecond, KVBreakerThreshold: 3, KVBreakerCooldown: cooldown,
	})
	defer s.Close()
	// Warm the registry's last known set; with a one-minute CacheTTL it
	// makes no KV calls below, so kv.calls counts only limiter and cache.
	if _, err := reg.Live(bg); err != nil {
		t.Fatal(err)
	}

	kv.block.Store(true)
	req := func(prompt string) *gatewayv1.GenerateRequest {
		return &gatewayv1.GenerateRequest{Tenant: "t", Prompt: prompt, MaxTokens: 2}
	}
	// Request 1: limiter, lookup and store all time out: 3 failures, open.
	if _, err := timedGenerate(s, req("a"), 5*time.Second); err != nil {
		t.Fatalf("first outage request: %v", err)
	}
	if st := s.Snapshot(); st.KVBreakerTrips != 1 {
		t.Fatalf("KVBreakerTrips=%d after 3 failures, want 1", st.KVBreakerTrips)
	}
	// While open, requests skip the KV entirely and are fast.
	before := kv.calls.Load()
	for i := 0; i < 5; i++ {
		took, err := timedGenerate(s, req("b"), 5*time.Second)
		if err != nil || took > 500*time.Millisecond {
			t.Fatalf("breaker open: request %d took %v err %v", i, took, err)
		}
	}
	if n := kv.calls.Load() - before; n != 0 {
		t.Fatalf("breaker open: %d calls reached the KV, want 0", n)
	}
	if st := s.Snapshot(); st.KVSkipped != 15 || st.RateLimitFailOpen != 6 {
		t.Fatalf("breaker open: KVSkipped=%d (want 15) RateLimitFailOpen=%d (want 6)", st.KVSkipped, st.RateLimitFailOpen)
	}

	// The KV comes back. After the cool-down the next request probes it,
	// the breaker closes, and the cache round-trips through the KV again.
	kv.block.Store(false)
	time.Sleep(cooldown + 50*time.Millisecond)
	for i := 0; i < 2; i++ {
		if _, err := timedGenerate(s, req("c"), 5*time.Second); err != nil {
			t.Fatalf("recovered: request %d: %v", i, err)
		}
	}
	if s.kvBreaker.isOpen() {
		t.Fatal("breaker still open after the KV recovered")
	}
	if st := s.Snapshot(); st.CacheHits != 1 {
		t.Fatalf("recovered: CacheHits=%d, want 1 (the second \"c\" served from the KV-backed cache)", st.CacheHits)
	}

	// If the KV is still down when a cool-down ends, the probe fails and the
	// breaker re-opens, rather than letting every request through to it.
	kv.block.Store(true)
	for i := 0; i < 2; i++ { // the first trips it, the second is skipped
		_, _ = timedGenerate(s, req("d"), 5*time.Second)
	}
	time.Sleep(cooldown + 50*time.Millisecond)
	_, _ = timedGenerate(s, req("e"), 5*time.Second) // the probe, which fails
	if !s.kvBreaker.isOpen() {
		t.Fatal("breaker closed after a failed probe")
	}
	if st := s.Snapshot(); st.KVBreakerTrips != 3 {
		t.Fatalf("KVBreakerTrips=%d, want 3 (trip, trip, failed probe)", st.KVBreakerTrips)
	}
}

// kvCache is an exact-match Cache whose entries live in the test's KV, so a
// KV outage is a cache outage, as it is with semcache.
type kvCache struct{ kv *blockingKV }

func (c *kvCache) Lookup(ctx context.Context, prompt string) (string, bool, error) {
	v, ok, err := c.kv.Get(ctx, "cache/"+prompt)
	return string(v), ok, err
}

func (c *kvCache) Store(ctx context.Context, prompt, text string) error {
	return c.kv.Put(ctx, "cache/"+prompt, []byte(text))
}
