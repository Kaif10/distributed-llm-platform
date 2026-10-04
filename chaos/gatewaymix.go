package chaos

// The optional LLM-serving mix: a real gateway.Server (configured like
// cmd/gateway: fail-open rate limiting, exact-match semantic cache,
// bounded-load prefix routing, hedging) in front of real mock inference
// workers over real loopback gRPC, sharing the SAME nemesis-disrupted KV
// (through sched/kvadapter over kv/client) for the rate limiter's buckets,
// the worker registry, and the cache.
//
// Generation has no linearizable model, but it does have a ground truth:
// the mock worker's output is a pure function of (prompt, max_tokens), and
// tenantWorker (below) tags the first token with the tenant, so the right
// answer for (tenant, prompt, max_tokens) is known exactly. check() asserts:
//
//  1. Correctness: every successful response, cached or not, equals that
//     ground truth in full. Catches a truncated stream, a truncated answer
//     cached and replayed, and a cache entry served across tenants.
//  2. Error discipline during chaos: a failed request ends with a
//     retryable status (Unavailable, DeadlineExceeded, ResourceExhausted,
//     Canceled), never Internal/Unknown/anything else; and no Generate
//     call outlives its deadline by more than hangSlack.
//  3. Availability after healing: once the cluster answers again, a final
//     batch of finalBatchSize requests must succeed at >= finalBatchMin.
//  4. No leaked worker streams: every worker's in-flight count returns to
//     zero shortly after the traffic stops (cancellation reached it).
//  5. No panics in any goroutine the mix starts (panicLog).
//
// Traffic shapes: uncached unique prompts (prefix routing and hedging),
// cache-on requests over a small prompt pool shared by every tenant (cache
// hits, and cross-tenant collisions if the cache were not tenant-scoped),
// slow readers (each Send takes a few ms: the stream must not be truncated
// while the gateway waits on its consumer), and stalled readers (Send blocks
// until the request's deadline, the shape of a client that stopped reading
// and whose flow-control window filled: Generate must still return, and the
// worker stream must be cancelled).
//
// Note what (2) cannot see: the gateway maps every worker failure to
// Unavailable ("all inference attempts failed"), so a worker answering
// Internal is not visible as a code; it is (3) that fails if workers stop
// answering correctly.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"dsys/gateway"
	gatewayv1 "dsys/gen/gateway/v1"
	inferv1 "dsys/gen/infer/v1"
	"dsys/infer/mock"
	"dsys/kvapi"
	"dsys/ratelimit"
	"dsys/router"
	"dsys/semcache"
)

const (
	gwMaxTokens    = 8
	finalBatchSize = 40
	finalBatchMin  = 0.95
	hangSlack      = 2 * time.Second
)

// streamMode is how the fake client consumes the gateway's token stream.
type streamMode int

const (
	modeNormal  streamMode = iota
	modeSlow               // every Send takes a few ms
	modeStalled            // Send blocks until the request's context ends
)

func (m streamMode) String() string {
	return [...]string{"normal", "slow", "stalled"}[m]
}

// gwResult is one recorded Generate call.
type gwResult struct {
	tenant, prompt string
	noCache        bool
	mode           streamMode
	final          bool // part of the post-heal batch
	text           string
	cached         bool
	err            error
	elapsed        time.Duration
	deadline       time.Duration
}

type gatewayMix struct {
	gw      *gateway.Server
	servers []*grpc.Server
	workers []*mock.Worker
	faults  injectedFaults
	pl      *panicLog
	prompts []string // the cache-on pool, shared by every tenant

	requests atomic.Int64
	errors   atomic.Int64

	mu      sync.Mutex
	results []gwResult
}

func newGatewayMix(kv kvapi.KV, sc Scenario, pl *panicLog) (*gatewayMix, error) {
	m := &gatewayMix{faults: sc.faults, pl: pl}
	for i := 0; i < 6; i++ {
		m.prompts = append(m.prompts, strings.Repeat("shared system prompt. ", 12)+fmt.Sprintf("question %d?", i))
	}
	reg := router.NewRegistry(kv, router.Options{Prefix: "chaos-workers", CacheTTL: 50 * time.Millisecond})
	lim := ratelimit.New(kv, ratelimit.Options{Prefix: "chaos-rl", Rate: 1000, Burst: 1000})
	var cache gateway.Cache = semcache.New(kv, semcache.NewNGramEmbedder(256), semcache.Options{Prefix: "chaos-cache"})
	if sc.faults.gwCacheTruncate || sc.faults.gwCacheIgnoreTenant {
		cache = &faultyCache{inner: cache, truncate: sc.faults.gwCacheTruncate, ignoreTenant: sc.faults.gwCacheIgnoreTenant}
	}

	for i := 0; i < sc.NumWorkers; i++ {
		w := mock.New(mock.Options{
			ID: fmt.Sprintf("chaos-w%d", i), BasePrefillMs: 2, PrefillPerChar: 0.02, TokenMs: 2,
			Seed: sc.Seed + int64(i) + 4000,
		})
		m.workers = append(m.workers, w)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			m.close()
			return nil, err
		}
		gs := grpc.NewServer()
		inferv1.RegisterInferenceServer(gs, &tenantWorker{
			inner: w, internalRate: sc.faults.gwWorkerInternal, truncate: sc.faults.gwWorkerTruncate,
			rng: rand.New(rand.NewSource(sc.Seed + int64(i) + 4100)),
		})
		go func() { _ = gs.Serve(lis) }()
		m.servers = append(m.servers, gs)
		if _, err := reg.Register(context.Background(), router.Worker{ID: w.ID(), Addr: lis.Addr().String()}, time.Minute); err != nil {
			m.close()
			return nil, err
		}
	}

	m.gw = gateway.New(gateway.Options{
		KV: kv, Registry: reg, Limiter: lim, Cache: cache,
		RateLimitFailOpen: true, // cmd/gateway's default
		PrefixRouting:     true, LoadFactor: 1.25, HedgeAfter: 150 * time.Millisecond,
		Dial: func(addr string) (inferv1.InferenceClient, error) {
			cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			return inferv1.NewInferenceClient(cc), nil
		},
	})
	return m, nil
}

func (m *gatewayMix) close() {
	if m.gw != nil {
		m.gw.Close()
	}
	for _, s := range m.servers {
		s.Stop()
	}
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// tenantWorker wraps a mock worker so its answer depends on the tenant (the
// first token is prefixed "[tenant] "), which is what lets the content
// check see a cross-tenant cache hit. It also carries the injected worker
// faults the checker tests use.
type tenantWorker struct {
	inferv1.UnimplementedInferenceServer
	inner        *mock.Worker
	internalRate float64 // injected: fail this fraction of generations with Internal
	truncate     bool    // injected: end every stream early, marked done

	mu  sync.Mutex
	rng *rand.Rand
}

var errTruncated = errors.New("chaos: injected truncation")

func (t *tenantWorker) Generate(req *inferv1.GenerateRequest, stream inferv1.Inference_GenerateServer) error {
	if t.internalRate > 0 {
		t.mu.Lock()
		fail := t.rng.Float64() < t.internalRate
		t.mu.Unlock()
		if fail {
			return status.Error(codes.Internal, "chaos: injected worker failure")
		}
	}
	err := t.inner.Generate(req, &tagStream{
		Inference_GenerateServer: stream, tenant: req.Tenant, maxTokens: req.MaxTokens, truncate: t.truncate,
	})
	if errors.Is(err, errTruncated) {
		return nil
	}
	return err
}

func (t *tenantWorker) Health(ctx context.Context, req *inferv1.HealthRequest) (*inferv1.HealthResponse, error) {
	return t.inner.Health(ctx, req)
}

type tagStream struct {
	inferv1.Inference_GenerateServer
	tenant    string
	maxTokens int32
	truncate  bool
	stopped   bool
}

func (s *tagStream) Send(tok *inferv1.Token) error {
	if s.stopped {
		return errTruncated
	}
	if tok.Index == 0 {
		tok.Text = "[" + s.tenant + "] " + tok.Text
	}
	if s.truncate && s.maxTokens >= 3 && tok.Index == s.maxTokens-2 {
		tok.Done, tok.FinishReason = true, "stop" // claims to be complete; one token short
		s.stopped = true
	}
	return s.Inference_GenerateServer.Send(tok)
}

// faultyCache wraps the real cache with injected cache bugs.
type faultyCache struct {
	inner        gateway.Cache
	truncate     bool // store only half of every answer
	ignoreTenant bool // strip the gateway's tenant qualifier from the key
}

func (c *faultyCache) key(k string) string {
	if c.ignoreTenant {
		if _, rest, ok := strings.Cut(k, "\n"); ok {
			return rest
		}
	}
	return k
}

func (c *faultyCache) Lookup(ctx context.Context, prompt string) (string, bool, error) {
	return c.inner.Lookup(ctx, c.key(prompt))
}

func (c *faultyCache) Store(ctx context.Context, prompt, text string) error {
	if c.truncate {
		text = text[:len(text)/2]
	}
	return c.inner.Store(ctx, c.key(prompt), text)
}

// collectStream is the fake client end of Generate's server stream. It
// assembles the full answer and, per mode, consumes slowly or not at all.
// Embedding grpc.ServerStream satisfies the rest of the interface; Generate
// only calls Send and Context.
type collectStream struct {
	grpc.ServerStream
	ctx      context.Context
	mode     streamMode
	delay    time.Duration
	plainErr bool // injected: fail Send with a non-status error
	text     strings.Builder
	cached   bool
}

func (s *collectStream) Context() context.Context { return s.ctx }

func (s *collectStream) Send(tok *gatewayv1.Token) error {
	if s.plainErr {
		return errors.New("chaos: injected non-status transport error")
	}
	switch s.mode {
	case modeStalled:
		<-s.ctx.Done() // a client that stopped reading: the send never completes
		return status.FromContextError(s.ctx.Err()).Err()
	case modeSlow:
		select {
		case <-time.After(s.delay):
		case <-s.ctx.Done():
			return status.FromContextError(s.ctx.Err()).Err()
		}
	}
	s.text.WriteString(tok.Text)
	s.cached = s.cached || tok.Cached
	return nil
}

// ---------------------------------------------------------------------------
// Traffic
// ---------------------------------------------------------------------------

// do issues one Generate and records the outcome. Called on harness
// goroutines that are already under panicLog.
func (m *gatewayMix) do(ctx context.Context, res gwResult, delay time.Duration) {
	octx, cancel := context.WithTimeout(ctx, res.deadline)
	defer cancel()
	st := &collectStream{ctx: octx, mode: res.mode, delay: delay,
		plainErr: m.faults.gwPlainSendError && !res.final && res.mode == modeNormal}
	req := &gatewayv1.GenerateRequest{Tenant: res.tenant, Prompt: res.prompt, NoCache: res.noCache, MaxTokens: gwMaxTokens}
	m.requests.Add(1)
	start := time.Now()
	res.err = m.gw.Generate(req, st)
	res.elapsed = time.Since(start)
	if res.err != nil {
		m.errors.Add(1)
	} else {
		res.text, res.cached = st.text.String(), st.cached
	}
	m.mu.Lock()
	m.results = append(m.results, res)
	m.mu.Unlock()
}

func (m *gatewayMix) run(ctx context.Context, sc Scenario, wg *sync.WaitGroup) {
	unique := []string{
		strings.Repeat("alpha system prompt. ", 15),
		strings.Repeat("beta system prompt. ", 15),
	}
	for c := 0; c < max(2, sc.NumClients/3); c++ {
		m.pl.goWG(wg, fmt.Sprintf("gateway-client-%d", c), func() {
			r := rand.New(rand.NewSource(sc.Seed + int64(c) + 5000))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				res := gwResult{tenant: fmt.Sprintf("t%d", r.Intn(3)), deadline: 3 * time.Second}
				var delay time.Duration
				switch x := r.Float64(); {
				case x < 0.45: // uncached, unique prompt
					res.noCache = true
					res.prompt = unique[r.Intn(len(unique))] + fmt.Sprintf("q%d", r.Int())
				case x < 0.85: // cache on, shared pool
					res.prompt = m.prompts[r.Intn(len(m.prompts))]
				case x < 0.93: // slow reader, cache on or off
					res.mode, res.noCache = modeSlow, r.Intn(2) == 0
					res.prompt = m.prompts[r.Intn(len(m.prompts))]
					delay = time.Duration(3+r.Intn(8)) * time.Millisecond
				default: // stalled reader with a short deadline
					res.mode, res.noCache = modeStalled, r.Intn(2) == 0
					res.prompt = m.prompts[r.Intn(len(m.prompts))]
					res.deadline = 400 * time.Millisecond
				}
				m.do(ctx, res, delay)
				time.Sleep(time.Duration(5+r.Intn(15)) * time.Millisecond)
			}
		})
	}
}

// finalBatch sends finalBatchSize well-behaved requests after the cluster
// has healed (cache on and off, every tenant), 4 at a time.
func (m *gatewayMix) finalBatch(ctx context.Context) {
	var wg sync.WaitGroup
	next := atomic.Int64{}
	for g := 0; g < 4; g++ {
		m.pl.goWG(&wg, "gateway-final", func() {
			for {
				i := next.Add(1) - 1
				if i >= finalBatchSize {
					return
				}
				m.do(ctx, gwResult{
					tenant: fmt.Sprintf("t%d", i%3), prompt: m.prompts[int(i)%len(m.prompts)],
					noCache: i%2 == 0, final: true, deadline: 3 * time.Second,
				}, 0)
			}
		})
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

// reference computes the ground-truth answer for (tenant, prompt) by running
// a fresh, fault-free tenantWorker in-process with no delays.
type reference struct {
	w    *tenantWorker
	memo map[string]string
}

func newReference() *reference {
	noSleep := func(context.Context, time.Duration) error { return nil }
	return &reference{
		w:    &tenantWorker{inner: mock.New(mock.Options{ID: "reference", Sleep: noSleep})},
		memo: map[string]string{},
	}
}

type refStream struct {
	grpc.ServerStream
	text strings.Builder
}

func (s *refStream) Context() context.Context { return context.Background() }
func (s *refStream) Send(tok *inferv1.Token) error {
	s.text.WriteString(tok.Text)
	return nil
}

func (r *reference) answer(tenant, prompt string) (string, error) {
	k := tenant + "\x00" + prompt
	if v, ok := r.memo[k]; ok {
		return v, nil
	}
	st := &refStream{}
	if err := r.w.Generate(&inferv1.GenerateRequest{Tenant: tenant, Prompt: prompt, MaxTokens: gwMaxTokens}, st); err != nil {
		return "", err
	}
	r.memo[k] = st.text.String()
	return r.memo[k], nil
}

func retryableGatewayCode(c codes.Code) bool {
	switch c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Canceled:
		return true
	}
	return false
}

// gwStats summarises a run for the Report.
type gwStats struct {
	cacheHits, slowOK, stalled, finalOK, finalTotal int
}

// check runs the final batch and every assertion in the file comment.
// Call it after the run's traffic has stopped and the cluster has healed.
func (m *gatewayMix) check(ctx context.Context) (violations []string, st gwStats) {
	m.finalBatch(ctx)

	ref := newReference()
	m.mu.Lock()
	results := append([]gwResult(nil), m.results...)
	m.mu.Unlock()

	var wrong, badCode, hung []string
	byCode := map[codes.Code]int{}
	for _, r := range results {
		if r.mode == modeStalled {
			st.stalled++
		}
		if r.elapsed > r.deadline+hangSlack && len(hung) < 3 {
			hung = append(hung, fmt.Sprintf("%s-stream request took %s against a %s deadline", r.mode, r.elapsed.Round(time.Millisecond), r.deadline))
		}
		if r.final {
			st.finalTotal++
		}
		if r.err != nil {
			c := status.Code(r.err)
			byCode[c]++
			if !retryableGatewayCode(c) && len(badCode) < 3 {
				badCode = append(badCode, fmt.Sprintf("%s (%s stream): %v", c, r.mode, r.err))
			}
			continue
		}
		if r.final {
			st.finalOK++
		}
		if r.cached {
			st.cacheHits++
		}
		if r.mode == modeSlow {
			st.slowOK++
		}
		want, err := ref.answer(r.tenant, r.prompt)
		if err != nil {
			violations = append(violations, fmt.Sprintf("gateway: computing the reference answer: %v", err))
			continue
		}
		if r.text != want && len(wrong) < 3 {
			wrong = append(wrong, fmt.Sprintf("tenant=%s cached=%v %s stream: got %q, want %q", r.tenant, r.cached, r.mode, r.text, want))
		}
	}
	if len(wrong) > 0 {
		violations = append(violations, "gateway: wrong answer (differs from the full, uncached answer for that tenant+prompt): "+strings.Join(wrong, "; "))
	}
	if len(badCode) > 0 {
		violations = append(violations, "gateway: non-retryable error code: "+strings.Join(badCode, "; "))
	}
	if len(hung) > 0 {
		violations = append(violations, "gateway: Generate outlived its deadline (hang): "+strings.Join(hung, "; "))
	}
	if st.finalTotal > 0 && float64(st.finalOK) < finalBatchMin*float64(st.finalTotal) {
		violations = append(violations, fmt.Sprintf("gateway: only %d/%d requests succeeded after the cluster healed (want >= %.0f%%); errors by code: %v",
			st.finalOK, st.finalTotal, finalBatchMin*100, byCode))
	}

	// Leaked worker streams: every generation must have ended (finished or
	// been cancelled) once nothing is in flight at the gateway.
	deadline := time.Now().Add(3 * time.Second)
	for {
		var busy []string
		for _, w := range m.workers {
			if n := w.Inflight(); n != 0 {
				busy = append(busy, fmt.Sprintf("%s=%d", w.ID(), n))
			}
		}
		if len(busy) == 0 {
			break
		}
		if time.Now().After(deadline) {
			violations = append(violations, "gateway: worker generations still running 3s after all traffic ended (cancellation leak): "+strings.Join(busy, ", "))
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return violations, st
}
