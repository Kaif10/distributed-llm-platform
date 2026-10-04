package gateway

// The gateway's request path. See api.go for the design rationale; this
// file is the implementation.
//
// # Concurrency shape of one request
//
// Generate is a server-streaming RPC, so the handler owns one goroutine per
// request and must not block forever anywhere. The hedging path adds up to
// two more goroutines (one per attempt), joined by a single channel of
// results. The rule that keeps this honest: every goroutine this handler
// starts is bound by a context derived from the request's context, so when
// the client goes away, everything the request started goes away with it.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gatewayv1 "dsys/gen/gateway/v1"
	inferv1 "dsys/gen/infer/v1"
	"dsys/obs"
	"dsys/router"
)

// Server implements gatewayv1.GatewayServer.
type Server struct {
	gatewayv1.UnimplementedGatewayServer
	opts Options

	mu    sync.Mutex
	conns map[string]inferv1.InferenceClient // addr -> client, lazily dialled
	cc    map[string]*grpc.ClientConn

	requests       atomic.Uint64
	rateLimited    atomic.Uint64
	cacheHits      atomic.Uint64
	hedgesLaunched atomic.Uint64
	hedgesWon      atomic.Uint64
	cancelled      atomic.Uint64
	workerErrors   atomic.Uint64

	routedMu sync.Mutex
	routedTo map[string]uint64

	// localInflight counts the streams THIS gateway currently has open to
	// each worker. The registry's Inflight field is only as fresh as the
	// worker's last lease renewal (about once a second), which is far too
	// stale to steer routing: a burst of concurrent requests would all read
	// the same zeros and pile onto the same worker. What a gateway knows
	// exactly, and instantly, is its own outstanding requests, so routing
	// decisions use registry-reported load PLUS this. It is the same reason
	// real load balancers count outstanding requests locally rather than
	// trusting a periodic health report.
	inflightMu    sync.Mutex
	localInflight map[string]int32
}

// New returns a gateway Server.
func New(opts Options) *Server {
	if opts.PrefixChars <= 0 {
		opts.PrefixChars = 256
	}
	if opts.MaxInflightPerWorker <= 0 {
		opts.MaxInflightPerWorker = 8
	}
	if opts.DefaultMaxTokens <= 0 {
		opts.DefaultMaxTokens = 64
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	s := &Server{
		opts:          opts,
		conns:         map[string]inferv1.InferenceClient{},
		cc:            map[string]*grpc.ClientConn{},
		routedTo:      map[string]uint64{},
		localInflight: map[string]int32{},
	}
	if s.opts.Dial == nil {
		s.opts.Dial = s.dialGRPC
	}
	return s
}

func (s *Server) dialGRPC(addr string) (inferv1.InferenceClient, error) {
	// obs.GRPCDialOption propagates the caller's trace context (W3C
	// traceparent) in outbound request metadata, so a worker's span nests
	// under the gateway.attempt span that triggered it.
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), obs.GRPCDialOption())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cc[addr] = cc
	s.mu.Unlock()
	return inferv1.NewInferenceClient(cc), nil
}

// client returns a cached client for addr.
func (s *Server) client(addr string) (inferv1.InferenceClient, error) {
	s.mu.Lock()
	c, ok := s.conns[addr]
	s.mu.Unlock()
	if ok {
		return c, nil
	}
	c, err := s.opts.Dial(addr)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if existing, ok := s.conns[addr]; ok {
		c = existing
	} else {
		s.conns[addr] = c
	}
	s.mu.Unlock()
	return c, nil
}

// Close releases dialled connections.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cc := range s.cc {
		_ = cc.Close()
	}
	s.cc = map[string]*grpc.ClientConn{}
	s.conns = map[string]inferv1.InferenceClient{}
}

// Snapshot returns cumulative counters. (The gRPC method is Stats; this is
// the in-process accessor binaries and tests use.)
func (s *Server) Snapshot() Stats {
	st := Stats{
		Requests:       s.requests.Load(),
		RateLimited:    s.rateLimited.Load(),
		CacheHits:      s.cacheHits.Load(),
		HedgesLaunched: s.hedgesLaunched.Load(),
		HedgesWon:      s.hedgesWon.Load(),
		Cancelled:      s.cancelled.Load(),
		WorkerErrors:   s.workerErrors.Load(),
		RoutedTo:       map[string]uint64{},
	}
	s.routedMu.Lock()
	for k, v := range s.routedTo {
		st.RoutedTo[k] = v
	}
	s.routedMu.Unlock()
	if s.opts.Registry != nil {
		if live, err := s.opts.Registry.Live(context.Background()); err == nil {
			st.LiveWorkers = len(live)
		}
	}
	return st
}

func (s *Server) countRoute(id string) {
	s.routedMu.Lock()
	s.routedTo[id]++
	s.routedMu.Unlock()
	s.opts.Metrics.IncRouted(id)
}

func (s *Server) addInflight(id string, delta int32) {
	s.inflightMu.Lock()
	s.localInflight[id] += delta
	if s.localInflight[id] <= 0 {
		delete(s.localInflight, id)
	}
	s.inflightMu.Unlock()
}

// withLocalLoad returns live workers with this gateway's own outstanding
// request count folded into Inflight.
func (s *Server) withLocalLoad(live []router.Worker) []router.Worker {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if len(s.localInflight) == 0 {
		return live
	}
	out := make([]router.Worker, len(live))
	copy(out, live)
	for i := range out {
		out[i].Inflight += s.localInflight[out[i].ID]
	}
	return out
}

// RegisterWorker records a worker's lease in the shared registry.
func (s *Server) RegisterWorker(ctx context.Context, req *gatewayv1.RegisterWorkerRequest) (*gatewayv1.RegisterWorkerResponse, error) {
	if req.WorkerId == "" || req.Addr == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id and addr are required")
	}
	ttl := time.Duration(req.LeaseMs) * time.Millisecond
	if ttl <= 0 {
		ttl = 3 * time.Second
	}
	until, err := s.opts.Registry.Register(ctx, router.Worker{
		ID:       req.WorkerId,
		Addr:     req.Addr,
		Model:    req.Model,
		Inflight: req.Inflight,
	}, ttl)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "registry: %v", err)
	}
	return &gatewayv1.RegisterWorkerResponse{LeaseUntilMs: until.UnixMilli()}, nil
}

// Stats is the gRPC method.
func (s *Server) Stats(context.Context, *gatewayv1.StatsRequest) (*gatewayv1.StatsResponse, error) {
	st := s.Snapshot()
	return &gatewayv1.StatsResponse{
		Requests: st.Requests, RateLimited: st.RateLimited, CacheHits: st.CacheHits,
		HedgesLaunched: st.HedgesLaunched, HedgesWon: st.HedgesWon, Cancelled: st.Cancelled,
		WorkerErrors: st.WorkerErrors, RoutedTo: st.RoutedTo, LiveWorkers: int32(st.LiveWorkers),
	}, nil
}

// ---------------------------------------------------------------------------
// Generate
// ---------------------------------------------------------------------------

// Generate is the whole request path.
func (s *Server) Generate(req *gatewayv1.GenerateRequest, stream grpc.ServerStreamingServer[gatewayv1.Token]) error {
	ctx := stream.Context()
	s.requests.Add(1)
	started := s.opts.Clock()

	if req.Prompt == "" {
		return status.Error(codes.InvalidArgument, "prompt is required")
	}
	tenant := req.Tenant
	if tenant == "" {
		tenant = "default"
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = s.opts.DefaultMaxTokens
	}

	// One parent span for the whole request; the four stages below are
	// child spans, and streamFromWorkers adds one gateway.attempt span per
	// worker attempt (primary plus any hedge). See dsys/obs's doc comment
	// for the full span-name list.
	ctx, span := obs.Tracer("gateway").Start(ctx, "gateway.Generate", trace.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.Int("prompt_chars", len(req.Prompt)),
	))
	defer span.End()

	// 1. Rate limit. First, because it must be the cheapest rejection and
	// must not be dodgeable by anything downstream.
	if s.opts.Limiter != nil {
		rlCtx, rlSpan := obs.Tracer("gateway").Start(ctx, "gateway.ratelimit")
		ok, retryAfter, err := s.opts.Limiter.Take(rlCtx, tenant, 1)
		if err != nil {
			rlSpan.End()
			return status.Errorf(codes.Unavailable, "rate limiter: %v", err)
		}
		if !ok {
			s.rateLimited.Add(1)
			s.opts.Metrics.IncRateLimited(tenant)
			rlSpan.SetAttributes(attribute.Bool("limited", true))
			rlSpan.End()
			return status.Errorf(codes.ResourceExhausted,
				"rate limit exceeded for tenant %q retry_after_ms=%d", tenant, retryAfter.Milliseconds())
		}
		rlSpan.SetAttributes(attribute.Bool("limited", false))
		rlSpan.End()
	}

	// 2. Semantic cache. A hit costs one KV read and no GPU. Keyed by
	// tenant as well as prompt; see cacheKey.
	if s.opts.Cache != nil && !req.NoCache {
		ccCtx, ccSpan := obs.Tracer("gateway").Start(ctx, "gateway.cache_lookup")
		text, hit, err := s.opts.Cache.Lookup(ccCtx, cacheKey(tenant, req.Prompt))
		ccSpan.SetAttributes(attribute.Bool("cache_hit", hit))
		ccSpan.End()
		if err == nil && hit {
			s.cacheHits.Add(1)
			span.SetAttributes(attribute.Bool("cached", true))
			s.opts.Metrics.ObserveRequest(tenant, true, false)
			return s.streamCached(stream, text, started)
		} else if err != nil {
			// A cache failure must never fail the request: fall through to
			// the model. Caches are an optimisation, not a dependency.
			s.workerErrors.Add(0)
		}
	}

	// 3. Route.
	rtCtx, rtSpan := obs.Tracer("gateway").Start(ctx, "gateway.route", trace.WithAttributes(
		attribute.Bool("prefix_routing", s.opts.PrefixRouting),
	))
	live, err := s.opts.Registry.Live(rtCtx)
	if err != nil {
		rtSpan.End()
		return status.Errorf(codes.Unavailable, "registry: %v", err)
	}
	s.opts.Metrics.SetLiveWorkers(len(live))
	if len(live) == 0 {
		rtSpan.End()
		return status.Error(codes.Unavailable, "no inference workers registered")
	}
	prefix := router.Prefix(req.Prompt, s.opts.PrefixChars)
	live = s.withLocalLoad(live)
	var primary router.Worker
	var ok bool
	if s.opts.PrefixRouting {
		if s.opts.LoadFactor > 1 {
			primary, ok = router.PickBounded(live, prefix, s.opts.LoadFactor, "")
		} else {
			primary, ok = router.Pick(live, prefix, s.opts.MaxInflightPerWorker, "")
		}
	} else {
		primary, ok = router.PickLeastLoaded(live, "")
	}
	if !ok {
		rtSpan.End()
		return status.Error(codes.Unavailable, "no eligible worker")
	}
	rtSpan.SetAttributes(attribute.String("worker", primary.ID))
	rtSpan.End()

	// 4. Stream, with hedging.
	return s.streamFromWorkers(ctx, stream, req, tenant, maxTokens, prefix, live, primary, started)
}

// cacheKey is what the gateway hands the Cache as "the prompt": the prompt
// qualified by tenant, so one tenant can never be served a completion that
// was generated for another (a cross-tenant data leak, not just a stale
// answer). Doing it here keeps the Cache interface tenant-agnostic.
//
// The tenant goes in as a hex SHA-256, not verbatim, because the cache does
// not treat its key as opaque bytes: semcache lowercases and collapses
// whitespace (so tenants "Acme" and "acme" would merge), and its opt-in near
// path matches keys within a few character edits (so "tenant-a" and
// "tenant-b" would be one edit apart). Two different tenants' digests differ
// in ~60 of 64 lowercase hex characters, which survives normalisation and is
// far outside any near-duplicate budget, while within one tenant the prefix
// is identical and near matching behaves exactly as before.
func cacheKey(tenant, prompt string) string {
	sum := sha256.Sum256([]byte(tenant))
	return "tenant:" + hex.EncodeToString(sum[:]) + "\n" + prompt
}

// streamCached replays a cached completion as a token stream, so a cache
// hit is indistinguishable from a fresh generation to the client except for
// the `cached` flag and the speed.
func (s *Server) streamCached(stream grpc.ServerStreamingServer[gatewayv1.Token], text string, started time.Time) error {
	ttft := s.opts.Clock().Sub(started).Milliseconds()
	s.opts.Metrics.ObserveTTFT(float64(ttft))
	if err := stream.Send(&gatewayv1.Token{
		Text: text, Index: 0, Cached: true, TtftMs: ttft,
	}); err != nil {
		return err
	}
	return stream.Send(&gatewayv1.Token{Index: 1, Done: true, FinishReason: "stop", Cached: true})
}

// attempt is one worker's stream, consumed by a goroutine.
type attempt struct {
	worker  router.Worker
	isHedge bool
	// first carries the first token (or an error) so the racer can pick a
	// winner as soon as either attempt produces anything.
	first chan attemptFirst
	// tokens carries the remaining tokens after the first, in order, and is
	// closed when runAttempt returns. Closing it is the ONLY end-of-stream
	// signal: the consumer learns the stream is over by draining it, so it
	// can never stop while tokens are still buffered. (An earlier version
	// signalled completion on errs and selected on both channels; with both
	// ready, select picks at random, so a fast worker and a slow client
	// truncated the response and cached the truncated text.)
	tokens chan *inferv1.Token
	// errs holds why the stream ended without a done token, written (at
	// most once, buffered) BEFORE tokens is closed.
	errs   chan error
	cancel context.CancelFunc
	span   trace.Span // gateway.attempt; ended when runAttempt returns
}

type attemptFirst struct {
	tok *inferv1.Token
	err error
	a   *attempt
}

// streamFromWorkers runs the primary attempt, optionally hedges, forwards
// the winner's tokens, and cancels everything else.
func (s *Server) streamFromWorkers(
	ctx context.Context,
	stream grpc.ServerStreamingServer[gatewayv1.Token],
	req *gatewayv1.GenerateRequest,
	tenant string,
	maxTokens int32,
	prefix string,
	live []router.Worker,
	primary router.Worker,
	started time.Time,
) error {
	// Every attempt hangs off this context, so returning from this function
	// for ANY reason (client cancel, error, completion) tears down every
	// worker stream we started. This is the cancellation guarantee.
	runCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	first := make(chan attemptFirst, 2)
	reqID := fmt.Sprintf("%d-%s", started.UnixNano(), tenant)

	start := func(w router.Worker, isHedge bool) *attempt {
		a := &attempt{
			worker:  w,
			isHedge: isHedge,
			first:   first,
			tokens:  make(chan *inferv1.Token, 64),
			errs:    make(chan error, 1),
		}
		actx, cancel := context.WithCancel(runCtx)
		a.cancel = cancel
		actx, a.span = obs.Tracer("gateway").Start(actx, "gateway.attempt", trace.WithAttributes(
			attribute.String("worker", w.ID),
			attribute.Bool("hedge", isHedge),
		))
		s.countRoute(w.ID)
		s.addInflight(w.ID, 1)
		go func() {
			defer s.addInflight(w.ID, -1)
			defer a.span.End()
			s.runAttempt(actx, a, reqID, req.Prompt, tenant, maxTokens)
		}()
		return a
	}

	primaryAttempt := start(primary, false)
	attempts := []*attempt{primaryAttempt}

	var hedgeTimer <-chan time.Time
	if s.opts.HedgeAfter > 0 && len(live) > 1 {
		t := time.NewTimer(s.opts.HedgeAfter)
		defer t.Stop()
		hedgeTimer = t.C
	}

	// Wait for a winner: the first attempt to produce a first token.
	var winner *attempt
	var firstTok *inferv1.Token
	var lastErr error
	pending := 1

	for winner == nil {
		select {
		case <-ctx.Done():
			s.cancelled.Add(1)
			return status.FromContextError(ctx.Err()).Err()

		case <-hedgeTimer:
			hedgeTimer = nil
			// The primary is slow to start. Launch a second attempt on a
			// different worker. This is only safe because generation has no
			// side effects; the loser is cancelled and its tokens dropped.
			var hedgeWorker router.Worker
			var ok bool
			if s.opts.PrefixRouting {
				hedgeWorker, ok = router.Pick(live, prefix, 0, primary.ID)
			} else {
				hedgeWorker, ok = router.PickLeastLoaded(live, primary.ID)
			}
			if ok {
				s.hedgesLaunched.Add(1)
				s.opts.Metrics.IncHedgeLaunched()
				attempts = append(attempts, start(hedgeWorker, true))
				pending++
			}

		case f := <-first:
			if f.err != nil {
				pending--
				lastErr = f.err
				s.workerErrors.Add(1)
				if pending == 0 {
					// Every attempt failed. If we never hedged, try once on
					// another worker before giving up.
					if len(attempts) == 1 && len(live) > 1 {
						var alt router.Worker
						var ok bool
						if s.opts.PrefixRouting {
							alt, ok = router.Pick(live, prefix, 0, primary.ID)
						} else {
							alt, ok = router.PickLeastLoaded(live, primary.ID)
						}
						if ok {
							attempts = append(attempts, start(alt, false))
							pending++
							continue
						}
					}
					return status.Errorf(codes.Unavailable, "all inference attempts failed: %v", lastErr)
				}
				continue
			}
			winner = f.a
			firstTok = f.tok
		}
	}

	// Cancel the losers immediately: their tokens are waste.
	for _, a := range attempts {
		if a != winner {
			a.span.SetAttributes(attribute.Bool("won", false))
			a.cancel()
		}
	}
	winner.span.SetAttributes(attribute.Bool("won", true))
	if winner.isHedge {
		s.hedgesWon.Add(1)
		s.opts.Metrics.IncHedgeWon()
	}

	ttft := s.opts.Clock().Sub(started).Milliseconds()
	hedged := len(attempts) > 1

	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("worker", winner.worker.ID),
		attribute.Bool("cached", false),
		attribute.Bool("hedged", hedged),
		attribute.Bool("hedge_won", winner.isHedge),
		attribute.Bool("prefix_cache_hit", firstTok.PrefixCacheHit),
	)
	s.opts.Metrics.ObserveRequest(tenant, false, hedged)
	s.opts.Metrics.ObserveTTFT(float64(ttft))

	// Forward the first token with the metadata the benchmark reads.
	out := &gatewayv1.Token{
		Text: firstTok.Text, Index: firstTok.Index, Done: firstTok.Done,
		FinishReason: firstTok.FinishReason,
		Worker:       winner.worker.ID, Hedged: hedged, HedgeWon: winner.isHedge,
		TtftMs: ttft, PrefixCacheHit: firstTok.PrefixCacheHit, PrefillMs: firstTok.PrefillMs,
	}
	if err := stream.Send(out); err != nil {
		return err
	}

	// Forward the rest, accumulating for the cache. Only a stream that
	// reached done=true is a complete answer, and only a complete answer
	// may be cached: anything else (worker error, worker EOF without done,
	// client gone) returns without Store, so a partial completion can never
	// be served to the next caller as if it were the whole thing.
	var full []byte
	full = append(full, firstTok.Text...)
	store := func() {
		if s.opts.Cache != nil && !req.NoCache && len(full) > 0 {
			_ = s.opts.Cache.Store(ctx, cacheKey(tenant, req.Prompt), string(full))
		}
	}
	if firstTok.Done {
		store()
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			s.cancelled.Add(1)
			return status.FromContextError(ctx.Err()).Err()
		case tok, ok := <-winner.tokens:
			if !ok {
				// Closed before a done token. runAttempt writes the reason
				// to errs before closing tokens, so this does not block.
				err := errStreamEndedEarly
				select {
				case e := <-winner.errs:
					if e != nil {
						err = e
					}
				default:
				}
				if ctx.Err() != nil {
					s.cancelled.Add(1)
					return status.FromContextError(ctx.Err()).Err()
				}
				s.workerErrors.Add(1)
				return status.Errorf(codes.Unavailable, "worker %s: %v", winner.worker.ID, err)
			}
			full = append(full, tok.Text...)
			if err := stream.Send(&gatewayv1.Token{
				Text: tok.Text, Index: tok.Index, Done: tok.Done, FinishReason: tok.FinishReason,
				Worker: winner.worker.ID,
			}); err != nil {
				return err
			}
			if tok.Done {
				store()
				return nil
			}
		}
	}
}

// errStreamEndedEarly: the worker closed its stream (EOF) without ever
// sending a done token, so what we have is not a complete answer.
var errStreamEndedEarly = errors.New("worker stream ended before done")

// runAttempt opens one worker stream and pumps it into the attempt's
// channels. It exits promptly when its context is cancelled, which is what
// makes the loser of a hedge stop costing GPU time.
func (s *Server) runAttempt(ctx context.Context, a *attempt, reqID, prompt, tenant string, maxTokens int32) {
	defer close(a.tokens)

	cli, err := s.client(a.worker.Addr)
	if err != nil {
		a.first <- attemptFirst{err: err, a: a}
		return
	}
	st, err := cli.Generate(ctx, &inferv1.GenerateRequest{
		RequestId: reqID, Tenant: tenant, Prompt: prompt, MaxTokens: maxTokens,
	})
	if err != nil {
		a.first <- attemptFirst{err: err, a: a}
		return
	}

	tok, err := st.Recv()
	if err != nil {
		a.first <- attemptFirst{err: err, a: a}
		return
	}
	a.first <- attemptFirst{tok: tok, a: a}

	if tok.Done {
		return // the deferred close of tokens is the end-of-stream signal
	}
	// Every exit below either follows a done token or writes the reason to
	// errs first; the deferred close(a.tokens) then tells the consumer, after
	// every token sent so far. Nothing here ever drops a token: the send
	// blocks until the consumer takes it or the attempt is cancelled.
	for {
		tok, err := st.Recv()
		if err != nil {
			if isEOF(err) {
				err = errStreamEndedEarly
			}
			a.errs <- err
			return
		}
		select {
		case a.tokens <- tok:
		case <-ctx.Done():
			a.errs <- ctx.Err()
			return
		}
		if tok.Done {
			return
		}
	}
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF)
}
