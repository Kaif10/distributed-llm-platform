// Package gateway is the client-facing inference front end: the piece of
// this project that turns a replicated KV, a scheduler's worth of lease and
// fencing machinery, and a fleet of model-hosting workers into something
// that answers "generate text for this prompt" well under load.
//
// # The request path
//
//	client ─Generate(stream)─▶ gateway replica (any of N, all stateless)
//	                              │ 1. rate limit: per-tenant token bucket in the KV (CAS)
//	                              │ 2. semantic cache: seen this prompt (opt-in: a near-dup)? stream the cached answer
//	                              │ 3. route: rendezvous-hash the prompt PREFIX onto a live worker
//	                              │ 4. stream tokens back; hedge to a 2nd worker if the 1st is slow
//	                              ▼    to start; cancel the loser; cancel the worker if the client goes
//	                        inference worker (Python model host, or the Go mock)
//
// # Why each step is where it is
//
// Rate limiting comes first because it is the cheapest rejection and the
// one that must not be dodgeable: a tenant over quota should not even get a
// cache lookup. It lives in the KV so that fifty gateway replicas enforce
// one budget, not fifty; the bucket is updated with CAS, so two replicas
// admitting "the last token" at the same instant cannot both succeed.
//
// The cache comes second because a hit costs one KV read and no GPU. Its
// shared state ((tenant, prompt) hash -> answer) is in the KV so a hit on one replica
// is a hit on all; only the vector index for near-duplicates is local.
//
// Routing by prompt prefix exists because modern inference servers (vLLM,
// SGLang) keep the attention KV cache of recent prompts and can skip
// prefill for a shared prefix. That only helps if requests sharing a prefix
// land on the SAME worker. Rendezvous hashing gives that property with
// minimal reshuffling when workers join or leave, and a load-aware fallback
// keeps one hot prefix from pinning one worker into the ground.
//
// Hedging comes last, and only works because generation has no side
// effects: launching a second attempt for a slow first one is safe exactly
// when the operation is idempotent. That is the same reason a retried Put
// needed RequestMeta in Phase 1 and a retried Complete needed a fence in
// Phase 4; here the "fence" is that the loser is cancelled and its tokens
// are simply never forwarded.
//
// # Cancellation is not optional
//
// A cancelled client stream must cancel the worker's generation. Tokens
// produced after a cancel are pure waste, and with hedging in play the
// loser of every race would otherwise keep generating. Context propagation
// end to end (client -> gateway -> worker) is the mechanism; the mock
// worker and the Python worker both check for it on every token.
package gateway

import (
	"context"
	"time"

	inferv1 "dsys/gen/infer/v1"
	"dsys/kvapi"
	"dsys/obs"
	"dsys/ratelimit"
	"dsys/router"
)

// Cache is what the gateway needs from a semantic cache. Implemented by
// package semcache; a nil Cache disables caching.
//
// Lookup returns the cached completion for a prompt that is identical or
// semantically close enough (per the implementation's threshold) to one
// seen before. Store records a completed generation. Both must be safe for
// concurrent use from many request goroutines.
type Cache interface {
	Lookup(ctx context.Context, prompt string) (text string, hit bool, err error)
	Store(ctx context.Context, prompt, text string) error
}

// Dialer opens a client to an inference worker at addr. The default uses
// gRPC with insecure credentials; tests inject in-process clients.
type Dialer func(addr string) (inferv1.InferenceClient, error)

// Options configures a gateway Server.
type Options struct {
	KV       kvapi.KV
	Limiter  *ratelimit.Limiter // nil disables rate limiting
	Cache    Cache              // nil disables caching
	Registry *router.Registry   // required

	// RateLimitFailOpen admits a request when the rate limiter cannot reach
	// the KV (an election, a partition) instead of failing it. The limiter
	// protects workers from one tenant's excess; failing closed turns a
	// control-plane blip into a full inference outage while the workers are
	// healthy, which is the worse trade for a serving path. Admissions made
	// this way are counted in Stats.RateLimitFailOpen. ratelimit.ErrContended
	// is NOT a KV failure (the KV is up, many gateways are hammering one
	// tenant's bucket), so it still fails the request even with this set:
	// failing open there would wave through exactly the tenant causing it.
	// false (the zero value) keeps the old fail-closed behaviour;
	// cmd/gateway defaults its -rl-fail-open flag to true.
	RateLimitFailOpen bool

	// KVCallTimeout bounds each KV call on the request path: the limiter's
	// Take, the cache's Lookup and Store, and the registry read. The real
	// KV client keeps retrying for its whole -kv-timeout (10s by default)
	// when there is no Raft leader, so without this bound "fail open" meant
	// a request stalled for that long once in the limiter and again in the
	// cache before it was admitted. A healthy KV answers in milliseconds, so
	// the bound only ever bites during an outage. 0 means 300ms; negative
	// disables the bound (each call then waits as long as the KV client
	// and the request's own context allow).
	KVCallTimeout time.Duration
	// KVBreakerThreshold is how many consecutive KV failures (errors or
	// KVCallTimeout expiries) open the KV circuit breaker, after which the
	// limiter and the cache are skipped (fail open / miss) for
	// KVBreakerCooldown, then probed with one request. See kvBreaker.
	// 0 means 5; negative disables the breaker.
	KVBreakerThreshold int
	// KVBreakerCooldown is how long the breaker stays open before probing.
	// 0 means 2s.
	KVBreakerCooldown time.Duration

	// PrefixRouting picks the worker by rendezvous-hashing the prompt prefix
	// (see router). false picks the least-loaded live worker instead; the
	// benchmark toggles this to show the effect on prefix-cache hit rate.
	PrefixRouting bool
	// PrefixChars is how much of the prompt counts as "the prefix" for
	// routing. 0 means 256.
	PrefixChars int
	// MaxInflightPerWorker makes routing skip a worker at or above this
	// load. 0 means 8. Ignored when LoadFactor is set.
	MaxInflightPerWorker int
	// LoadFactor turns on bounded-load prefix routing (router.PickBounded):
	// skip any worker above LoadFactor times the fleet's average load and
	// fall through to the prefix's next-ranked worker. 0 keeps plain
	// affinity (router.Pick with MaxInflightPerWorker). Must be > 1 when
	// set; 1.25 is the usual choice.
	LoadFactor float64

	// HedgeAfter launches a second attempt on a different worker if the
	// first has not produced its first token within this long. 0 disables.
	HedgeAfter time.Duration

	// DefaultMaxTokens applies when a request says 0. 0 means 64.
	DefaultMaxTokens int32

	// Metrics receives Prometheus counters/histograms for the request path.
	// nil (the zero value from a struct literal that doesn't set it)
	// disables metrics entirely: every call site nil-checks before use,
	// exactly like Cache/Limiter above, so existing callers and tests are
	// unaffected.
	Metrics *obs.Metrics

	Dial  Dialer
	Clock func() time.Time
}

// Stats are cumulative counters.
type Stats struct {
	Requests       uint64
	RateLimited    uint64
	CacheHits      uint64
	HedgesLaunched uint64
	HedgesWon      uint64
	Cancelled      uint64
	WorkerErrors   uint64
	// RateLimitFailOpen counts requests admitted without a rate-limit
	// decision because the limiter's KV was unavailable (see
	// Options.RateLimitFailOpen).
	RateLimitFailOpen uint64
	// KVBreakerTrips counts how often the KV circuit breaker opened;
	// KVSkipped counts limiter/cache steps skipped while it was open.
	KVBreakerTrips uint64
	KVSkipped      uint64
	RoutedTo       map[string]uint64
	LiveWorkers    int
}
