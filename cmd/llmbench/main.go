// llmbench is the Phase 5 exit-criterion tool: a load generator for the
// gateway that measures what the serving path is supposed to improve.
//
// Each request is one of -prefixes long "system prompts" (~600 chars,
// generated deterministically from -seed) followed by a unique short user
// question, so requests share prefixes the way real chat traffic does. Per
// request we record time to first token (TTFT), total latency, and the
// first token's metadata: which worker served it, whether the worker's
// prefix cache covered the system prompt, how long prefill took, and
// whether a hedge was launched and won.
//
// The report then answers the Phase 5 questions directly:
//
//   - prefix-cache hit rate and prefill_ms: does prefix-aware routing land
//     a shared prefix on the same worker? Compare gateway -prefix-routing
//     against -prefix-routing=false.
//   - per-prefix -> worker spread: how many distinct workers each prefix hit.
//     1.0 is perfect affinity; with least-loaded routing it approaches the
//     worker count.
//   - TTFT p99 with and without -hedge-after on a fleet whose workers stall
//     (infer_worker.py --stall-prob): hedging should pull the tail in, at
//     the cost of hedges launched.
//   - rate-limited count: how often tenants hit their bucket at this -c.
//
// The semantic cache is bypassed by default (-no-cache) so these numbers
// describe the serving path, not cache luck. -cache-test runs a separate
// small phase of 50 identical prompts and reports the cache hit rate.
//
//	llmbench -gateway 127.0.0.1:7500 -n 300 -c 16
//	llmbench -gateway 127.0.0.1:7500 -json >> runs.jsonl
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gatewayv1 "dsys/gen/gateway/v1"
)

// result is what one completed request contributes to the report.
type result struct {
	prefix    int
	ttft      time.Duration
	total     time.Duration
	tokens    int
	worker    string
	hedged    bool
	hedgeWon  bool
	prefixHit bool
	prefillMs int64
	cached    bool
	attempts  int // 1 + rate-limited retries
}

// report is the machine-readable summary (-json), one line per run.
type report struct {
	N                int                      `json:"n"`
	Concurrency      int                      `json:"c"`
	Tenants          int                      `json:"tenants"`
	Prefixes         int                      `json:"prefixes"`
	MaxTokens        int                      `json:"max_tokens"`
	NoCache          bool                     `json:"no_cache"`
	ElapsedMs        int64                    `json:"elapsed_ms"`
	Throughput       float64                  `json:"req_per_s"`
	TokensPerS       float64                  `json:"tokens_per_s"`
	Errors           int                      `json:"errors"`
	TTFTp50Ms        float64                  `json:"ttft_p50_ms"`
	TTFTp95Ms        float64                  `json:"ttft_p95_ms"`
	TTFTp99Ms        float64                  `json:"ttft_p99_ms"`
	Totalp50Ms       float64                  `json:"total_p50_ms"`
	Totalp99Ms       float64                  `json:"total_p99_ms"`
	PrefixHitRate    float64                  `json:"prefix_hit_rate"`
	PrefillMeanMs    float64                  `json:"prefill_mean_ms"`
	HedgesLaunched   int                      `json:"hedges_launched"`
	HedgesWon        int                      `json:"hedges_won"`
	RateLimited      int64                    `json:"rate_limited"`
	Cached           int                      `json:"cached"`
	PerWorker        map[string]int           `json:"per_worker"`
	PrefixSpread     float64                  `json:"prefix_spread_avg"`
	PrefixWorkers    map[string][]string      `json:"prefix_workers,omitempty"`
	CacheTestHits    int                      `json:"cache_test_hits,omitempty"`
	CacheTestN       int                      `json:"cache_test_n,omitempty"`
	CacheTestHitRate float64                  `json:"cache_test_hit_rate,omitempty"`
	Gateway          *gatewayv1.StatsResponse `json:"-"`
}

func main() {
	gwFlag := flag.String("gateway", "127.0.0.1:7500", "comma-separated gateway addresses (round-robin per client goroutine)")
	n := flag.Int("n", 300, "total requests")
	c := flag.Int("c", 16, "concurrent client goroutines, one request at a time each")
	tenants := flag.Int("tenants", 4, "distinct tenants (t0..), round-robin")
	prefixes := flag.Int("prefixes", 8, "distinct ~600-char system prompts, one per request at random")
	maxTokens := flag.Int("max-tokens", 32, "max_tokens per request")
	noCache := flag.Bool("no-cache", true, "bypass the semantic cache so the run measures the serving path")
	cacheTest := flag.Bool("cache-test", false, "also run a phase of 50 identical prompts and report cache hit rate")
	seed := flag.Int64("seed", 1, "seed for prompt generation and prefix choice")
	timeout := flag.Duration("timeout", 60*time.Second, "per-request deadline")
	jsonOut := flag.Bool("json", false, "print one JSON line instead of the table")
	verbose := flag.Bool("v", false, "print each prefix's worker set")
	flag.Parse()

	if *n <= 0 || *c <= 0 || *tenants <= 0 || *prefixes <= 0 {
		fatal(errors.New("-n, -c, -tenants and -prefixes must be positive"))
	}
	addrs := splitAddrs(*gwFlag)
	if len(addrs) == 0 {
		fatal(errors.New("-gateway is required"))
	}

	conns := make([]*grpc.ClientConn, len(addrs))
	clients := make([]gatewayv1.GatewayClient, len(addrs))
	for i, a := range addrs {
		cc, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fatal(fmt.Errorf("dial %s: %w", a, err))
		}
		defer cc.Close()
		conns[i] = cc
		clients[i] = gatewayv1.NewGatewayClient(cc)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sysPrompts := makePrefixes(*prefixes, *seed)
	// The prefix for each request is fixed up front so two runs with the same
	// seed send the same traffic (that is what makes -json diffs meaningful).
	rng := rand.New(rand.NewSource(*seed))
	reqPrefix := make([]int, *n)
	for i := range reqPrefix {
		reqPrefix[i] = rng.Intn(*prefixes)
	}

	before, _ := clients[0].Stats(ctx, &gatewayv1.StatsRequest{})

	results := make([]result, *n)
	errs := make([]error, *n)
	var next atomic.Int64
	var rateLimited atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			gw := clients[w%len(clients)]
			for {
				i := int(next.Add(1)) - 1
				if i >= *n {
					return
				}
				prompt := sysPrompts[reqPrefix[i]] + fmt.Sprintf("\n\nUser: question %d: %s", i, question(*seed, i))
				req := &gatewayv1.GenerateRequest{
					Tenant:    fmt.Sprintf("t%d", i%*tenants),
					Prompt:    prompt,
					MaxTokens: int32(*maxTokens),
					NoCache:   *noCache,
				}
				r, err := one(ctx, gw, req, *timeout, &rateLimited)
				r.prefix = reqPrefix[i]
				results[i], errs[i] = r, err
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	after, _ := clients[0].Stats(ctx, &gatewayv1.StatsRequest{})

	rep := summarise(results, errs, elapsed)
	rep.N, rep.Concurrency, rep.Tenants, rep.Prefixes, rep.MaxTokens, rep.NoCache = *n, *c, *tenants, *prefixes, *maxTokens, *noCache
	rep.RateLimited = rateLimited.Load()
	if after != nil {
		rep.Gateway = diffStats(before, after)
	}

	if *cacheTest {
		rep.CacheTestN = 50
		rep.CacheTestHits = runCacheTest(ctx, clients[0], sysPrompts[0], *tenants, *maxTokens, *timeout)
		rep.CacheTestHitRate = float64(rep.CacheTestHits) / float64(rep.CacheTestN)
	}

	if *jsonOut {
		if !*verbose {
			rep.PrefixWorkers = nil
		}
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(rep); err != nil {
			fatal(err)
		}
		return
	}
	printTable(os.Stdout, rep, *verbose, errs)
}

// one runs a single request to completion, retrying with backoff when the
// tenant is rate limited (the request is identical on each retry: generation
// has no side effects, so a retry is always safe). TTFT and total are for
// the attempt that succeeded; attempts counts the rate-limited ones.
func one(ctx context.Context, gw gatewayv1.GatewayClient, req *gatewayv1.GenerateRequest, timeout time.Duration, rateLimited *atomic.Int64) (result, error) {
	backoff := 50 * time.Millisecond
	var r result
	for {
		r.attempts++
		rr, err := attempt(ctx, gw, req, timeout)
		if err == nil {
			rr.attempts = r.attempts
			return rr, nil
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.ResourceExhausted {
			return r, err
		}
		rateLimited.Add(1)
		// The gateway's message may carry a retry hint, but a doubling
		// backoff capped at 1s is enough to drain a token bucket politely.
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(backoff + time.Duration(rand.Int63n(int64(backoff)))):
		}
		backoff = min(backoff*2, time.Second)
	}
}

func attempt(ctx context.Context, gw gatewayv1.GatewayClient, req *gatewayv1.GenerateRequest, timeout time.Duration) (result, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var r result
	t0 := time.Now()
	stream, err := gw.Generate(ctx, req)
	if err != nil {
		return r, err
	}
	first := true
	for {
		tok, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return r, err
		}
		if first {
			first = false
			r.ttft = time.Since(t0)
			r.worker = tok.Worker
			r.hedged, r.hedgeWon = tok.Hedged, tok.HedgeWon
			r.prefixHit, r.prefillMs = tok.PrefixCacheHit, tok.PrefillMs
			r.cached = tok.Cached
		}
		r.tokens++
		if tok.Done {
			// Drain to EOF so the gateway sees a clean close, not a cancel.
			continue
		}
	}
	if first {
		return r, errors.New("empty stream")
	}
	r.total = time.Since(t0)
	return r, nil
}

// runCacheTest sends the same prompt 50 times, sequentially, with the cache
// enabled. The first is a miss that populates the cache; the rest should be
// hits (cached=true on the first token) as long as the gateway stored it.
func runCacheTest(ctx context.Context, gw gatewayv1.GatewayClient, sysPrompt string, tenants, maxTokens int, timeout time.Duration) int {
	prompt := sysPrompt + "\n\nUser: cache test: summarise the above in one line."
	hits := 0
	var rl atomic.Int64
	for i := 0; i < 50; i++ {
		req := &gatewayv1.GenerateRequest{Tenant: fmt.Sprintf("t%d", i%tenants), Prompt: prompt, MaxTokens: int32(maxTokens)}
		r, err := one(ctx, gw, req, timeout, &rl)
		if err == nil && r.cached {
			hits++
		}
	}
	return hits
}

// -- prompt generation --------------------------------------------------------

var words = strings.Fields(`
consensus replica leader follower election term log entry commit apply snapshot
lease fence token generation heartbeat quorum partition retry idempotent dedupe
shard rebalance migrate controller config epoch route prefix cache hedge cancel
tenant bucket refill burst latency throughput tail percentile stream token prompt
worker registry discovery gateway stateless durable ordered linearizable`)

// makePrefixes returns k distinct system prompts of about 600 chars each,
// deterministic in seed. They start with a distinctive header so the
// gateway's routing prefix (first 256 chars) differs between them.
func makePrefixes(k int, seed int64) []string {
	out := make([]string, k)
	for i := range out {
		rng := rand.New(rand.NewSource(seed*1000 + int64(i)))
		var b strings.Builder
		fmt.Fprintf(&b, "System prompt #%d (persona %s-%s). You are an assistant for a distributed systems course. ",
			i, words[rng.Intn(len(words))], words[rng.Intn(len(words))])
		for b.Len() < 600 {
			b.WriteString(words[rng.Intn(len(words))])
			b.WriteByte(' ')
		}
		out[i] = strings.TrimSpace(b.String())
	}
	return out
}

func question(seed int64, i int) string {
	rng := rand.New(rand.NewSource(seed*7919 + int64(i)))
	parts := make([]string, 6)
	for j := range parts {
		parts[j] = words[rng.Intn(len(words))]
	}
	return "explain " + strings.Join(parts, " ") + "?"
}

// -- reporting ----------------------------------------------------------------

func summarise(results []result, errs []error, elapsed time.Duration) report {
	rep := report{PerWorker: map[string]int{}, PrefixWorkers: map[string][]string{}}
	var ttft, total []time.Duration
	var prefillSum float64
	tokens := 0
	prefixSets := map[int]map[string]bool{}
	for i, r := range results {
		if errs[i] != nil {
			rep.Errors++
			continue
		}
		ttft = append(ttft, r.ttft)
		total = append(total, r.total)
		tokens += r.tokens
		if r.prefixHit {
			rep.PrefixHitRate++
		}
		prefillSum += float64(r.prefillMs)
		if r.hedged {
			rep.HedgesLaunched++
		}
		if r.hedgeWon {
			rep.HedgesWon++
		}
		if r.cached {
			rep.Cached++
		}
		rep.PerWorker[r.worker]++
		if prefixSets[r.prefix] == nil {
			prefixSets[r.prefix] = map[string]bool{}
		}
		prefixSets[r.prefix][r.worker] = true
	}
	ok := len(ttft)
	rep.ElapsedMs = elapsed.Milliseconds()
	rep.Throughput = float64(ok) / elapsed.Seconds()
	rep.TokensPerS = float64(tokens) / elapsed.Seconds()
	if ok > 0 {
		sort.Slice(ttft, func(i, j int) bool { return ttft[i] < ttft[j] })
		sort.Slice(total, func(i, j int) bool { return total[i] < total[j] })
		rep.TTFTp50Ms, rep.TTFTp95Ms, rep.TTFTp99Ms = ms(pct(ttft, 50)), ms(pct(ttft, 95)), ms(pct(ttft, 99))
		rep.Totalp50Ms, rep.Totalp99Ms = ms(pct(total, 50)), ms(pct(total, 99))
		rep.PrefixHitRate /= float64(ok)
		rep.PrefillMeanMs = prefillSum / float64(ok)
	}
	if len(prefixSets) > 0 {
		sum := 0
		for p, set := range prefixSets {
			sum += len(set)
			ids := make([]string, 0, len(set))
			for id := range set {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			rep.PrefixWorkers[fmt.Sprintf("p%d", p)] = ids
		}
		rep.PrefixSpread = float64(sum) / float64(len(prefixSets))
	}
	return rep
}

func printTable(w io.Writer, rep report, verbose bool, errs []error) {
	fmt.Fprintf(w, "%d requests, %d goroutines, %d tenants, %d prefixes, max_tokens %d, no_cache=%v, %v total\n",
		rep.N, rep.Concurrency, rep.Tenants, rep.Prefixes, rep.MaxTokens, rep.NoCache, time.Duration(rep.ElapsedMs)*time.Millisecond)
	fmt.Fprintf(w, "throughput:    %.1f req/s  %.0f tokens/s  errors=%d\n", rep.Throughput, rep.TokensPerS, rep.Errors)
	fmt.Fprintf(w, "ttft:          p50 %.0fms  p95 %.0fms  p99 %.0fms\n", rep.TTFTp50Ms, rep.TTFTp95Ms, rep.TTFTp99Ms)
	fmt.Fprintf(w, "total:         p50 %.0fms  p99 %.0fms\n", rep.Totalp50Ms, rep.Totalp99Ms)
	fmt.Fprintf(w, "prefix cache:  hit rate %.1f%%  mean prefill %.0fms\n", 100*rep.PrefixHitRate, rep.PrefillMeanMs)
	fmt.Fprintf(w, "hedges:        launched %d  won %d\n", rep.HedgesLaunched, rep.HedgesWon)
	fmt.Fprintf(w, "rate limited:  %d (retried with backoff)  semantic-cache hits: %d\n", rep.RateLimited, rep.Cached)
	fmt.Fprintf(w, "prefix spread: %.2f distinct workers per prefix (1.00 = perfect affinity)\n", rep.PrefixSpread)

	ids := make([]string, 0, len(rep.PerWorker))
	for id := range rep.PerWorker {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	fmt.Fprintln(w, "per worker:")
	for _, id := range ids {
		name := id
		if name == "" {
			name = "(cache/none)"
		}
		fmt.Fprintf(w, "  %-32s %5d\n", name, rep.PerWorker[id])
	}
	if verbose {
		keys := make([]string, 0, len(rep.PrefixWorkers))
		for k := range rep.PrefixWorkers {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return len(keys[i]) < len(keys[j]) || (len(keys[i]) == len(keys[j]) && keys[i] < keys[j])
		})
		fmt.Fprintln(w, "per prefix -> workers:")
		for _, k := range keys {
			fmt.Fprintf(w, "  %-4s %s\n", k, strings.Join(rep.PrefixWorkers[k], ", "))
		}
	}
	if rep.CacheTestN > 0 {
		fmt.Fprintf(w, "cache test:    %d/%d identical prompts served from the semantic cache (%.0f%%)\n",
			rep.CacheTestHits, rep.CacheTestN, 100*rep.CacheTestHitRate)
	}
	if g := rep.Gateway; g != nil {
		fmt.Fprintf(w, "gateway delta: requests=%d rate_limited=%d cache_hits=%d hedges=%d/%d cancelled=%d worker_errors=%d live_workers=%d\n",
			g.Requests, g.RateLimited, g.CacheHits, g.HedgesLaunched, g.HedgesWon, g.Cancelled, g.WorkerErrors, g.LiveWorkers)
	}
	if rep.Errors > 0 {
		seen := map[string]int{}
		for _, err := range errs {
			if err != nil {
				seen[status.Convert(err).Message()]++
			}
		}
		fmt.Fprintln(w, "errors:")
		for msg, n := range seen {
			fmt.Fprintf(w, "  %4d  %s\n", n, msg)
		}
	}
}

// diffStats returns after-before for the cumulative counters, so a report
// describes this run even against a long-lived gateway.
func diffStats(before, after *gatewayv1.StatsResponse) *gatewayv1.StatsResponse {
	if before == nil {
		return after
	}
	return &gatewayv1.StatsResponse{
		Requests:       after.Requests - before.Requests,
		RateLimited:    after.RateLimited - before.RateLimited,
		CacheHits:      after.CacheHits - before.CacheHits,
		HedgesLaunched: after.HedgesLaunched - before.HedgesLaunched,
		HedgesWon:      after.HedgesWon - before.HedgesWon,
		Cancelled:      after.Cancelled - before.Cancelled,
		WorkerErrors:   after.WorkerErrors - before.WorkerErrors,
		LiveWorkers:    after.LiveWorkers,
	}
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)*p/100, len(sorted)-1)]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "llmbench:", status.Convert(err).Message())
	os.Exit(1)
}
