// gateway runs one replica of the client-facing inference gateway: the
// gatewayv1.Gateway gRPC service over a shared KV.
//
// Every gateway replica is stateless. Rate-limit buckets, the worker
// registry and the semantic cache's exact-match table all live in the KV,
// so any number of replicas behind one address enforce one budget, route
// over one live worker set and share one cache. Start as many as you like
// with the same -kv/-ctrl; workers register with any of them.
//
// Against a flat raftkv cluster, with prefix routing on and hedging after
// 150ms of silence:
//
//	gateway -addr 127.0.0.1:7500 -kv 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -hedge-after 150ms
//
// Against a sharded cluster:
//
//	gateway -addr 127.0.0.1:7500 -ctrl 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003
//
// Exactly one of -kv / -ctrl must be given. -prefix-routing=false picks the
// least-loaded worker instead; the Phase 5 benchmark (cmd/llmbench) compares
// the two on prefix-cache hit rate.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"dsys/gateway"
	gatewayv1 "dsys/gen/gateway/v1"
	"dsys/kv/client"
	"dsys/obs"
	"dsys/ratelimit"
	"dsys/router"
	"dsys/sched/kvadapter"
	"dsys/semcache"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7500", "address to serve the Gateway API on")
	kvFlag := flag.String("kv", "", "comma-separated addresses of a flat KV cluster (raftkv replicas or one kvserver)")
	ctrlFlag := flag.String("ctrl", "", "comma-separated shard controller addresses (sharded KV); use INSTEAD of -kv")
	prefixRouting := flag.Bool("prefix-routing", true, "route by rendezvous-hashing the prompt prefix (false = least-loaded worker)")
	prefixChars := flag.Int("prefix-chars", 0, "how many leading prompt chars form the routing prefix (0 = 256)")
	hedgeAfter := flag.Duration("hedge-after", 0, "launch a second attempt if no first token within this long (0 = off)")
	rate := flag.Float64("rate", 10, "rate-limit tokens per second per tenant")
	burst := flag.Float64("burst", 20, "rate-limit burst per tenant")
	leaseFraction := flag.Float64("rl-lease-fraction", 0.1, "lease up to this fraction of a tenant's burst per KV CAS and spend it locally (0 = exact, one CAS per request); see ratelimit.Options")
	leaseTTL := flag.Duration("rl-lease-ttl", time.Second, "drop unused leased tokens after this long")
	rlFailOpen := flag.Bool("rl-fail-open", true, "admit requests when the rate limiter's KV is unavailable instead of failing them (contention still fails); see gateway.Options.RateLimitFailOpen")
	cache := flag.Bool("cache", true, "enable the semantic cache")
	cacheNear := flag.Bool("cache-near", false, "also serve near-duplicate prompts (cosine >= -cache-threshold and <= 3 char edits); off = exact match only. Lexical, cannot see meaning; see semcache")
	cacheThreshold := flag.Float64("cache-threshold", 0.92, "cosine similarity for a near-duplicate cache hit (with -cache-near)")
	cacheTTL := flag.Duration("cache-ttl", 0, "expire cache entries after this long (0 = never)")
	maxInflight := flag.Int("max-inflight", 8, "routing skips a worker at or above this many streams (ignored with -load-factor)")
	loadFactor := flag.Float64("load-factor", 1.25, "bounded-load prefix routing: skip workers above this multiple of the fleet's average load (0 = plain affinity)")
	defaultMaxTokens := flag.Int("default-max-tokens", 0, "max_tokens when a request says 0 (0 = 64)")
	statsEvery := flag.Duration("stats-every", 10*time.Second, "how often to log Stats() (0 = never)")
	sessions := flag.Int("kv-sessions", kvadapter.DefaultSessions, "KV client identities kept for concurrent mutations")
	kvTimeout := flag.Duration("kv-timeout", 10*time.Second, "give up retrying one KV operation after this long")
	verbose := flag.Bool("v", false, "log KV client retries")
	otlpEndpoint := flag.String("otlp-endpoint", "", "OTLP/gRPC endpoint for traces, e.g. 127.0.0.1:4317 (empty = tracing disabled)")
	metricsAddr := flag.String("metrics-addr", ":9091", "address to serve Prometheus /metrics on")
	flag.Parse()

	kvAddrs := splitAddrs(*kvFlag)
	ctrlAddrs := splitAddrs(*ctrlFlag)
	if len(kvAddrs) == 0 && len(ctrlAddrs) == 0 {
		log.Fatal("exactly one of -kv or -ctrl is required")
	}
	if len(kvAddrs) > 0 && len(ctrlAddrs) > 0 {
		log.Fatal("-kv and -ctrl are mutually exclusive; give exactly one")
	}

	tracingShutdown, err := obs.InitTracing(context.Background(), "gateway", *otlpEndpoint)
	if err != nil {
		log.Fatalf("obs: init tracing: %v", err)
	}
	defer func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracingShutdown(shCtx); err != nil {
			log.Printf("obs: tracing shutdown: %v", err)
		}
	}()
	metrics := obs.NewMetrics("gateway")

	opts := []client.Option{client.WithTimeout(*kvTimeout)}
	if *verbose {
		opts = append(opts, client.WithLogger(log.Printf))
	}
	var kvc *client.Client
	var backend string
	if len(kvAddrs) > 0 {
		kvc = client.NewFlat(kvAddrs, opts...)
		backend = "flat kv " + strings.Join(kvAddrs, ",")
	} else {
		kvc = client.NewSharded(ctrlAddrs, opts...)
		backend = "sharded kv via controller " + strings.Join(ctrlAddrs, ",")
	}
	defer kvc.Close()

	// One adapter (one bounded pool of KV identities) serves every layer:
	// the limiter's CAS on buckets, the registry's CAS on the worker set and
	// the cache's Puts all go through it.
	kv := kvadapter.New(kvc, kvadapter.WithSessions(*sessions))

	limiter := ratelimit.New(kv, ratelimit.Options{Rate: *rate, Burst: *burst, LeaseFraction: *leaseFraction, LeaseTTL: *leaseTTL})
	registry := router.NewRegistry(kv, router.Options{})
	var semantic gateway.Cache
	cacheDesc := "off"
	if *cache {
		semantic = semcache.New(kv, semcache.NewNGramEmbedder(256), semcache.Options{
			Near:      *cacheNear,
			Threshold: float32(*cacheThreshold),
			TTL:       *cacheTTL,
		})
		cacheDesc = "exact"
		if *cacheNear {
			cacheDesc = "exact+near"
		}
	}

	srv := gateway.New(gateway.Options{
		KV:                   kv,
		Limiter:              limiter,
		RateLimitFailOpen:    *rlFailOpen,
		Cache:                semantic,
		Registry:             registry,
		PrefixRouting:        *prefixRouting,
		PrefixChars:          *prefixChars,
		MaxInflightPerWorker: *maxInflight,
		LoadFactor:           *loadFactor,
		HedgeAfter:           *hedgeAfter,
		DefaultMaxTokens:     int32(*defaultMaxTokens),
		Metrics:              metrics,
	})
	defer srv.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	gs := grpc.NewServer(obs.GRPCServerOption())
	gatewayv1.RegisterGatewayServer(gs, srv)

	metricsLis, err := net.Listen("tcp", *metricsAddr)
	if err != nil {
		log.Fatalf("listen (metrics) %s: %v", *metricsAddr, err)
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsSrv := &http.Server{Handler: metricsMux}
	go func() {
		if err := metricsSrv.Serve(metricsLis); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server: %v", err)
		}
	}()

	// SIGTERM as well as SIGINT: SIGTERM is what docker stop, Kubernetes
	// and systemd send, and without it the process skipped the graceful
	// path below and was SIGKILLed mid-stream after the grace period.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *statsEvery > 0 {
		go func() {
			t := time.NewTicker(*statsEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					log.Print("stats: ", formatStats(srv.Snapshot()))
				}
			}
		}()
	}

	go func() {
		<-ctx.Done()
		log.Print("shutting down")
		// GracefulStop waits for open token streams; a client that keeps a
		// stream open forever must not hold the process, so cap it. Stop()
		// cancels the stream contexts, which cancels the workers.
		done := make(chan struct{})
		go func() { gs.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			gs.Stop()
		}
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shCtx)
	}()

	log.Printf("gateway listening on %s (%s, prefix-routing %v, hedge-after %v, rate %.3g/s burst %.3g, cache %s, max-inflight %d), metrics on %s",
		lis.Addr(), backend, *prefixRouting, *hedgeAfter, *rate, *burst, cacheDesc, *maxInflight, metricsLis.Addr())
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	log.Print("final stats: ", formatStats(srv.Snapshot()))
}

// formatStats renders Stats on one line with a stable worker order.
func formatStats(s gateway.Stats) string {
	var b strings.Builder
	b.WriteString("requests=")
	b.WriteString(u(s.Requests))
	b.WriteString(" rate_limited=" + u(s.RateLimited))
	b.WriteString(" rl_fail_open=" + u(s.RateLimitFailOpen))
	b.WriteString(" cache_hits=" + u(s.CacheHits))
	b.WriteString(" hedges=" + u(s.HedgesLaunched) + "/" + u(s.HedgesWon))
	b.WriteString(" cancelled=" + u(s.Cancelled))
	b.WriteString(" worker_errors=" + u(s.WorkerErrors))
	b.WriteString(" live_workers=" + u(uint64(s.LiveWorkers)))
	ids := make([]string, 0, len(s.RoutedTo))
	for id := range s.RoutedTo {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	b.WriteString(" routed_to={")
	for i, id := range ids {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(id + ":" + u(s.RoutedTo[id]))
	}
	b.WriteString("}")
	return b.String()
}

func u(v uint64) string { return strconv.FormatUint(v, 10) }

func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
