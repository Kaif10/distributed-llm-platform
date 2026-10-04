// sched runs one scheduler replica: the schedv1.Scheduler gRPC service over
// a job queue whose entire state lives in the KV store.
//
// Because the scheduler holds no state of its own, any number of replicas
// can serve the same queue (same -prefix, same KV) at once; producers and
// workers may talk to whichever one they like. Each replica also runs a
// reaper that reclaims jobs whose lease expired, but only while it holds
// the queue's leader lease in the KV, so expired jobs are reaped once, not
// once per replica.
//
// Against a flat raftkv cluster:
//
//	sched -addr 127.0.0.1:9001 -kv 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
//
// Against a sharded cluster (owning groups are resolved via the controller):
//
//	sched -addr 127.0.0.1:9001 -ctrl 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003
//
// Exactly one of -kv / -ctrl must be given.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	schedv1 "dsys/gen/sched/v1"
	"dsys/kv/client"
	"dsys/obs"
	"dsys/sched"
	"dsys/sched/grpcserver"
	"dsys/sched/kvadapter"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "address to serve the Scheduler API on")
	kvFlag := flag.String("kv", "", "comma-separated addresses of a flat KV cluster (raftkv replicas or one kvserver)")
	ctrlFlag := flag.String("ctrl", "", "comma-separated shard controller addresses (sharded KV); use INSTEAD of -kv")
	prefix := flag.String("prefix", "sched", "key prefix namespacing this queue in the KV")
	maxQueue := flag.Uint64("max-queue", 0, "maximum live jobs before Submit is refused (0 = 1024)")
	lease := flag.Duration("lease", 10*time.Second, "default lease when a claimer passes 0")
	maxAttempts := flag.Uint("max-attempts", 0, "attempts before a job goes FAILED instead of being requeued (0 = 3)")
	reapInterval := flag.Duration("reap-interval", time.Second, "how often the reaper scans for expired leases")
	sessions := flag.Int("kv-sessions", kvadapter.DefaultSessions, "KV client identities kept for concurrent mutations")
	kvTimeout := flag.Duration("kv-timeout", 10*time.Second, "give up retrying one KV operation after this long")
	otlpEndpoint := flag.String("otlp-endpoint", "", "OTLP/gRPC endpoint for traces, e.g. 127.0.0.1:4317 (empty = tracing disabled)")
	metricsAddr := flag.String("metrics-addr", ":9092", "address to serve Prometheus /metrics on")
	queueDepthEvery := flag.Duration("queue-depth-every", 2*time.Second, "how often to refresh the sched_queue_depth gauge")
	flag.Parse()

	kvAddrs := splitAddrs(*kvFlag)
	ctrlAddrs := splitAddrs(*ctrlFlag)
	if len(kvAddrs) == 0 && len(ctrlAddrs) == 0 {
		log.Fatal("exactly one of -kv or -ctrl is required")
	}
	if len(kvAddrs) > 0 && len(ctrlAddrs) > 0 {
		log.Fatal("-kv and -ctrl are mutually exclusive; give exactly one")
	}

	tracingShutdown, err := obs.InitTracing(context.Background(), "sched", *otlpEndpoint)
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
	metrics := obs.NewMetrics("sched")

	opts := []client.Option{client.WithTimeout(*kvTimeout), client.WithLogger(log.Printf)}
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

	q := sched.New(kvadapter.New(kvc, kvadapter.WithSessions(*sessions)), sched.Options{
		Prefix:       *prefix,
		MaxQueue:     *maxQueue,
		DefaultLease: *lease,
		MaxAttempts:  uint32(*maxAttempts),
	})

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	gs := grpc.NewServer(obs.GRPCServerOption())
	schedv1.RegisterSchedulerServer(gs, grpcserver.New(q, metrics))

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

	// SIGTERM too: it is what `docker stop` (and systemd, k8s) sends, and
	// without it the graceful path below never runs in a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The reaper identifies itself by host:addr so the leader record in the
	// KV says which replica is currently reaping.
	hostname, _ := os.Hostname()
	holder := hostname + ":" + lis.Addr().String()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sched.RunReaper(ctx, q, holder, *reapInterval)
	}()

	// sched_queue_depth (tail-head) has no natural mutation hook the way the
	// gRPC-level counters do, so a small ticker polls Stats() the way the
	// task description asks for.
	go func() {
		t := time.NewTicker(*queueDepthEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if head, tail, err := q.Stats(ctx); err == nil {
					metrics.SetQueueDepth(int64(tail) - int64(head))
				}
			}
		}
	}()

	go func() {
		<-ctx.Done()
		log.Print("shutting down")
		// Let in-flight RPCs finish, but not forever: a CAS against a KV
		// that is itself mid-election could otherwise hold us up.
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

	log.Printf("sched listening on %s (prefix %q, %s, reaper %q every %v), metrics on %s",
		lis.Addr(), *prefix, backend, holder, *reapInterval, metricsLis.Addr())
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	wg.Wait() // reaper releases (or lets lapse) its leader lease on ctx cancel
}

func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
