// Command mockworker serves infer/mock over gRPC and keeps itself registered
// with one or more gateways, like py/infer_worker.py but in Go and without a
// thread-per-request cap.
//
// Its main use is measuring the GATEWAY: with near-zero prefill and token
// latency, the worker is never the bottleneck, so a load test measures what
// the gateway and its KV (rate limiting, cache lookups, routing) can do.
//
//	mockworker -addr 127.0.0.1:7660 -gateway 127.0.0.1:7650 -prefill-ms 0.01 -token-ms 0.01
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	gatewayv1 "dsys/gen/gateway/v1"
	inferv1 "dsys/gen/infer/v1"
	"dsys/infer/mock"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7660", "address to serve Inference on (also advertised)")
	gwFlag := flag.String("gateway", "127.0.0.1:7500", "comma-separated gateway addresses to register with")
	id := flag.String("id", "", "worker id (default go-<addr>)")
	prefillMs := flag.Float64("prefill-ms", 20, "base prefill latency (ms); the mock treats 0 as its default, so use a tiny value for ~none")
	perChar := flag.Float64("prefill-per-char", 0.25, "extra prefill per uncached prompt char (ms)")
	tokenMs := flag.Float64("token-ms", 12, "inter-token latency (ms)")
	cacheBlocks := flag.Int("kv-cache-blocks", 512, "prefix cache capacity in 64-char blocks")
	leaseMs := flag.Int64("lease-ms", 3000, "registration lease")
	flag.Parse()

	if *id == "" {
		*id = "go-" + *addr
	}
	w := mock.New(mock.Options{
		ID: *id, BasePrefillMs: *prefillMs, PrefillPerChar: *perChar,
		TokenMs: *tokenMs, CacheBlocks: *cacheBlocks,
	})

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	gs := grpc.NewServer()
	inferv1.RegisterInferenceServer(gs, w)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	// SIGTERM too: it is what `docker stop` (and systemd, k8s) sends, and
	// without it the graceful path below never runs in a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go registerLoop(ctx, strings.Split(*gwFlag, ","), *id, *addr, *leaseMs, w)

	log.Printf("mockworker %s serving on %s (prefill %.3fms + %.3fms/char, token %.3fms)",
		*id, *addr, *prefillMs, *perChar, *tokenMs)
	<-ctx.Done()
	gs.GracefulStop()
}

// registerLoop renews the lease at a third of its length against the first
// gateway that answers, rotating on failure. Gateways share the registry
// through the KV, so registering with any one of them is enough.
func registerLoop(ctx context.Context, gws []string, id, addr string, leaseMs int64, w *mock.Worker) {
	idx := 0
	t := time.NewTicker(time.Duration(leaseMs/3) * time.Millisecond)
	defer t.Stop()
	for {
		target := strings.TrimSpace(gws[idx%len(gws)])
		cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = gatewayv1.NewGatewayClient(cc).RegisterWorker(rctx, &gatewayv1.RegisterWorkerRequest{
				WorkerId: id, Addr: addr, Inflight: w.Inflight(), LeaseMs: leaseMs, Model: "mock",
			})
			cancel()
			_ = cc.Close()
		}
		if err != nil {
			log.Printf("register with %s: %v; trying next gateway", target, err)
			idx++
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
