// kvserver runs a single-node durable KV store over gRPC.
//
//	kvserver -addr 127.0.0.1:7001 -data ./data/node1
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/server"
	"dsys/kv/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7001", "listen address (loopback by default, like every other server here; use :7001 to accept remote clients)")
	dataDir := flag.String("data", "data/node1", "data directory")
	noSync := flag.Bool("nosync", false, "disable fsync (UNSAFE: loses acknowledged writes on crash)")
	flag.Parse()

	start := time.Now()
	st, err := store.Open(*dataDir, store.Options{NoSync: *noSync})
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	log.Printf("recovered %d keys from %s in %s", st.Len(), *dataDir, time.Since(start).Round(time.Millisecond))

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	kvv1.RegisterKVServer(gs, server.New(st))

	// SIGTERM too: it is what `docker stop` (and systemd, k8s) sends, and
	// without it the graceful path below never runs in a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Print("shutting down")
		gs.GracefulStop()
	}()

	log.Printf("kvserver listening on %s", lis.Addr())
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	if err := st.Close(); err != nil {
		log.Printf("close store: %v", err)
	}
}
