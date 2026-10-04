// Command kvctl is a small command-line client for the KV gRPC service.
//
//	kvctl [-addr a:1,b:2,c:3] [-timeout 10s] put <key> <value>
//	kvctl [-addr ...] get <key>
//	kvctl [-addr ...] del <key>
//	kvctl [-addr ...] cas <key> <expected|-> <value>   ("-" = expect absent)
//	kvctl [-addr ...] bench [-n 1000] [-c 8]
//	kvctl -addr <one node> [-timeout 3s] health          (that node's OWN status; exit 0 = SERVING)
//
// -addr is a comma-separated list of servers for a single flat cluster (one
// Raft group holding the whole keyspace, as in Phase 2). -ctrl is a
// comma-separated list of shard controller addresses, used INSTEAD of -addr
// for a sharded cluster (several Raft groups, each owning a subset of the
// keyspace, per package shardkv). Exactly one of -addr / -ctrl must be
// given.
//
// All of the cluster awareness (retry the SAME request against the next
// replica, follow "not leader" hints, resolve a key's owning group from the
// controller and re-resolve on "wrong group") lives in package kv/client;
// kvctl is a thin CLI over it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dsys/kv/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func main() {
	addrFlag := flag.String("addr", "", "comma-separated server addresses (flat, single-group mode)")
	ctrlFlag := flag.String("ctrl", "", "comma-separated shard controller addresses (sharded mode)")
	timeout := flag.Duration("timeout", 10*time.Second, "give up retrying a request after this long")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl (-addr host:port,... | -ctrl host:port,...) [-timeout 10s] put|get|del|cas|bench|health ...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	addrs := parseAddrs(*addrFlag)
	ctrlAddrs := parseAddrs(*ctrlFlag)
	if flag.Arg(0) == "health" {
		os.Exit(health(addrs, *timeout))
	}
	if len(addrs) == 0 && len(ctrlAddrs) == 0 {
		fatal(errors.New("exactly one of -addr or -ctrl is required"))
	}
	if len(addrs) > 0 && len(ctrlAddrs) > 0 {
		fatal(errors.New("-addr and -ctrl are mutually exclusive; give exactly one"))
	}

	var c *client.Client
	if len(addrs) > 0 {
		c = client.NewFlat(addrs)
	} else {
		c = client.NewSharded(ctrlAddrs)
	}
	defer c.Close()

	// One deadline for the whole command, retries included (bench too).
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var err error
	cmd, a := flag.Arg(0), flag.Args()[1:]
	switch {
	case cmd == "put" && len(a) == 2:
		if err = c.Put(ctx, a[0], []byte(a[1])); err == nil {
			fmt.Println("ok")
		}
	case cmd == "get" && len(a) == 1:
		var v []byte
		var found bool
		if v, found, err = c.Get(ctx, a[0]); err == nil {
			fmt.Println(pick(found, string(v), "(not found)"))
		}
	case cmd == "del" && len(a) == 1:
		var existed bool
		if existed, err = c.Delete(ctx, a[0]); err == nil {
			fmt.Println(pick(existed, "deleted", "(not found)"))
		}
	case cmd == "cas" && len(a) == 3:
		var expected []byte
		expectAbsent := a[1] == "-"
		if !expectAbsent {
			expected = []byte(a[1])
		}
		var swapped bool
		var current []byte
		if swapped, current, err = c.CAS(ctx, a[0], expected, expectAbsent, []byte(a[2])); err == nil {
			fmt.Println(pick(swapped, "swapped", "not swapped, current="+string(current)))
		}
	case cmd == "bench":
		err = bench(ctx, c, a)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

func parseAddrs(flagVal string) []string {
	var out []string
	for _, a := range strings.Split(flagVal, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// bench issues n puts spread across c goroutines and reports throughput,
// latency, and how many attempts had to be retried (in sharded mode, this
// includes retries caused by the shard having moved to a different group
// mid-run). A leader change, or a shard reassignment, shows up as retries,
// not errors. Latency is measured per logical put, retries included.
//
// Keys are named bench-<i>; shard.Key2Shard hashes them, so with more than
// a handful of keys they naturally spread across multiple shards (and, in
// sharded mode, multiple groups) without any special-casing here.
func bench(ctx context.Context, root *client.Client, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 1000, "total number of puts")
	c := fs.Int("c", 8, "concurrent goroutines")
	fs.Parse(args)
	if *n <= 0 || *c <= 0 {
		return errors.New("bench: -n and -c must be positive")
	}

	lat := make([]time.Duration, *n)
	var next atomic.Int64
	var firstErr atomic.Pointer[error]
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker gets its OWN client identity via Session(), not
			// the shared root Client. The store's dedup table assumes at
			// most one outstanding request per client at a time (see
			// kv/store's package doc); this worker issues its puts one at
			// a time, sequentially, so a private counter satisfies that
			// exactly, whereas *c* workers sharing one identity would not:
			// they run concurrently, so two of their puts can commit out of
			// order relative to the shared counter, and the one that lands
			// second with a now-lower id is correctly refused as stale
			// (store.ErrStaleRequest) rather than silently misapplied,
			// which is safe, but would abort the whole benchmark run on
			// what is actually normal concurrent traffic, not a fault.
			kv := root.Session()
			for {
				i := int(next.Add(1)) - 1
				if i >= *n || firstErr.Load() != nil {
					return
				}
				// Put builds its meta once per logical put; every retry
				// inside it reuses that meta.
				t0 := time.Now()
				if err := kv.Put(ctx, fmt.Sprintf("bench-%d", i), []byte("v")); err != nil {
					firstErr.CompareAndSwap(nil, &err)
					return
				}
				lat[i] = time.Since(t0)
			}
		}()
	}
	wg.Wait()
	if ep := firstErr.Load(); ep != nil {
		return *ep
	}
	elapsed := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	fmt.Printf("%d puts, %d goroutines, %v total\n", *n, *c, elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f ops/s\n", float64(*n)/elapsed.Seconds())
	fmt.Printf("latency p50: %v  p99: %v\n", lat[*n*50/100], lat[min(*n*99/100, *n-1)])
	// Retries() is cumulative across the root and every Session, so this
	// covers all workers (and, in sharded mode, controller re-queries).
	fmt.Printf("retries: %d\n", root.Retries())
	return nil
}

// health asks exactly ONE node (raftkv, shardkv or shardctrl, at its client
// address) for its own grpc.health.v1 status and exits 0 only on SERVING.
// Deliberately no failover and no leader-following: the question is "is
// THIS node healthy", which a request any majority could answer cannot
// tell (see cmd/internal/node.Health for how a node decides).
func health(addrs []string, timeout time.Duration) int {
	if len(addrs) != 1 {
		fmt.Fprintln(os.Stderr, "kvctl: health takes exactly one -addr (the node to check)")
		return 2
	}
	conn, err := grpc.NewClient(addrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		return 1
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvctl: health:", status.Convert(err).Message())
		return 1
	}
	fmt.Println(resp.GetStatus())
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return 1
	}
	return 0
}

func pick(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}

func fatal(err error) {
	var ae *client.AttemptError
	msg := status.Convert(err).Message()
	if errors.As(err, &ae) {
		msg = ae.Error()
	}
	fmt.Fprintln(os.Stderr, "kvctl:", msg)
	os.Exit(1)
}
