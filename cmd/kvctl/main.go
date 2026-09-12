// Command kvctl is a small command-line client for the KV gRPC service.
//
//	kvctl [-addr host:port] put <key> <value>
//	kvctl [-addr host:port] get <key>
//	kvctl [-addr host:port] del <key>
//	kvctl [-addr host:port] cas <key> <expected|-> <value>   ("-" = expect absent)
//	kvctl [-addr host:port] bench [-n 1000] [-c 8]
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "dsys/gen/kv/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// clientID identifies this process; nextReqID numbers its mutating requests.
// Together they form RequestMeta, which the server uses to deduplicate. A retry
// of a failed or timed-out mutation MUST reuse the same meta so the server can
// recognise it as the same operation instead of applying it twice.
var (
	clientID  string
	nextReqID atomic.Uint64
)

func newMeta() *kvv1.RequestMeta {
	return &kvv1.RequestMeta{ClientId: clientID, RequestId: nextReqID.Add(1)}
}

func main() {
	addr := flag.String("addr", "localhost:7001", "server address")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl [-addr host:port] put|get|del|cas|bench ...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		fatal(err)
	}
	clientID = hex.EncodeToString(raw)

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	kv := kvv1.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd, a := flag.Arg(0), flag.Args()[1:]
	switch {
	case cmd == "put" && len(a) == 2:
		if _, err = kv.Put(ctx, &kvv1.PutRequest{Meta: newMeta(), Key: a[0], Value: []byte(a[1])}); err == nil {
			fmt.Println("ok")
		}
	case cmd == "get" && len(a) == 1:
		var r *kvv1.GetResponse
		if r, err = kv.Get(ctx, &kvv1.GetRequest{Key: a[0]}); err == nil {
			fmt.Println(pick(r.GetFound(), string(r.GetValue()), "(not found)"))
		}
	case cmd == "del" && len(a) == 1:
		var r *kvv1.DeleteResponse
		if r, err = kv.Delete(ctx, &kvv1.DeleteRequest{Meta: newMeta(), Key: a[0]}); err == nil {
			fmt.Println(pick(r.GetExisted(), "deleted", "(not found)"))
		}
	case cmd == "cas" && len(a) == 3:
		req := &kvv1.CASRequest{Meta: newMeta(), Key: a[0], Value: []byte(a[2])}
		if a[1] == "-" {
			req.ExpectAbsent = true
		} else {
			req.Expected = []byte(a[1])
		}
		var r *kvv1.CASResponse
		if r, err = kv.CompareAndSwap(ctx, req); err == nil {
			fmt.Println(pick(r.GetSwapped(), "swapped", "not swapped, current="+string(r.GetCurrent())))
		}
	case cmd == "bench":
		err = bench(kv, a)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

// bench issues n puts spread across c goroutines and reports throughput and latency.
func bench(kv kvv1.KVClient, args []string) error {
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
			for {
				i := int(next.Add(1)) - 1
				if i >= *n || firstErr.Load() != nil {
					return
				}
				req := &kvv1.PutRequest{Meta: newMeta(), Key: fmt.Sprintf("bench-%d", i), Value: []byte("v")}
				t0 := time.Now()
				if _, err := kv.Put(context.Background(), req); err != nil {
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
	return nil
}

func pick(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "kvctl:", status.Convert(err).Message())
	os.Exit(1)
}
