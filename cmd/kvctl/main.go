// Command kvctl is a small command-line client for the KV gRPC service.
//
//	kvctl [-addr a:1,b:2,c:3] [-timeout 10s] put <key> <value>
//	kvctl [-addr ...] get <key>
//	kvctl [-addr ...] del <key>
//	kvctl [-addr ...] cas <key> <expected|-> <value>   ("-" = expect absent)
//	kvctl [-addr ...] bench [-n 1000] [-c 8]
//
// -addr is a comma-separated list of servers. kvctl is cluster-aware: it
// talks to one server at a time and, when that server is unreachable, slow,
// or answers "not leader", moves on to the next one (or straight to the
// leader if the follower named it) and retries the SAME request.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "dsys/gen/kv/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

// Retry policy.
const (
	backoffMin = 10 * time.Millisecond
	backoffMax = 200 * time.Millisecond
	// attemptTimeout bounds a single RPC so a hung server surfaces as
	// DeadlineExceeded and we move on, instead of eating the whole budget.
	attemptTimeout = 2 * time.Second
)

// leaderHint matches the "leader=<addr>" fragment a follower may include in
// its "not leader" status message.
var leaderHint = regexp.MustCompile(`leader=([^\s,;)\]]+)`)

// cluster is a set of server addresses plus a cursor pointing at the one
// that most recently worked. Connections are created lazily and shared.
type cluster struct {
	mu      sync.Mutex
	addrs   []string
	conns   map[string]*grpc.ClientConn
	cur     int           // index into addrs of the preferred server
	retries atomic.Uint64 // total retried attempts, for bench
}

func newCluster(addrs []string) *cluster {
	return &cluster{addrs: addrs, conns: make(map[string]*grpc.ClientConn)}
}

func (c *cluster) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		cc.Close()
	}
}

// pick returns the preferred server's index, address, and client.
func (c *cluster) pick() (int, string, kvv1.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.cur
	addr := c.addrs[i]
	cc, ok := c.conns[addr]
	if !ok {
		var err error
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return i, addr, nil, err
		}
		c.conns[addr] = cc
	}
	return i, addr, kvv1.NewKVClient(cc), nil
}

// advance moves the cursor after a failure at index failed. It only moves if
// the cursor still points at the failed server, so concurrent callers do not
// leapfrog past a server that another goroutine has just found healthy. If
// hint names a leader, the cursor jumps there (adding the address if it is
// not already in the list).
func (c *cluster) advance(failed int, hint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hint != "" {
		for i, a := range c.addrs {
			if a == hint {
				c.cur = i
				return
			}
		}
		c.addrs = append(c.addrs, hint)
		c.cur = len(c.addrs) - 1
		return
	}
	if c.cur == failed {
		c.cur = (failed + 1) % len(c.addrs)
	}
}

// markGood remembers that server i answered, so the next call goes there first.
func (c *cluster) markGood(i int) {
	c.mu.Lock()
	if c.cur != i && i < len(c.addrs) {
		c.cur = i
	}
	c.mu.Unlock()
}

// retryable reports whether err means "try another server" and, if the
// server told us who the leader is, that address.
func retryable(err error) (ok bool, leader string) {
	st, isStatus := status.FromError(err)
	if !isStatus {
		return false, ""
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
	default:
		return false, ""
	}
	if m := leaderHint.FindStringSubmatch(st.Message()); m != nil {
		leader = m[1]
	}
	return true, leader
}

// attemptError is the last transient failure when do gives up: which server
// it came from, plus the gRPC status.
type attemptError struct {
	addr string
	err  error
}

func (e *attemptError) Error() string {
	return "gave up; last error from " + e.addr + ": " + status.Convert(e.err).Message()
}
func (e *attemptError) Unwrap() error { return e.err }

// do runs one logical request against the cluster, retrying on transient
// failures until ctx expires. fn is invoked with a fresh per-attempt context
// and whichever server is currently preferred.
//
// IMPORTANT: fn must close over a request built ONCE, outside do, so that
// every attempt carries the same RequestMeta. That is what makes the retry
// safe: a server that already applied the operation (e.g. the old leader
// committed it but the reply was lost) recognises the (client_id, request_id)
// pair and returns the original result instead of applying it again.
// Building a new meta per attempt would turn one put into N puts.
func (c *cluster) do(ctx context.Context, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	backoff := backoffMin
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			c.retries.Add(1)
		}
		i, addr, kv, err := c.pick()
		if err == nil {
			actx, cancel := context.WithTimeout(ctx, attemptTimeout)
			err = fn(actx, kv)
			cancel()
			if err == nil {
				c.markGood(i)
				return nil
			}
		}
		ok, leader := retryable(err)
		if !ok {
			return err // a real answer (NotFound, InvalidArgument, ...): do not retry
		}
		lastErr = &attemptError{addr: addr, err: err}
		if ctx.Err() != nil {
			return lastErr
		}
		c.advance(i, leader)
		if leader != "" {
			continue // we know where to go; no need to wait
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return lastErr
		}
		backoff = min(backoff*2, backoffMax)
	}
}

func main() {
	addrFlag := flag.String("addr", "localhost:7001", "comma-separated server addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "give up retrying a request after this long")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl [-addr host:port,...] [-timeout 10s] put|get|del|cas|bench ...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	var addrs []string
	for _, a := range strings.Split(*addrFlag, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		fatal(errors.New("-addr: no server addresses"))
	}

	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		fatal(err)
	}
	clientID = hex.EncodeToString(raw)

	cl := newCluster(addrs)
	defer cl.close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var err error
	cmd, a := flag.Arg(0), flag.Args()[1:]
	switch {
	case cmd == "put" && len(a) == 2:
		req := &kvv1.PutRequest{Meta: newMeta(), Key: a[0], Value: []byte(a[1])} // one meta for all attempts
		err = cl.do(ctx, func(ctx context.Context, kv kvv1.KVClient) error {
			_, err := kv.Put(ctx, req)
			return err
		})
		if err == nil {
			fmt.Println("ok")
		}
	case cmd == "get" && len(a) == 1:
		req := &kvv1.GetRequest{Key: a[0]}
		var r *kvv1.GetResponse
		err = cl.do(ctx, func(ctx context.Context, kv kvv1.KVClient) error {
			var err error
			r, err = kv.Get(ctx, req)
			return err
		})
		if err == nil {
			fmt.Println(pick(r.GetFound(), string(r.GetValue()), "(not found)"))
		}
	case cmd == "del" && len(a) == 1:
		req := &kvv1.DeleteRequest{Meta: newMeta(), Key: a[0]}
		var r *kvv1.DeleteResponse
		err = cl.do(ctx, func(ctx context.Context, kv kvv1.KVClient) error {
			var err error
			r, err = kv.Delete(ctx, req)
			return err
		})
		if err == nil {
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
		err = cl.do(ctx, func(ctx context.Context, kv kvv1.KVClient) error {
			var err error
			r, err = kv.CompareAndSwap(ctx, req)
			return err
		})
		if err == nil {
			fmt.Println(pick(r.GetSwapped(), "swapped", "not swapped, current="+string(r.GetCurrent())))
		}
	case cmd == "bench":
		err = bench(ctx, cl, a)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

// bench issues n puts spread across c goroutines and reports throughput,
// latency, and how many attempts had to be retried. Each put goes through the
// retry wrapper, so a leader change mid-run shows up as retries, not errors.
// Latency is measured per logical put, retries included.
func bench(ctx context.Context, cl *cluster, args []string) error {
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
				// Built once per logical put; every retry reuses this meta.
				req := &kvv1.PutRequest{Meta: newMeta(), Key: fmt.Sprintf("bench-%d", i), Value: []byte("v")}
				t0 := time.Now()
				err := cl.do(ctx, func(ctx context.Context, kv kvv1.KVClient) error {
					_, err := kv.Put(ctx, req)
					return err
				})
				if err != nil {
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
	fmt.Printf("retries: %d\n", cl.retries.Load())
	return nil
}

func pick(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}

func fatal(err error) {
	var ae *attemptError
	msg := status.Convert(err).Message()
	if errors.As(err, &ae) {
		msg = ae.Error()
	}
	fmt.Fprintln(os.Stderr, "kvctl:", msg)
	os.Exit(1)
}
