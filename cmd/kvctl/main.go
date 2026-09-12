// Command kvctl is a small command-line client for the KV gRPC service.
//
//	kvctl [-addr a:1,b:2,c:3] [-timeout 10s] put <key> <value>
//	kvctl [-addr ...] get <key>
//	kvctl [-addr ...] del <key>
//	kvctl [-addr ...] cas <key> <expected|-> <value>   ("-" = expect absent)
//	kvctl [-addr ...] bench [-n 1000] [-c 8]
//
// -addr is a comma-separated list of servers for a single flat cluster (one
// Raft group holding the whole keyspace, as in Phase 2). kvctl is
// cluster-aware in this mode: it talks to one server at a time and, when
// that server is unreachable, slow, or answers "not leader", moves on to
// the next one (or straight to the leader if the follower named it) and
// retries the SAME request.
//
// -ctrl is a comma-separated list of shard controller addresses, used
// INSTEAD of -addr for a sharded cluster (several Raft groups, each owning
// a subset of the keyspace, per package shardkv). In this mode kvctl asks
// the controller which group currently owns each key's shard (caching that
// answer briefly so e.g. bench does not re-query on every single op) and
// then talks to that group's replicas using the exact same
// retry/leader-hint machinery as -addr mode. If a group answers "wrong
// group for this key's shard" (the shard moved since we last asked), kvctl
// invalidates its cached config, re-queries, and retries against whichever
// group owns it now.
//
// Exactly one of -addr / -ctrl must be given.
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
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "dsys/gen/kv/v1"
	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/shard"

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

// randomHexID returns a fresh random client identity, the same way the
// process-wide one below is generated.
func randomHexID() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

// Retry policy.
const (
	backoffMin = 10 * time.Millisecond
	backoffMax = 200 * time.Millisecond
	// attemptTimeout bounds a single RPC so a hung server surfaces as
	// DeadlineExceeded and we move on, instead of eating the whole budget.
	attemptTimeout = 2 * time.Second
	// configCacheTTL bounds how long a sharded-mode config answer is
	// reused before kvctl asks the controller again. Short enough that a
	// shard move is picked up quickly on its own (on top of the immediate
	// invalidation a wrong-group error already triggers), long enough that
	// bench does not hammer the controller once per op.
	configCacheTTL = 200 * time.Millisecond
)

// leaderHint matches the "leader=<addr>" fragment a follower may include in
// its "not leader" status message.
var leaderHint = regexp.MustCompile(`leader=([^\s,;)\]]+)`)

// totalRetries counts every retried attempt across every cluster this
// process talks to: a single global counter rather than a per-cluster
// field, since sharded mode builds a fresh *cluster[kvv1.KVClient] each
// time the owning group changes and retries must still accumulate across
// that. bench reports it, including retries caused by config invalidation.
var totalRetries atomic.Uint64

// ---------------------------------------------------------------------------
// connPool: one *grpc.ClientConn per address, shared by every cluster[C]
// this process creates. Sharded mode builds a new cluster[kvv1.KVClient]
// each time it resolves a key's owning group, but that is cheap because
// connections to addresses already seen are reused rather than redialed.
// ---------------------------------------------------------------------------

type connPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newConnPool() *connPool { return &connPool{conns: make(map[string]*grpc.ClientConn)} }

func (p *connPool) get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cc, ok := p.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = cc
	return cc, nil
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cc := range p.conns {
		cc.Close()
	}
}

// ---------------------------------------------------------------------------
// cluster[C]: a set of server addresses plus a cursor pointing at the one
// that most recently worked. Generic over the client stub type so the same
// retry/leader-hint machinery serves both the KV API (kvv1.KVClient) and,
// in sharded mode, the ShardCtrl API (shardctrlv1.ShardCtrlClient) used to
// ask the controller for the current config.
// ---------------------------------------------------------------------------

type cluster[C any] struct {
	mu        sync.Mutex
	addrs     []string
	pool      *connPool
	newClient func(grpc.ClientConnInterface) C
	cur       int
}

func newCluster[C any](addrs []string, pool *connPool, newClient func(grpc.ClientConnInterface) C) *cluster[C] {
	return &cluster[C]{addrs: addrs, pool: pool, newClient: newClient}
}

// pick returns the preferred server's index, address, and client.
func (c *cluster[C]) pick() (int, string, C, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero C
	i := c.cur
	addr := c.addrs[i]
	cc, err := c.pool.get(addr)
	if err != nil {
		return i, addr, zero, err
	}
	return i, addr, c.newClient(cc), nil
}

// advance moves the cursor after a failure at index failed. It only moves if
// the cursor still points at the failed server, so concurrent callers do not
// leapfrog past a server that another goroutine has just found healthy. If
// hint names a leader, the cursor jumps there (adding the address if it is
// not already in the list).
func (c *cluster[C]) advance(failed int, hint string) {
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
func (c *cluster[C]) markGood(i int) {
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

// attemptError is the last transient failure when doOn gives up: which
// server it came from, plus the gRPC status.
type attemptError struct {
	addr string
	err  error
}

func (e *attemptError) Error() string {
	return "gave up; last error from " + e.addr + ": " + status.Convert(e.err).Message()
}
func (e *attemptError) Unwrap() error { return e.err }

// doOn runs one logical request against cl, retrying on transient failures
// until ctx expires. fn is invoked with a fresh per-attempt context and
// whichever server cl currently prefers. This is the ONE retry/leader-hint
// loop kvctl has: -addr mode calls it directly on its single flat cluster,
// and sharded mode calls it once per group (see shardedClient.do), so
// neither mode duplicates the logic.
//
// IMPORTANT: fn must close over a request built ONCE, outside doOn, so that
// every attempt carries the same RequestMeta. That is what makes the retry
// safe: a server that already applied the operation (e.g. the old leader
// committed it but the reply was lost) recognises the (client_id, request_id)
// pair and returns the original result instead of applying it again.
// Building a new meta per attempt would turn one put into N puts.
func doOn[C any](ctx context.Context, cl *cluster[C], fn func(ctx context.Context, c C) error) error {
	backoff := backoffMin
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			totalRetries.Add(1)
		}
		i, addr, c, err := cl.pick()
		if err == nil {
			actx, cancel := context.WithTimeout(ctx, attemptTimeout)
			err = fn(actx, c)
			cancel()
			if err == nil {
				cl.markGood(i)
				return nil
			}
		}
		ok, hintedLeader := retryable(err)
		if !ok {
			return err // a real answer (NotFound, InvalidArgument, wrong group, ...): do not retry here
		}
		lastErr = &attemptError{addr: addr, err: err}
		if ctx.Err() != nil {
			return lastErr
		}
		cl.advance(i, hintedLeader)
		if hintedLeader != "" {
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

// ---------------------------------------------------------------------------
// Sharded mode: resolve a key's owning group from the shard controller,
// then run the request against that group's cluster.
// ---------------------------------------------------------------------------

func newKVClientFromConn(cc grpc.ClientConnInterface) kvv1.KVClient { return kvv1.NewKVClient(cc) }

// shardedClient implements sharded-mode dispatch: which group owns a key's
// shard, cached briefly, with wrong-group invalidation on top.
type shardedClient struct {
	pool *connPool
	ctrl *cluster[shardctrlv1.ShardCtrlClient]

	mu      sync.Mutex
	cfg     *shardctrlv1.Config
	fetched time.Time

	// groupClusters caches one cluster per group id for the life of this
	// shardedClient. A cluster remembers which of its replicas last
	// answered as leader (see cluster.pick/markGood), so reusing it across
	// requests to the same group is what makes steady-state traffic avoid
	// a "wrong replica, here's the leader" round trip on every single op.
	// Rebuilding a fresh cluster per request (the original bug here) throws
	// that memory away every time, so a request would need on average one
	// retry per call even with nothing wrong in the cluster: with n
	// replicas, guessing cold hits the actual leader only 1/n of the time.
	groupClusters map[int64]*cluster[kvv1.KVClient]
}

func newShardedClient(ctrlAddrs []string, pool *connPool) *shardedClient {
	return &shardedClient{
		pool: pool,
		ctrl: newCluster(ctrlAddrs, pool, func(cc grpc.ClientConnInterface) shardctrlv1.ShardCtrlClient {
			return shardctrlv1.NewShardCtrlClient(cc)
		}),
		groupClusters: make(map[int64]*cluster[kvv1.KVClient]),
	}
}

// config returns the cached controller config, refreshing it if stale or
// if force is set (used right after a wrong-group error).
func (s *shardedClient) config(ctx context.Context, force bool) (*shardctrlv1.Config, error) {
	s.mu.Lock()
	if !force && s.cfg != nil && time.Since(s.fetched) < configCacheTTL {
		cfg := s.cfg
		s.mu.Unlock()
		return cfg, nil
	}
	s.mu.Unlock()

	var resp *shardctrlv1.QueryResponse
	err := doOn(ctx, s.ctrl, func(ctx context.Context, c shardctrlv1.ShardCtrlClient) error {
		var err error
		resp, err = c.Query(ctx, &shardctrlv1.QueryRequest{Num: -1})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("query shard controller: %w", err)
	}
	s.mu.Lock()
	s.cfg = resp.GetConfig()
	s.fetched = time.Now()
	cfg := s.cfg
	s.mu.Unlock()
	return cfg, nil
}

func (s *shardedClient) invalidate() {
	s.mu.Lock()
	s.cfg = nil
	s.mu.Unlock()
}

// ownerCluster returns a cluster for whichever group currently owns key's
// shard, per the (possibly cached) config.
func (s *shardedClient) ownerCluster(ctx context.Context, key string, force bool) (*cluster[kvv1.KVClient], error) {
	cfg, err := s.config(ctx, force)
	if err != nil {
		return nil, err
	}
	shardID := shard.Key2Shard(key)
	shards := cfg.GetShards()
	if shardID >= len(shards) {
		return nil, fmt.Errorf("kvctl: controller config has no entry for shard %d", shardID)
	}
	gid := shards[shardID]
	if gid == 0 {
		return nil, fmt.Errorf("kvctl: shard %d is currently unassigned", shardID)
	}
	group := cfg.GetGroups()[gid]
	if group == nil || len(group.GetAddrs()) == 0 {
		return nil, fmt.Errorf("kvctl: no known addresses for group %d (shard %d)", gid, shardID)
	}
	return s.clusterForGroup(gid, group.GetAddrs()), nil
}

// clusterForGroup returns the cached cluster for gid, creating one (or
// replacing it, on the rare event the group's address list itself changed)
// the first time it is needed. See groupClusters' doc comment for why this
// caching matters.
func (s *shardedClient) clusterForGroup(gid int64, addrs []string) *cluster[kvv1.KVClient] {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl, ok := s.groupClusters[gid]; ok && slices.Equal(cl.addrs, addrs) {
		return cl
	}
	cl := newCluster(addrs, s.pool, newKVClientFromConn)
	s.groupClusters[gid] = cl
	return cl
}

// isWrongGroup reports whether err is the FailedPrecondition status
// shardkv.Server.toStatus produces for ErrWrongGroup ("shardkv: wrong
// group for this key's shard"): the shard has moved to a different group
// since we last asked the controller.
func isWrongGroup(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.FailedPrecondition && strings.Contains(st.Message(), "wrong group")
}

// do runs one logical KV request in sharded mode: resolve the owning
// group, run it there via doOn (so within-group leader hints are followed
// exactly as in -addr mode), and on a wrong-group failure invalidate the
// cached config and retry against whichever group owns the shard now.
func (s *shardedClient) do(ctx context.Context, key string, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	force := false
	for {
		cl, err := s.ownerCluster(ctx, key, force)
		if err != nil {
			return err
		}
		err = doOn(ctx, cl, fn)
		if err == nil {
			return nil
		}
		if isWrongGroup(err) {
			totalRetries.Add(1)
			s.invalidate()
			force = true
			if ctx.Err() != nil {
				return err
			}
			continue
		}
		return err
	}
}

// ---------------------------------------------------------------------------
// kvRunner: the one seam put/get/del/cas/bench go through, so they do not
// need to know whether they are in flat (-addr) or sharded (-ctrl) mode.
// ---------------------------------------------------------------------------

type kvRunner interface {
	run(ctx context.Context, key string, fn func(ctx context.Context, kv kvv1.KVClient) error) error
}

// flatRunner is -addr mode: one cluster for the whole keyspace, no
// controller involved.
type flatRunner struct{ cl *cluster[kvv1.KVClient] }

func (r flatRunner) run(ctx context.Context, _ string, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	return doOn(ctx, r.cl, fn)
}

// shardedRunner is -ctrl mode.
type shardedRunner struct{ sc *shardedClient }

func (r shardedRunner) run(ctx context.Context, key string, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	return r.sc.do(ctx, key, fn)
}

func main() {
	addrFlag := flag.String("addr", "", "comma-separated server addresses (flat, single-group mode)")
	ctrlFlag := flag.String("ctrl", "", "comma-separated shard controller addresses (sharded mode)")
	timeout := flag.Duration("timeout", 10*time.Second, "give up retrying a request after this long")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl (-addr host:port,... | -ctrl host:port,...) [-timeout 10s] put|get|del|cas|bench ...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	addrs := parseAddrs(*addrFlag)
	ctrlAddrs := parseAddrs(*ctrlFlag)
	if len(addrs) == 0 && len(ctrlAddrs) == 0 {
		fatal(errors.New("exactly one of -addr or -ctrl is required"))
	}
	if len(addrs) > 0 && len(ctrlAddrs) > 0 {
		fatal(errors.New("-addr and -ctrl are mutually exclusive; give exactly one"))
	}

	var genErr error
	clientID, genErr = randomHexID()
	if genErr != nil {
		fatal(genErr)
	}

	pool := newConnPool()
	defer pool.closeAll()

	var runner kvRunner
	if len(addrs) > 0 {
		runner = flatRunner{cl: newCluster(addrs, pool, newKVClientFromConn)}
	} else {
		runner = shardedRunner{sc: newShardedClient(ctrlAddrs, pool)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var err error
	cmd, a := flag.Arg(0), flag.Args()[1:]
	switch {
	case cmd == "put" && len(a) == 2:
		key := a[0]
		req := &kvv1.PutRequest{Meta: newMeta(), Key: key, Value: []byte(a[1])} // one meta for all attempts
		err = runner.run(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
			_, err := kv.Put(ctx, req)
			return err
		})
		if err == nil {
			fmt.Println("ok")
		}
	case cmd == "get" && len(a) == 1:
		key := a[0]
		req := &kvv1.GetRequest{Key: key}
		var r *kvv1.GetResponse
		err = runner.run(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
			var err error
			r, err = kv.Get(ctx, req)
			return err
		})
		if err == nil {
			fmt.Println(pick(r.GetFound(), string(r.GetValue()), "(not found)"))
		}
	case cmd == "del" && len(a) == 1:
		key := a[0]
		req := &kvv1.DeleteRequest{Meta: newMeta(), Key: key}
		var r *kvv1.DeleteResponse
		err = runner.run(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
			var err error
			r, err = kv.Delete(ctx, req)
			return err
		})
		if err == nil {
			fmt.Println(pick(r.GetExisted(), "deleted", "(not found)"))
		}
	case cmd == "cas" && len(a) == 3:
		key := a[0]
		req := &kvv1.CASRequest{Meta: newMeta(), Key: key, Value: []byte(a[2])}
		if a[1] == "-" {
			req.ExpectAbsent = true
		} else {
			req.Expected = []byte(a[1])
		}
		var r *kvv1.CASResponse
		err = runner.run(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
			var err error
			r, err = kv.CompareAndSwap(ctx, req)
			return err
		})
		if err == nil {
			fmt.Println(pick(r.GetSwapped(), "swapped", "not swapped, current="+string(r.GetCurrent())))
		}
	case cmd == "bench":
		err = bench(ctx, runner, a)
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
// mid-run). Each put goes through runner.run, so a leader change, or a
// shard reassignment, shows up as retries, not errors. Latency is measured
// per logical put, retries included.
//
// Keys are named bench-<i>; shard.Key2Shard hashes them, so with more than
// a handful of keys they naturally spread across multiple shards (and, in
// sharded mode, multiple groups) without any special-casing here.
func bench(ctx context.Context, runner kvRunner, args []string) error {
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
			// Each worker gets its OWN client identity, not the shared
			// process-wide one newMeta() hands out. The store's dedup table
			// assumes at most one outstanding request per client at a time
			// (see kv/store's package doc); this worker issues its puts one
			// at a time, sequentially, so a private counter satisfies that
			// exactly, whereas *c* workers sharing one identity would not:
			// they run concurrently, so two of their puts can commit out of
			// order relative to the shared counter, and the one that lands
			// second with a now-lower id is correctly refused as stale
			// (store.ErrStaleRequest) rather than silently misapplied —
			// which is safe, but would abort the whole benchmark run on
			// what is actually normal concurrent traffic, not a fault.
			var workerReqID uint64
			workerClientID, err := randomHexID()
			if err != nil {
				firstErr.CompareAndSwap(nil, &err)
				return
			}
			for {
				i := int(next.Add(1)) - 1
				if i >= *n || firstErr.Load() != nil {
					return
				}
				// Built once per logical put; every retry reuses this meta.
				workerReqID++
				key := fmt.Sprintf("bench-%d", i)
				meta := &kvv1.RequestMeta{ClientId: workerClientID, RequestId: workerReqID}
				req := &kvv1.PutRequest{Meta: meta, Key: key, Value: []byte("v")}
				t0 := time.Now()
				err := runner.run(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
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
	fmt.Printf("retries: %d\n", totalRetries.Load())
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
