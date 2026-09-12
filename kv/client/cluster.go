package client

import (
	"context"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Retry policy.
const (
	backoffMin = 10 * time.Millisecond
	backoffMax = 200 * time.Millisecond
	// defaultAttemptTimeout bounds a single RPC so a hung server surfaces
	// as DeadlineExceeded and we move on, instead of eating the whole
	// budget. Override with WithAttemptTimeout.
	defaultAttemptTimeout = 2 * time.Second
)

// leaderHint matches the "leader=<addr>" fragment a follower may include in
// its "not leader" status message (see raftkv.Server.toStatus and
// shardkv.Server.toStatus).
var leaderHint = regexp.MustCompile(`leader=([^\s,;)\]]+)`)

// ---------------------------------------------------------------------------
// connPool: one *grpc.ClientConn per address, shared by every cluster[C] a
// Client family creates. Sharded mode builds a new cluster[kvv1.KVClient]
// each time it first meets a group, but that is cheap because connections
// to addresses already seen are reused rather than redialed.
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

// closeAll closes every connection and returns the first close error.
func (p *connPool) closeAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var first error
	for addr, cc := range p.conns {
		if err := cc.Close(); err != nil && first == nil {
			first = err
		}
		delete(p.conns, addr)
	}
	return first
}

// ---------------------------------------------------------------------------
// cluster[C]: a set of server addresses plus a cursor pointing at the one
// that most recently worked. Generic over the client stub type so the same
// retry/leader-hint machinery serves both the KV API (kvv1.KVClient) and,
// in sharded mode, the ShardCtrl API (shardctrlv1.ShardCtrlClient) used to
// ask the controller for the current config.
// ---------------------------------------------------------------------------

type cluster[C any] struct {
	mu    sync.Mutex
	addrs []string
	// configured is the address list the cluster was built from. addrs may
	// grow beyond it when a leader hint names an address we did not know;
	// configured is what identifies the group when sharded mode asks
	// whether a cached cluster still matches the controller's view.
	configured []string
	pool       *connPool
	newClient  func(grpc.ClientConnInterface) C
	cur        int

	// retries is the family-wide counter every cluster shares (see
	// shared.retries): sharded mode owns several clusters at once and
	// retries must accumulate across all of them.
	retries *atomic.Uint64
	cfg     *config
}

func newCluster[C any](addrs []string, sh *shared, newClient func(grpc.ClientConnInterface) C) *cluster[C] {
	return &cluster[C]{
		addrs:      slices.Clone(addrs),
		configured: slices.Clone(addrs),
		pool:       sh.pool,
		newClient:  newClient,
		retries:    &sh.retries,
		cfg:        &sh.cfg,
	}
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

// builtFrom reports whether the cluster was configured with exactly addrs.
// configured never changes after construction, so no lock is needed.
func (c *cluster[C]) builtFrom(addrs []string) bool { return slices.Equal(c.configured, addrs) }

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

// AttemptError is what a Client returns when it gives up retrying a
// transient failure because the caller's context expired: which server the
// last attempt went to, plus the gRPC status it (or the transport) produced.
// Unwrap yields that status error, so status.Code(err) still works.
type AttemptError struct {
	Addr string
	Err  error
}

func (e *AttemptError) Error() string {
	return "gave up; last error from " + e.Addr + ": " + status.Convert(e.Err).Message()
}
func (e *AttemptError) Unwrap() error { return e.Err }

// doOn runs one logical request against cl, retrying on transient failures
// until ctx expires. fn is invoked with a fresh per-attempt context and
// whichever server cl currently prefers. This is the ONE retry/leader-hint
// loop the package has: flat mode calls it directly on its single cluster,
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
			cl.retries.Add(1)
		}
		i, addr, c, err := cl.pick()
		if err == nil {
			actx, cancel := context.WithTimeout(ctx, cl.cfg.attemptTimeout)
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
		lastErr = &AttemptError{Addr: addr, Err: err}
		if ctx.Err() != nil {
			return lastErr
		}
		cl.cfg.logf("kv/client: %s: %v (retrying)", addr, status.Convert(err).Message())
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
