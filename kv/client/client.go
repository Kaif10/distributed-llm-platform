// Package client is the Go client for the KV gRPC service, in both of its
// deployments:
//
//   - NewFlat: one group holding the whole keyspace (a single kvserver, or a
//     raftkv cluster as in Phase 2). The Client talks to one server at a
//     time and, when that server is unreachable, slow, or answers "not
//     leader", moves on to the next one (or straight to the leader if the
//     follower named it) and retries the SAME request.
//   - NewSharded: several Raft groups, each owning a subset of the keyspace
//     (Phase 3, package shardkv). The Client asks the shard controller which
//     group currently owns each key's shard, caches that answer briefly, and
//     then talks to that group's replicas using exactly the same
//     retry/leader-hint machinery as flat mode. If a group answers "wrong
//     group for this key's shard" (the shard moved since we last asked), the
//     Client invalidates its cached config, re-queries, and retries against
//     whichever group owns it now.
//
// # Identity and the one-outstanding-request rule
//
// Every mutation carries a RequestMeta: this Client's random client id plus
// a per-Client request counter. The server deduplicates on that pair, which
// is what makes retrying a mutation safe (see kv/store's package doc). The
// store's dedup table assumes request ids arrive IN ORDER per client
// identity: at most one outstanding mutation per identity at a time. Two
// goroutines sharing one Client can therefore have their mutations commit
// out of counter order, and the one that lands second with a lower id is
// (correctly) refused as stale rather than silently misapplied.
//
// So the rule is: a Client may have at most one outstanding mutation at a
// time. Concurrent callers must each own their own identity; call Session()
// to get one that shares this Client's connections, config, and retry
// counter but numbers its own requests. Gets carry no meta and may be
// issued concurrently from any Client.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	kvv1 "dsys/gen/kv/v1"
)

// config is the per-family tunables; see the With* options.
type config struct {
	timeout        time.Duration
	attemptTimeout time.Duration
	configCacheTTL time.Duration
	logf           func(format string, args ...any)
}

// Option configures a Client (and every Session derived from it).
type Option func(*config)

// WithTimeout bounds each logical operation (all of its retries together).
// 0, the default, means the caller's context is the only bound.
func WithTimeout(d time.Duration) Option { return func(c *config) { c.timeout = d } }

// WithAttemptTimeout bounds a single RPC attempt so a hung server surfaces
// as DeadlineExceeded and the Client moves on to another replica. Default
// 2s.
func WithAttemptTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.attemptTimeout = d
		}
	}
}

// WithConfigCacheTTL sets how long a sharded Client reuses the controller's
// config before asking again (a wrong-group answer always invalidates it
// immediately regardless). Default 200ms. Ignored by flat Clients.
func WithConfigCacheTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.configCacheTTL = d
		}
	}
}

// WithLogger receives one line per retry, leader jump, and shard
// re-resolution. Default: discard.
func WithLogger(logf func(format string, args ...any)) Option {
	return func(c *config) {
		if logf != nil {
			c.logf = logf
		}
	}
}

// shared is everything a Client and its Sessions have in common: the
// connections, the flat-or-sharded dispatch, the tunables, and the retry
// counter. Only the identity lives on the Client itself.
type shared struct {
	cfg    config
	pool   *connPool
	runner runner
	// retries counts every retried attempt across every cluster this
	// family talks to: a single counter rather than a per-cluster field,
	// since sharded mode holds one cluster per group (plus the controller's)
	// and retries must still accumulate across all of them, including
	// retries caused by config invalidation.
	retries atomic.Uint64
}

// Client is a KV client with one identity. See the package doc for the
// one-outstanding-mutation rule; use Session for concurrent callers.
//
// Client's internals are goroutine-safe; the rule is about the store's
// dedup protocol, not about data races.
type Client struct {
	sh        *shared
	clientID  string
	nextReqID atomic.Uint64
	// root is true for the Client NewFlat/NewSharded returned. Only it
	// closes the shared connections; Close on a Session is a no-op.
	root bool
}

func newShared(opts []Option) *shared {
	sh := &shared{
		cfg: config{
			attemptTimeout: defaultAttemptTimeout,
			configCacheTTL: defaultConfigCacheTTL,
			logf:           func(string, ...any) {},
		},
		pool: newConnPool(),
	}
	for _, o := range opts {
		o(&sh.cfg)
	}
	return sh
}

// NewFlat returns a Client for a single group: addrs are that group's
// replicas (one address is fine for a standalone kvserver). Nothing is
// dialed until the first operation.
func NewFlat(addrs []string, opts ...Option) *Client {
	if len(addrs) == 0 {
		panic("kv/client: NewFlat needs at least one address")
	}
	sh := newShared(opts)
	sh.runner = flatRunner{cl: newCluster(addrs, sh, newKVClientFromConn)}
	return &Client{sh: sh, clientID: randomHexID(), root: true}
}

// NewSharded returns a Client for a sharded cluster: ctrlAddrs are the
// shard controller's replicas. Owning groups are resolved per key.
func NewSharded(ctrlAddrs []string, opts ...Option) *Client {
	if len(ctrlAddrs) == 0 {
		panic("kv/client: NewSharded needs at least one controller address")
	}
	sh := newShared(opts)
	sh.runner = newShardedClient(ctrlAddrs, sh)
	return &Client{sh: sh, clientID: randomHexID(), root: true}
}

// Session returns a Client with a fresh identity that shares this Client's
// connections, configuration, and retry counter. Each goroutine that issues
// mutations concurrently must use its own Session (see the package doc).
// Sessions are cheap: no dialing, just a random id.
func (c *Client) Session() *Client {
	return &Client{sh: c.sh, clientID: randomHexID()}
}

// ClientID is this Client's identity as it appears in RequestMeta.
func (c *Client) ClientID() string { return c.clientID }

// Retries reports how many attempts were retried, cumulatively, across
// this Client and every Session sharing its connections (including
// sharded-mode retries caused by a shard having moved).
func (c *Client) Retries() uint64 { return c.sh.retries.Load() }

// Close releases every connection this Client and its Sessions share. Call
// it once, on the Client NewFlat/NewSharded returned; on a Session it is a
// no-op.
func (c *Client) Close() error {
	if !c.root {
		return nil
	}
	return c.sh.pool.closeAll()
}

// newMeta issues the next RequestMeta for this identity. Callers build the
// request ONCE per logical mutation and reuse it across every retry; see
// doOn.
func (c *Client) newMeta() *kvv1.RequestMeta {
	return &kvv1.RequestMeta{ClientId: c.clientID, RequestId: c.nextReqID.Add(1)}
}

// randomHexID returns a fresh random client identity. crypto/rand.Read
// cannot fail on supported platforms (Go 1.24+), so there is no error path.
func randomHexID() string {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		panic("kv/client: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(raw)
}

// do applies the per-operation timeout, if any, and dispatches through the
// flat or sharded runner.
func (c *Client) do(ctx context.Context, key string, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	if c.sh.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.sh.cfg.timeout)
		defer cancel()
	}
	return c.sh.runner.run(ctx, key, fn)
}

// Put writes value at key.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	req := &kvv1.PutRequest{Meta: c.newMeta(), Key: key, Value: value} // one meta for all attempts
	return c.do(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
		_, err := kv.Put(ctx, req)
		return err
	})
}

// Get reads key. found is false when the key is absent (err is nil then).
func (c *Client) Get(ctx context.Context, key string) (value []byte, found bool, err error) {
	req := &kvv1.GetRequest{Key: key}
	var r *kvv1.GetResponse
	err = c.do(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
		var err error
		r, err = kv.Get(ctx, req)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return r.GetValue(), r.GetFound(), nil
}

// Delete removes key, reporting whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (existed bool, err error) {
	req := &kvv1.DeleteRequest{Meta: c.newMeta(), Key: key} // one meta for all attempts
	var r *kvv1.DeleteResponse
	err = c.do(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
		var err error
		r, err = kv.Delete(ctx, req)
		return err
	})
	if err != nil {
		return false, err
	}
	return r.GetExisted(), nil
}

// CAS writes value only if key's current value equals expected (or, with
// expectAbsent, only if key is absent). swapped=false with err=nil is a
// normal outcome: someone else got there first, and current is what the
// key holds now.
func (c *Client) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (swapped bool, current []byte, err error) {
	req := &kvv1.CASRequest{Meta: c.newMeta(), Key: key, Expected: expected, ExpectAbsent: expectAbsent, Value: value} // one meta for all attempts
	var r *kvv1.CASResponse
	err = c.do(ctx, key, func(ctx context.Context, kv kvv1.KVClient) error {
		var err error
		r, err = kv.CompareAndSwap(ctx, req)
		return err
	})
	if err != nil {
		return false, nil, err
	}
	return r.GetSwapped(), r.GetCurrent(), nil
}
