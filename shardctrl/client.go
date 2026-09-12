package shardctrl

// Client is a Go client for the shard controller, usable in-process (tests,
// a shardkv group's config poller) without going through cmd/shardctrl at
// all — it dials the addrses itself and speaks real gRPC, unlike Server's
// internals which never leave one process. It follows the same
// retry/leader-hint convention cmd/kvctl's cluster[C] uses for the KV API:
// try the server we last found good, follow a "leader=<addr>" hint on
// NotLeader, otherwise round-robin with backoff, and always retry a
// mutating request with the SAME RequestMeta so the server's dedup table
// recognises a retry instead of applying it twice.
import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	shardctrlv1 "dsys/gen/shardctrl/v1"
)

const (
	backoffMin = 10 * time.Millisecond
	backoffMax = 200 * time.Millisecond
)

// leaderHint matches the "leader=<addr>" fragment Server.toStatus includes
// in a NotLeader status message.
var leaderHint = regexp.MustCompile(`leader=([^\s,;)\]]+)`)

// Client talks to a shard controller replica group. Safe for concurrent
// use.
type Client struct {
	mu    sync.Mutex
	addrs []string
	conns map[string]*grpc.ClientConn
	cur   int // index into addrs most recently found to work

	clientID  string
	nextReqID atomic.Uint64
}

// NewClient returns a Client that will dial addrs lazily, one connection
// per address, on first use.
func NewClient(addrs []string) *Client {
	return &Client{
		addrs:    append([]string(nil), addrs...),
		conns:    make(map[string]*grpc.ClientConn),
		clientID: randomID(),
	}
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (c *Client) meta() *shardctrlv1.RequestMeta {
	return &shardctrlv1.RequestMeta{ClientId: c.clientID, RequestId: c.nextReqID.Add(1)}
}

// Close releases every connection this Client has opened.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, cc := range c.conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}

// ---------------------------------------------------------------------------
// Connection management and the retry/leader-hint loop. Structurally the
// same as cmd/kvctl's connPool + cluster[C] + doOn, specialised to one
// client type instead of being generic over it, since Client only ever
// talks to ShardCtrl.
// ---------------------------------------------------------------------------

func (c *Client) getConn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

// pick returns the preferred server's index, address, and stub.
func (c *Client) pick() (int, string, shardctrlv1.ShardCtrlClient, error) {
	c.mu.Lock()
	i := c.cur
	addr := c.addrs[i]
	c.mu.Unlock()
	cc, err := c.getConn(addr)
	if err != nil {
		return i, addr, nil, err
	}
	return i, addr, shardctrlv1.NewShardCtrlClient(cc), nil
}

// advance moves the cursor after a failure at index failed, jumping
// straight to a hinted leader if one was given; see cluster[C].advance in
// cmd/kvctl for the same logic and why it only moves the cursor if it is
// still pointing at the failed server.
func (c *Client) advance(failed int, hint string) {
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

func (c *Client) markGood(i int) {
	c.mu.Lock()
	if c.cur != i && i < len(c.addrs) {
		c.cur = i
	}
	c.mu.Unlock()
}

// retryable reports whether err means "try another replica" and, if the
// replica named the leader, that address.
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

// do runs fn against whichever replica this Client currently prefers,
// retrying on transient failures (following leader hints) until ctx
// expires. fn must be built from state fixed before the first attempt (in
// particular, any RequestMeta), so every attempt is the same logical
// request and the server's dedup table recognises a retry as such.
func (c *Client) do(ctx context.Context, fn func(ctx context.Context, sc shardctrlv1.ShardCtrlClient) error) error {
	backoff := backoffMin
	var lastErr error
	for {
		i, addr, sc, err := c.pick()
		if err == nil {
			err = fn(ctx, sc)
			if err == nil {
				c.markGood(i)
				return nil
			}
		}
		ok, hint := retryable(err)
		if !ok {
			return err
		}
		lastErr = fmt.Errorf("shardctrl client: %s: %w", addr, err)
		if ctx.Err() != nil {
			return lastErr
		}
		c.advance(i, hint)
		if hint != "" {
			continue // we know where to go; no need to wait
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return lastErr
		}
		if backoff < backoffMax {
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
		}
	}
}

// ---------------------------------------------------------------------------
// The four RPCs.
// ---------------------------------------------------------------------------

// Join adds (or re-adds, with new addresses) the given groups and triggers
// a rebalance. groups maps group id to that group's replica addresses.
func (c *Client) Join(ctx context.Context, groups map[int64][]string) error {
	meta := c.meta()
	protoGroups := make(map[int64]*shardctrlv1.Group, len(groups))
	for gid, addrs := range groups {
		protoGroups[gid] = &shardctrlv1.Group{Addrs: append([]string(nil), addrs...)}
	}
	return c.do(ctx, func(ctx context.Context, sc shardctrlv1.ShardCtrlClient) error {
		_, err := sc.Join(ctx, &shardctrlv1.JoinRequest{Meta: meta, Groups: protoGroups})
		return err
	})
}

// Leave removes the given groups and triggers a rebalance of whatever they
// held.
func (c *Client) Leave(ctx context.Context, gids []int64) error {
	meta := c.meta()
	return c.do(ctx, func(ctx context.Context, sc shardctrlv1.ShardCtrlClient) error {
		_, err := sc.Leave(ctx, &shardctrlv1.LeaveRequest{Meta: meta, Gids: gids})
		return err
	})
}

// Move forces shard to be owned by gid, which must already be a known
// group.
func (c *Client) Move(ctx context.Context, shard int64, gid int64) error {
	meta := c.meta()
	return c.do(ctx, func(ctx context.Context, sc shardctrlv1.ShardCtrlClient) error {
		_, err := sc.Move(ctx, &shardctrlv1.MoveRequest{Meta: meta, Shard: shard, Gid: gid})
		return err
	})
}

// Query returns config number num, or the latest config if num < 0 or past
// the latest.
func (c *Client) Query(ctx context.Context, num int64) (*shardctrlv1.Config, error) {
	var resp *shardctrlv1.QueryResponse
	err := c.do(ctx, func(ctx context.Context, sc shardctrlv1.ShardCtrlClient) error {
		var err error
		resp, err = sc.Query(ctx, &shardctrlv1.QueryRequest{Num: num})
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.GetConfig(), nil
}
