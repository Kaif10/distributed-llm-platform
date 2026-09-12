package grpctransport

// Controller adapter: lives here (shardkv/grpctransport/controller.go)
// rather than in cmd/shardkv/main.go, so it can be unit-tested alongside
// the rest of this package and reused by any future binary that needs a
// shardkv.Controller.
//
// It implements shardkv.Controller by talking DIRECTLY to the generated
// shardctrlv1.ShardCtrlClient (proto/shardctrl/v1), the same way Fetcher
// talks directly to shardkvv1.ShardMigrationClient. It deliberately does
// NOT depend on a higher-level dsys/shardctrl client package: as of
// writing, that package (owned by another engineer) has no client.go yet,
// and in any case QueryRequest carries no RequestMeta (see
// proto/shardctrl/v1/shardctrl.proto — only JoinRequest/LeaveRequest/
// MoveRequest do), so a query is a plain idempotent read with nothing to
// deduplicate. If dsys/shardctrl grows a richer client later, this adapter
// can be pointed at it instead; nothing else in this package needs to
// change since shardkv.Controller's contract stays the same.
import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/shard"
	"dsys/shardkv"
)

// controllerLeaderHint matches the same "leader=<addr>" fragment convention
// used by dsys/raft/grpctransport and cmd/kvctl: a shard controller that is
// not currently the leader answers with a status whose message may name the
// real leader, so the next attempt can go straight there.
var controllerLeaderHint = regexp.MustCompile(`leader=([^\s,;)\]]+)`)

// ControllerDefaultTimeout is the per-address deadline used when
// WithControllerTimeout is not given.
const ControllerDefaultTimeout = 500 * time.Millisecond

// ControllerOption configures a Controller.
type ControllerOption func(*Controller)

// WithControllerTimeout sets the per-address deadline. Non-positive values
// are ignored.
func WithControllerTimeout(d time.Duration) ControllerOption {
	return func(c *Controller) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithControllerDialOptions appends extra grpc.DialOptions.
func WithControllerDialOptions(opts ...grpc.DialOption) ControllerOption {
	return func(c *Controller) { c.dialOpts = append(c.dialOpts, opts...) }
}

// Controller implements shardkv.Controller over the ShardCtrl gRPC service.
// It remembers which address last answered (or was named as leader) and
// tries that one first on the next call, the same "sticky preferred
// server, follow the leader hint" idiom as cmd/kvctl's cluster and
// dsys/raft/grpctransport's Peer.
type Controller struct {
	timeout  time.Duration
	dialOpts []grpc.DialOption

	mu      sync.Mutex
	addrs   []string
	cur     int
	conns   map[string]*grpc.ClientConn
	clients map[string]shardctrlv1.ShardCtrlClient
}

// NewController returns a Controller that queries the shard controller
// cluster at addrs.
func NewController(addrs []string, opts ...ControllerOption) *Controller {
	c := &Controller{
		timeout: ControllerDefaultTimeout,
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		addrs:   append([]string(nil), addrs...),
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]shardctrlv1.ShardCtrlClient),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Controller) client(addr string) (shardctrlv1.ShardCtrlClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[addr]; ok {
		return cl, nil
	}
	conn, err := grpc.NewClient(addr, c.dialOpts...)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	cl := shardctrlv1.NewShardCtrlClient(conn)
	c.clients[addr] = cl
	return cl, nil
}

// addrsFromPreferred returns every known address, starting with the one
// currently preferred, wrapping around.
func (c *Controller) addrsFromPreferred() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.addrs))
	out = append(out, c.addrs[c.cur:]...)
	out = append(out, c.addrs[:c.cur]...)
	return out
}

// preferAddr makes addr the one tried first next time, adding it to the
// known set (e.g. a newly-named leader hint) if not already present.
func (c *Controller) preferAddr(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, a := range c.addrs {
		if a == addr {
			c.cur = i
			return
		}
	}
	c.addrs = append(c.addrs, addr)
	c.cur = len(c.addrs) - 1
}

func leaderHintFrom(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	if m := controllerLeaderHint.FindStringSubmatch(st.Message()); m != nil {
		return m[1]
	}
	return ""
}

// Query implements shardkv.Controller. It tries each known address once
// (preferred first), stopping at the first success; a failure naming the
// leader makes that address preferred for this attempt and future calls.
// If every address fails, the last error is returned (unlike Fetcher's
// PullShard, a failed Query is a real error: the poll loop that calls this
// simply tries again on its next tick, per shardkv.Server's design).
func (c *Controller) Query(ctx context.Context, num int64) (shardkv.Config, error) {
	if err := ctx.Err(); err != nil {
		return shardkv.Config{}, err
	}
	req := &shardctrlv1.QueryRequest{Num: num}

	var lastErr error
	for _, addr := range c.addrsFromPreferred() {
		cl, err := c.client(addr)
		if err != nil {
			lastErr = err
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		resp, err := cl.Query(cctx, req)
		cancel()
		if err != nil {
			lastErr = err
			if hint := leaderHintFrom(err); hint != "" {
				c.preferAddr(hint)
			}
			continue
		}
		c.preferAddr(addr)
		return configFromProto(resp.GetConfig()), nil
	}
	if lastErr == nil {
		lastErr = errors.New("grpctransport: no shard controller addresses configured")
	}
	return shardkv.Config{}, lastErr
}

// Close releases every cached connection.
func (c *Controller) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, cc := range c.conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)
	c.clients = make(map[string]shardctrlv1.ShardCtrlClient)
	return firstErr
}

// configFromProto converts the controller's wire-format Config into
// shardkv's plain-struct Config: the repeated int64 shards field copies
// into the fixed [shard.NShards]int64 array, and the map<int64,Group>
// becomes a map[int64][]string. See shardkv/api.go's Config doc: shardkv
// deliberately has no compile-time dependency on shardctrlv1 beyond this
// one conversion function.
func configFromProto(pc *shardctrlv1.Config) shardkv.Config {
	var cfg shardkv.Config
	if pc == nil {
		return cfg
	}
	cfg.Num = pc.GetNum()
	shards := pc.GetShards()
	for i := 0; i < shard.NShards && i < len(shards); i++ {
		cfg.Shards[i] = shards[i]
	}
	if groups := pc.GetGroups(); len(groups) > 0 {
		cfg.Groups = make(map[int64][]string, len(groups))
		for gid, g := range groups {
			cfg.Groups[gid] = append([]string(nil), g.GetAddrs()...)
		}
	}
	return cfg
}
