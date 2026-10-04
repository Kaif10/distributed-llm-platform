// Package grpctransport carries the cross-group shard-migration RPC over
// gRPC. It is distinct from, and does not import, package
// dsys/raft/grpctransport: that one carries THIS group's own Raft peer
// traffic (RequestVote/AppendEntries/InstallSnapshot); this one carries the
// ShardMigration service (proto/shardkv/v1) that lets one replica group
// pull a shard's data OUT of another, unrelated, replica group. The two
// never share a client or a server registration; cmd/shardkv puts both on
// its PEER listener (never the client one), because both are
// unauthenticated replica-to-replica traffic.
//
//	// outbound: a fetcher tried against a source group's replica addresses
//	// (WithPeerAddrs maps the client addresses the controller config
//	// records to the peer addresses the migration service listens on)
//	fetcher := grpctransport.NewFetcher(grpctransport.WithPeerAddrs(m))
//	snap, ok, err := fetcher.PullShard(ctx, sourceAddrs, configNum, shardID)
//
//	// inbound: expose this replica's HandlePullShard on a grpc.Server
//	gs := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
//	grpctransport.Register(gs, srv) // srv has HandlePullShard(int64, int) ([]byte, bool)
package grpctransport

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	shardkvv1 "dsys/gen/shardkv/v1"
)

// MaxMessageSize is the largest message this transport will send or accept
// (64 MiB), matching dsys/raft/grpctransport since a shard snapshot can be
// just as large as a Raft snapshot. The client side is configured
// automatically by NewFetcher; the SERVER side is not under this package's
// control, so the grpc.Server passed to Register must be created with
//
//	grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
const MaxMessageSize = 64 << 20

// DefaultTimeout is the per-address deadline used when WithTimeout is not
// given. It is deliberately short: PullShard tries every address in order,
// and a slow or down replica should not stall the whole attempt for long,
// since the poll loop that calls this retries on its own schedule anyway.
const DefaultTimeout = 300 * time.Millisecond

// Option configures a Fetcher.
type Option func(*Fetcher)

// WithTimeout sets the per-address deadline. Non-positive values are ignored.
func WithTimeout(d time.Duration) Option {
	return func(f *Fetcher) {
		if d > 0 {
			f.timeout = d
		}
	}
}

// WithDialOptions appends extra grpc.DialOptions (for example credentials
// other than insecure, or a custom resolver). They are applied after the
// package defaults and so may override them.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(f *Fetcher) { f.dialOpts = append(f.dialOpts, opts...) }
}

// WithPeerAddrs makes PullShard translate each address it is given through
// m before dialing. The addresses shardkv hands PullShard come from the
// controller config, i.e. whatever was registered with Join: the groups'
// CLIENT addresses, which is what clients need to find a key's group. The
// ShardMigration service is deliberately not served there (it would let
// any client dump a shard), only on each replica's peer address, so the
// fetcher needs this client->peer map. An address missing from m is
// skipped (and logged once): dialing the client port could never succeed.
func WithPeerAddrs(m map[string]string) Option {
	return func(f *Fetcher) {
		f.peerAddrs = make(map[string]string, len(m))
		for k, v := range m {
			f.peerAddrs[k] = v
		}
	}
}

// Fetcher implements shardkv.ShardFetcher over gRPC. It lazily dials and
// caches one connection per address it has ever been asked to try, shared
// across calls and safe for concurrent use.
type Fetcher struct {
	timeout   time.Duration
	dialOpts  []grpc.DialOption
	peerAddrs map[string]string // nil = dial as given; read-only after NewFetcher
	unmapped  sync.Map          // client addresses already warned about

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]shardkvv1.ShardMigrationClient
}

// NewFetcher returns a ready-to-use Fetcher. No connection is made until
// PullShard is first asked to try a given address.
func NewFetcher(opts ...Option) *Fetcher {
	f := &Fetcher{
		timeout: DefaultTimeout,
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(MaxMessageSize),
				grpc.MaxCallSendMsgSize(MaxMessageSize),
			),
		},
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]shardkvv1.ShardMigrationClient),
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

// getClient returns the ShardMigrationClient for addr, dialing (without
// blocking; grpc.NewClient performs no I/O) on first use.
func (f *Fetcher) getClient(addr string) (shardkvv1.ShardMigrationClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, f.dialOpts...)
	if err != nil {
		return nil, err
	}
	f.conns[addr] = conn
	c := shardkvv1.NewShardMigrationClient(conn)
	f.clients[addr] = c
	return c, nil
}

// PullShard implements shardkv.ShardFetcher. It tries each address in turn
// (they are all replicas of the same source group; only its current leader
// can have the frozen snapshot) with a per-address deadline of f.timeout,
// stopping at the first that answers have_it=true.
//
// Every address failing to dial, timing out, erroring, or answering
// have_it=false is the ordinary "not ready yet" case: PullShard returns
// ok=false, err=nil so the caller's poll loop retries later, exactly as
// documented on shardkv.ShardFetcher. A non-nil error is reserved for
// ctx itself having already been cancelled/expired on entry.
func (f *Fetcher) PullShard(ctx context.Context, addrs []string, configNum int64, shardID int) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	req := &shardkvv1.PullShardRequest{ConfigNum: configNum, Shard: int32(shardID)}
	for _, addr := range addrs {
		if f.peerAddrs != nil {
			peer, ok := f.peerAddrs[addr]
			if !ok {
				if _, warned := f.unmapped.LoadOrStore(addr, true); !warned {
					slog.Warn("shard pull: no peer address for source replica; add it to -peer-map", "client_addr", addr)
				}
				continue
			}
			addr = peer
		}
		c, err := f.getClient(addr)
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, f.timeout)
		resp, err := c.PullShard(cctx, req)
		cancel()
		if err != nil {
			continue
		}
		if resp.GetHaveIt() {
			return resp.GetMachineSnapshot(), true, nil
		}
	}
	return nil, false, nil
}

// Close releases every cached connection. Not called by shardkv itself;
// provided for tests and orderly shutdown by whoever owns the Fetcher.
func (f *Fetcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var firstErr error
	for _, cc := range f.conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	f.conns = make(map[string]*grpc.ClientConn)
	f.clients = make(map[string]shardkvv1.ShardMigrationClient)
	return firstErr
}

// PullShardHandler is what Register needs from a shardkv.Server: the ability
// to answer "do you have shard S's frozen data as of config N". Satisfied by
// (*shardkv.Server).HandlePullShard without shardkv.Server needing to import
// this package.
type PullShardHandler interface {
	HandlePullShard(configNum int64, shardID int) (snapshot []byte, ok bool)
}

// Register installs h as the ShardMigration service on gs. The server must
// be created with grpc.MaxRecvMsgSize(MaxMessageSize) to send large
// snapshots back out; see MaxMessageSize.
func Register(gs *grpc.Server, h PullShardHandler) {
	shardkvv1.RegisterShardMigrationServer(gs, &server{h: h})
}

// server adapts a PullShardHandler to the generated service interface.
type server struct {
	shardkvv1.UnimplementedShardMigrationServer
	h PullShardHandler
}

func (s *server) PullShard(_ context.Context, req *shardkvv1.PullShardRequest) (*shardkvv1.PullShardResponse, error) {
	snap, ok := s.h.HandlePullShard(req.GetConfigNum(), int(req.GetShard()))
	return &shardkvv1.PullShardResponse{HaveIt: ok, MachineSnapshot: snap}, nil
}
