// Package grpctransport carries Raft RPCs over gRPC.
//
// It is the glue between the transport-agnostic interfaces in package raft
// (Peer for outbound calls, Handler for inbound ones) and the generated
// raftv1 service:
//
//	// outbound: one Peer per remote node
//	peers := []raft.Peer{grpctransport.NewPeer("10.0.0.2:8001"), ...}
//
//	// inbound: expose this node's *raft.Raft on a grpc.Server
//	gs := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
//	grpctransport.Register(gs, rf)
//
// A Peer never returns an error: Raft's contract is (reply, ok) where
// ok=false means "treat as a lost message". Any failure (dial, deadline,
// server error, oversized message) is therefore reported as ok=false and
// logged at debug level only. Raft retries on its own schedule.
package grpctransport

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftv1 "dsys/gen/raft/v1"
	"dsys/raft"
)

// MaxMessageSize is the largest message this transport will send or accept
// (64 MiB). Snapshots are sent whole, so it bounds snapshot size. The client
// side is configured automatically by NewPeer; the SERVER side is not under
// this package's control, so the grpc.Server passed to Register must be
// created with
//
//	grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
//
// or InstallSnapshot calls larger than gRPC's 4 MiB default will be rejected.
const MaxMessageSize = 64 << 20

// DefaultTimeout is the per-call deadline used when WithTimeout is not given.
const DefaultTimeout = 500 * time.Millisecond

// snapshotTimeoutFactor scales the per-call timeout for InstallSnapshot,
// which may move tens of megabytes.
const snapshotTimeoutFactor = 10

// Option configures a Peer.
type Option func(*peer)

// WithTimeout sets the per-call deadline for RequestVote and AppendEntries.
// InstallSnapshot uses ten times this value. Non-positive values are ignored.
func WithTimeout(d time.Duration) Option {
	return func(p *peer) {
		if d > 0 {
			p.timeout = d
		}
	}
}

// WithLogger sets the logger used for debug output. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(p *peer) {
		if l != nil {
			p.log = l
		}
	}
}

// WithDialOptions appends extra grpc.DialOptions (for example credentials
// other than insecure, or a custom resolver). They are applied after the
// package defaults and so may override them.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(p *peer) { p.dialOpts = append(p.dialOpts, opts...) }
}

// peer implements raft.Peer over a lazily created *grpc.ClientConn.
type peer struct {
	addr     string
	timeout  time.Duration
	log      *slog.Logger
	dialOpts []grpc.DialOption

	mu     sync.Mutex // guards the fields below
	conn   *grpc.ClientConn
	client raftv1.RaftClient
}

// NewPeer returns a raft.Peer that talks to the Raft node at addr. No
// connection is made until the first call; gRPC then keeps reconnecting in
// the background, so a peer created for a node that is currently down simply
// returns ok=false until the node comes up.
//
// The returned Peer is safe for concurrent use.
func NewPeer(addr string, opts ...Option) raft.Peer {
	p := &peer{
		addr:    addr,
		timeout: DefaultTimeout,
		log:     slog.Default(),
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(MaxMessageSize),
				grpc.MaxCallSendMsgSize(MaxMessageSize),
			),
		},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// getClient returns the RaftClient, creating the underlying connection on
// first use. grpc.NewClient does not block or perform I/O, so holding the
// mutex across it is fine.
func (p *peer) getClient() (raftv1.RaftClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	conn, err := grpc.NewClient(p.addr, p.dialOpts...)
	if err != nil {
		return nil, err
	}
	p.conn = conn
	p.client = raftv1.NewRaftClient(conn)
	return p.client, nil
}

// Close releases the underlying connection. Raft never calls this; it exists
// for tests and for orderly shutdown by the process that owns the peers.
func (p *peer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil {
		return nil
	}
	err := p.conn.Close()
	p.conn, p.client = nil, nil
	return err
}

// call runs one unary RPC with a fresh deadline and maps any error to ok=false.
func call[Req, Resp any](p *peer, method string, timeout time.Duration, req Req,
	do func(ctx context.Context, c raftv1.RaftClient, req Req) (Resp, error)) (Resp, bool) {
	var zero Resp
	c, err := p.getClient()
	if err != nil {
		p.log.Debug("raft rpc: dial failed", "peer", p.addr, "method", method, "err", err)
		return zero, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := do(ctx, c, req)
	if err != nil {
		p.log.Debug("raft rpc failed", "peer", p.addr, "method", method, "err", err)
		return zero, false
	}
	return resp, true
}

func (p *peer) RequestVote(args *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	resp, ok := call(p, "RequestVote", p.timeout, requestVoteArgsToProto(args),
		func(ctx context.Context, c raftv1.RaftClient, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
			return c.RequestVote(ctx, req)
		})
	if !ok {
		return nil, false
	}
	return requestVoteReplyFromProto(resp), true
}

func (p *peer) AppendEntries(args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, bool) {
	resp, ok := call(p, "AppendEntries", p.timeout, appendEntriesArgsToProto(args),
		func(ctx context.Context, c raftv1.RaftClient, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
			return c.AppendEntries(ctx, req)
		})
	if !ok {
		return nil, false
	}
	return appendEntriesReplyFromProto(resp), true
}

func (p *peer) InstallSnapshot(args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, bool) {
	resp, ok := call(p, "InstallSnapshot", p.timeout*snapshotTimeoutFactor, installSnapshotArgsToProto(args),
		func(ctx context.Context, c raftv1.RaftClient, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
			return c.InstallSnapshot(ctx, req)
		})
	if !ok {
		return nil, false
	}
	return installSnapshotReplyFromProto(resp), true
}

// ---------------------------------------------------------------------------
// Inbound: raftv1.RaftServer -> raft.Handler
// ---------------------------------------------------------------------------

// Register installs h as the Raft service on gs. The server must be created
// with grpc.MaxRecvMsgSize(MaxMessageSize) to accept large snapshots; see
// MaxMessageSize.
func Register(gs *grpc.Server, h raft.Handler) {
	raftv1.RegisterRaftServer(gs, &server{h: h})
}

// server adapts a raft.Handler to the generated service interface.
type server struct {
	raftv1.UnimplementedRaftServer
	h raft.Handler
}

func (s *server) RequestVote(_ context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	return requestVoteReplyToProto(s.h.HandleRequestVote(requestVoteArgsFromProto(req))), nil
}

func (s *server) AppendEntries(_ context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return appendEntriesReplyToProto(s.h.HandleAppendEntries(appendEntriesArgsFromProto(req))), nil
}

func (s *server) InstallSnapshot(_ context.Context, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
	return installSnapshotReplyToProto(s.h.HandleInstallSnapshot(installSnapshotArgsFromProto(req))), nil
}
