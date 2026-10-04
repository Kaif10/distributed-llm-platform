package node

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"dsys/raft"
)

// ProbeTimeout bounds the quorum probe a leader runs per health check.
const ProbeTimeout = 2 * time.Second

// Health is a grpc.health.v1 server that answers for THIS replica only,
// from its own Raft state, instead of "can the cluster serve a request"
// (which any majority answers yes to while this node is broken):
//
//   - killed: NOT_SERVING.
//   - leader: SERVING only if Probe succeeds, i.e. a no-side-effect read
//     goes through this node's log, commits on a majority and is applied
//     by this node's state machine. A leader cut off in a minority, or
//     with a wedged apply loop, fails it.
//   - follower/candidate: SERVING only if it currently knows a leader
//     other than itself. Raft clears that the moment the node starts an
//     election (which is what a partitioned or isolated follower keeps
//     doing), and sets it only on accepting a leader's AppendEntries or
//     InstallSnapshot.
//
// Known gap: a follower stuck in a minority WITH a deposed leader that has
// not yet noticed keeps hearing heartbeats and reports SERVING; the
// deposed leader itself reports NOT_SERVING.
//
// Only Check is implemented; Watch stays Unimplemented.
type Health struct {
	healthpb.UnimplementedHealthServer
	Raft  *raft.Raft
	Me    int
	Probe func(ctx context.Context) error // run only while leader
}

func (h *Health) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if req.GetService() != "" {
		return nil, status.Errorf(codes.NotFound, "unknown service %q (only the overall \"\" status is served)", req.GetService())
	}
	return &healthpb.HealthCheckResponse{Status: h.status(ctx)}, nil
}

func (h *Health) status(ctx context.Context) healthpb.HealthCheckResponse_ServingStatus {
	const (
		up   = healthpb.HealthCheckResponse_SERVING
		down = healthpb.HealthCheckResponse_NOT_SERVING
	)
	if h.Raft.Killed() {
		return down
	}
	if _, isLeader := h.Raft.GetState(); isLeader {
		ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		defer cancel()
		if h.Probe(ctx) != nil {
			return down
		}
		return up
	}
	if hint := h.Raft.LeaderHint(); hint >= 0 && hint != h.Me {
		return up
	}
	return down
}
