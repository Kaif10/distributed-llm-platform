package obs

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// GRPCServerOption returns a grpc.ServerOption that installs the OTel stats
// handler, so every inbound RPC gets a span (named "<service>/<method>",
// e.g. "gateway.v1.Gateway/Generate") that continues a trace whose context
// arrived via W3C traceparent metadata, or starts a new one.
//
// Usage: grpc.NewServer(obs.GRPCServerOption(), otherOpts...)
func GRPCServerOption() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler())
}

// GRPCDialOption returns a grpc.DialOption that installs the OTel stats
// handler on an outbound client connection, so calls it makes carry the
// caller's trace context in gRPC metadata and are wrapped in a client span.
//
// Usage: grpc.NewClient(addr, obs.GRPCDialOption(), otherOpts...)
func GRPCDialOption() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler())
}
