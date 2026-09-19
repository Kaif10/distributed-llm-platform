// Package obs is a thin, dependency-isolating wrapper around OpenTelemetry
// tracing and Prometheus metrics, so the rest of the codebase (gateway,
// sched, their main.go entry points) does not have to hand-roll SDK
// boilerplate at every call site.
//
// # Tracing
//
// InitTracing wires a global TracerProvider that exports spans via
// OTLP/gRPC. Passing an empty endpoint disables export entirely (a
// no-op TracerProvider is installed) so instrumented call sites are safe
// to leave in place even when nobody is running a collector.
//
// # Span names (documented here for the dashboard/docs author)
//
// Gateway, one trace per Generate call:
//
//	gateway.Generate                 (root span; attrs: tenant, prompt_chars)
//	  gateway.ratelimit               (attrs: tenant, limited)
//	  gateway.cache_lookup            (attrs: cache_hit)
//	  gateway.route                   (attrs: worker, prefix_routing)
//	  gateway.attempt                 (attrs: worker, hedge, hedge_won) — one
//	                                   per attempt (primary + any hedge)
//
// The Go worker call (dsys/infer/mock) and the Python worker both use a
// gRPC client/server whose stats handler is obs.GRPCDialOption /
// obs.GRPCServerOption, so their own spans (infer.Generate on the Go side,
// worker.Generate on the Python side) nest under gateway.attempt
// automatically via W3C trace-context propagation over gRPC metadata.
//
// Scheduler, one trace per RPC:
//
//	sched.Submit
//	sched.Claim
//	sched.Heartbeat
//	sched.Complete
//	sched.Fail
//
// # Metrics
//
// See metrics.go for the full list of Prometheus metric names; they are
// deliberately independent of the span names above (metrics survive
// dashboard rewrites, spans survive trace-tool swaps; neither should have to
// agree on vocabulary with the other).
package obs

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InitTracing sets up the global OTel TracerProvider for serviceName,
// exporting spans via OTLP/gRPC to otlpEndpoint (e.g. "127.0.0.1:4317" or
// "otel-collector:4317").
//
// If otlpEndpoint is "", tracing is a genuine no-op: no exporter is created,
// no background goroutines run, and every span obs.Tracer() hands out is
// discarded immediately. This makes it safe to leave instrumentation in
// place in binaries/tests that never pass -otlp-endpoint.
//
// The returned shutdown func flushes and closes the exporter; call it
// during graceful shutdown, alongside the existing shutdown path in each
// main.go.
func InitTracing(ctx context.Context, serviceName, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if otlpEndpoint == "" {
		tp := sdktrace.NewTracerProvider() // no span processors: every span is dropped
		otel.SetTracerProvider(tp)
		return func(context.Context) error { return tp.Shutdown(context.Background()) }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, fmt.Errorf("obs: otlptracegrpc exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceNameKey.String(serviceName),
	))
	if err != nil {
		return nil, fmt.Errorf("obs: resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer is a convenience for otel.Tracer(name); it always returns a usable
// Tracer, backed by whatever provider InitTracing installed (or the global
// no-op default if InitTracing was never called).
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }
