// Package grpcserver exposes a sched.Queue as the schedv1.Scheduler gRPC
// service. It is a pure translation layer: request fields in, Queue call,
// response fields out, and sched's error values mapped onto status codes a
// client can act on:
//
//	ErrQueueFull        -> ResourceExhausted "queue full retry_after_ms=<n>"
//	ErrNoJob            -> ClaimResponse{Found: false}, NOT an error
//	ErrFenced           -> FailedPrecondition (stop working; discard results)
//	ErrTerminal         -> FailedPrecondition (job already DONE/FAILED)
//	ErrNotFound         -> NotFound
//	context errors      -> DeadlineExceeded
//	anything else       -> Unavailable (a KV hiccup; retry)
//
// FailedPrecondition is deliberately the code for BOTH fenced and terminal:
// from the worker's point of view they mean the same thing, "the state of
// the job no longer permits what you asked; do not retry as-is".
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	schedv1 "dsys/gen/sched/v1"
	"dsys/sched"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryAfter is the back-off hint returned with ResourceExhausted. Fixed for
// now; a smarter server would derive it from drain rate.
const RetryAfter = 200 * time.Millisecond

// Server implements schedv1.SchedulerServer over a Queue.
type Server struct {
	schedv1.UnimplementedSchedulerServer
	q *sched.Queue
}

// New returns a SchedulerServer serving q.
func New(q *sched.Queue) schedv1.SchedulerServer { return &Server{q: q} }

// toStatus maps a Queue error onto a gRPC status. nil passes through.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, sched.ErrQueueFull):
		return status.Errorf(codes.ResourceExhausted, "queue full retry_after_ms=%d", RetryAfter.Milliseconds())
	case errors.Is(err, sched.ErrFenced), errors.Is(err, sched.ErrTerminal):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, sched.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		// Anything else is the KV misbehaving underneath us (leader
		// election in progress, a shard mid-migration, a timeout). The
		// operation may or may not have been applied, but every Queue
		// mutation is a CAS, so re-issuing it is safe: retry.
		return status.Error(codes.Unavailable, fmt.Sprintf("sched: kv: %v", err))
	}
}

func (s *Server) Submit(ctx context.Context, req *schedv1.SubmitRequest) (*schedv1.SubmitResponse, error) {
	id, err := s.q.Submit(ctx, req.GetPayload(), req.GetIdempotencyKey())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedv1.SubmitResponse{Id: id}, nil
}

func (s *Server) Claim(ctx context.Context, req *schedv1.ClaimRequest) (*schedv1.ClaimResponse, error) {
	job, err := s.q.Claim(ctx, req.GetWorker(), time.Duration(req.GetLeaseMs())*time.Millisecond)
	if errors.Is(err, sched.ErrNoJob) {
		// Not an error: an empty queue is the normal idle state. The
		// worker polls again after a backoff.
		return &schedv1.ClaimResponse{Found: false}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedv1.ClaimResponse{Found: true, Job: job}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *schedv1.HeartbeatRequest) (*schedv1.HeartbeatResponse, error) {
	until, err := s.q.Heartbeat(ctx, req.GetId(), req.GetGen(), time.Duration(req.GetLeaseMs())*time.Millisecond)
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedv1.HeartbeatResponse{LeaseUntilMs: until.UnixMilli()}, nil
}

func (s *Server) Complete(ctx context.Context, req *schedv1.CompleteRequest) (*schedv1.CompleteResponse, error) {
	if err := s.q.Complete(ctx, req.GetId(), req.GetGen(), req.GetResult()); err != nil {
		return nil, toStatus(err)
	}
	return &schedv1.CompleteResponse{}, nil
}

func (s *Server) Fail(ctx context.Context, req *schedv1.FailRequest) (*schedv1.FailResponse, error) {
	requeued, err := s.q.Fail(ctx, req.GetId(), req.GetGen(), req.GetError())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedv1.FailResponse{Requeued: requeued}, nil
}

func (s *Server) Status(ctx context.Context, req *schedv1.StatusRequest) (*schedv1.StatusResponse, error) {
	job, err := s.q.Status(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedv1.StatusResponse{Job: job}, nil
}

func (s *Server) Stats(ctx context.Context, _ *schedv1.StatsRequest) (*schedv1.StatsResponse, error) {
	head, tail, err := s.q.Stats(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedv1.StatsResponse{Head: head, Tail: tail, MaxQueue: s.q.Options().MaxQueue}, nil
}
