package grpcserver

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	schedv1 "dsys/gen/sched/v1"
	"dsys/sched"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// memKV is a linearizable in-memory sched.KV: a map under one mutex, so
// every CAS observes the latest write. It is the only piece of "store" this
// test needs; the point of the test is the gRPC translation, not the KV.
type memKV struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemKV() *memKV { return &memKV{m: make(map[string][]byte)} }

func (k *memKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[key]
	return bytes.Clone(v), ok, nil
}

func (k *memKV) Put(_ context.Context, key string, value []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[key] = bytes.Clone(value)
	return nil
}

func (k *memKV) CAS(_ context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.m[key]
	if expectAbsent {
		if ok {
			return false, bytes.Clone(cur), nil
		}
	} else if !ok || !bytes.Equal(cur, expected) {
		return false, bytes.Clone(cur), nil
	}
	k.m[key] = bytes.Clone(value)
	return true, nil, nil
}

// start serves a Queue with opts on a loopback port and returns a client.
func start(t *testing.T, opts sched.Options) schedv1.SchedulerClient {
	t.Helper()
	q := sched.New(newMemKV(), opts)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	schedv1.RegisterSchedulerServer(gs, New(q))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return schedv1.NewSchedulerClient(cc)
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// The happy path: one job goes Submit -> Claim -> Heartbeat -> Complete
// and Status then shows it DONE with the result recorded.
func TestSubmitClaimHeartbeatComplete(t *testing.T) {
	ctx := testCtx(t)
	c := start(t, sched.Options{Prefix: "t"})

	sub, err := c.Submit(ctx, &schedv1.SubmitRequest{Payload: []byte("work")})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	cl, err := c.Claim(ctx, &schedv1.ClaimRequest{Worker: "w1", LeaseMs: 5000})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if !cl.GetFound() || cl.GetJob().GetId() != sub.GetId() {
		t.Fatalf("Claim = %v, want job %d", cl, sub.GetId())
	}
	job := cl.GetJob()
	if job.GetState() != schedv1.State_STATE_RUNNING || job.GetWorker() != "w1" || string(job.GetPayload()) != "work" {
		t.Fatalf("claimed job = %v", job)
	}

	hb, err := c.Heartbeat(ctx, &schedv1.HeartbeatRequest{Id: job.GetId(), Gen: job.GetGen(), LeaseMs: 5000})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if hb.GetLeaseUntilMs() < job.GetLeaseUntilMs() {
		t.Fatalf("Heartbeat shortened the lease: %d < %d", hb.GetLeaseUntilMs(), job.GetLeaseUntilMs())
	}

	if _, err := c.Complete(ctx, &schedv1.CompleteRequest{Id: job.GetId(), Gen: job.GetGen(), Result: []byte("42")}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	st, err := c.Status(ctx, &schedv1.StatusRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetJob().GetState() != schedv1.State_STATE_DONE || string(st.GetJob().GetResult()) != "42" {
		t.Fatalf("after Complete, job = %v", st.GetJob())
	}

	// The queue is drained: Claim reports found=false, not an error.
	cl2, err := c.Claim(ctx, &schedv1.ClaimRequest{Worker: "w2"})
	if err != nil {
		t.Fatalf("Claim on empty queue: %v", err)
	}
	if cl2.GetFound() {
		t.Fatalf("Claim on empty queue found %v", cl2.GetJob())
	}

	// Completing a DONE job again with the SAME gen is an idempotent success
	// (a worker whose first reply was lost must be able to retry), but
	// heartbeating a terminal job is FailedPrecondition (ErrTerminal), as is
	// completing it under any other gen (ErrFenced). An unknown id is
	// NotFound.
	if _, err := c.Complete(ctx, &schedv1.CompleteRequest{Id: job.GetId(), Gen: job.GetGen(), Result: []byte("42")}); err != nil {
		t.Fatalf("idempotent Complete retry: %v, want success", err)
	}
	_, err = c.Heartbeat(ctx, &schedv1.HeartbeatRequest{Id: job.GetId(), Gen: job.GetGen()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Heartbeat on terminal job: %v, want FailedPrecondition", err)
	}
	_, err = c.Complete(ctx, &schedv1.CompleteRequest{Id: job.GetId(), Gen: job.GetGen() + 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Complete on terminal job with another gen: %v, want FailedPrecondition", err)
	}
	_, err = c.Status(ctx, &schedv1.StatusRequest{Id: 1 << 40})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Status of unknown id: %v, want NotFound", err)
	}

	stats, err := c.Stats(ctx, &schedv1.StatsRequest{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.GetMaxQueue() != 1024 {
		t.Fatalf("Stats.MaxQueue = %d, want the 1024 default", stats.GetMaxQueue())
	}
}

// A stale fencing token must be refused with FailedPrecondition, and the
// job must be untouched by the zombie's write.
func TestFencedCompleteIsFailedPrecondition(t *testing.T) {
	ctx := testCtx(t)
	c := start(t, sched.Options{Prefix: "t"})

	if _, err := c.Submit(ctx, &schedv1.SubmitRequest{Payload: []byte("work")}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	cl, err := c.Claim(ctx, &schedv1.ClaimRequest{Worker: "w1"})
	if err != nil || !cl.GetFound() {
		t.Fatalf("Claim: %v %v", cl, err)
	}
	job := cl.GetJob()

	_, err = c.Complete(ctx, &schedv1.CompleteRequest{Id: job.GetId(), Gen: job.GetGen() + 1, Result: []byte("zombie")})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Complete with stale gen: %v, want FailedPrecondition", err)
	}
	_, err = c.Heartbeat(ctx, &schedv1.HeartbeatRequest{Id: job.GetId(), Gen: job.GetGen() + 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Heartbeat with stale gen: %v, want FailedPrecondition", err)
	}

	st, err := c.Status(ctx, &schedv1.StatusRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetJob().GetState() != schedv1.State_STATE_RUNNING || len(st.GetJob().GetResult()) != 0 {
		t.Fatalf("fenced write leaked into the record: %v", st.GetJob())
	}

	// The rightful holder still can.
	if _, err := c.Complete(ctx, &schedv1.CompleteRequest{Id: job.GetId(), Gen: job.GetGen(), Result: []byte("ok")}); err != nil {
		t.Fatalf("Complete with correct gen: %v", err)
	}
}

// A full queue refuses admission with ResourceExhausted and a retry hint.
func TestQueueFullIsResourceExhausted(t *testing.T) {
	ctx := testCtx(t)
	c := start(t, sched.Options{Prefix: "t", MaxQueue: 2})

	admitted, refused := 0, 0
	var lastErr error
	for i := 0; i < 10; i++ {
		_, err := c.Submit(ctx, &schedv1.SubmitRequest{Payload: []byte{byte(i)}})
		switch status.Code(err) {
		case codes.OK:
			if refused > 0 {
				t.Fatalf("Submit %d admitted after the queue reported full", i)
			}
			admitted++
		case codes.ResourceExhausted:
			refused++
			lastErr = err
		default:
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	if admitted == 0 || admitted > 2 || refused == 0 {
		t.Fatalf("admitted %d, refused %d with MaxQueue=2", admitted, refused)
	}
	if msg := status.Convert(lastErr).Message(); !strings.Contains(msg, "retry_after_ms=200") {
		t.Fatalf("ResourceExhausted message %q lacks retry_after_ms=200", msg)
	}

	// Finishing the job at the front makes room again. head is advanced
	// lazily, by the next Claim (or Reap) scan that walks past the terminal
	// record at the front, so one more Claim (whatever it finds) is what
	// frees the slot.
	cl, err := c.Claim(ctx, &schedv1.ClaimRequest{Worker: "w"})
	if err != nil || !cl.GetFound() {
		t.Fatalf("Claim: %v %v", cl, err)
	}
	if _, err := c.Complete(ctx, &schedv1.CompleteRequest{Id: cl.GetJob().GetId(), Gen: cl.GetJob().GetGen()}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if _, err := c.Claim(ctx, &schedv1.ClaimRequest{Worker: "w"}); err != nil {
		t.Fatalf("Claim after draining: %v", err)
	}
	if _, err := c.Submit(ctx, &schedv1.SubmitRequest{Payload: []byte("late")}); err != nil {
		t.Fatalf("Submit after drain: %v", err)
	}
}
