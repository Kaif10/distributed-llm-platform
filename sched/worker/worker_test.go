package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedv1 "dsys/gen/sched/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// fakeSched is an in-memory schedv1.SchedulerServer with exactly the
// semantics the worker library depends on: jobs with a gen, a lease, and a
// state; FailedPrecondition on any stale gen; accepted Completes recorded
// per (id, gen) so a test can prove a zombie never got its result in. No KV,
// no reaper: tests reassign jobs by hand with reassign().
type fakeSched struct {
	schedv1.UnimplementedSchedulerServer

	mu          sync.Mutex
	jobs        map[uint64]*schedv1.Job
	next        uint64
	maxAttempts uint32

	running    int
	maxRunning int

	// acceptedCompletes[id][gen] counts Completes the fake accepted.
	acceptedCompletes map[uint64]map[uint64]int
	fails             []*schedv1.FailRequest
	heartbeats        atomic.Int64

	// unavailableCompletes makes the next N Complete calls answer
	// Unavailable, to exercise the retry path.
	unavailableCompletes int
	completeGens         []uint64 // gens seen on every Complete attempt
}

func newFakeSched() *fakeSched {
	return &fakeSched{
		jobs:              map[uint64]*schedv1.Job{},
		maxAttempts:       3,
		acceptedCompletes: map[uint64]map[uint64]int{},
	}
}

func (f *fakeSched) Submit(_ context.Context, req *schedv1.SubmitRequest) (*schedv1.SubmitResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	f.jobs[f.next] = &schedv1.Job{Id: f.next, Payload: req.GetPayload(), State: schedv1.State_STATE_PENDING, SubmittedMs: time.Now().UnixMilli()}
	return &schedv1.SubmitResponse{Id: f.next}, nil
}

func (f *fakeSched) Claim(_ context.Context, req *schedv1.ClaimRequest) (*schedv1.ClaimResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id := uint64(1); id <= f.next; id++ {
		j := f.jobs[id]
		if j.State != schedv1.State_STATE_PENDING {
			continue
		}
		j.State = schedv1.State_STATE_RUNNING
		j.Gen++
		j.Worker = req.GetWorker()
		j.Attempts++
		j.LeaseUntilMs = time.Now().Add(time.Duration(req.GetLeaseMs()) * time.Millisecond).UnixMilli()
		f.running++
		f.maxRunning = max(f.maxRunning, f.running)
		return &schedv1.ClaimResponse{Found: true, Job: proto.Clone(j).(*schedv1.Job)}, nil
	}
	return &schedv1.ClaimResponse{Found: false}, nil
}

// checkOwner is the fencing check: the job must exist, be RUNNING, and the
// presented gen must be current.
func (f *fakeSched) checkOwner(id, gen uint64) (*schedv1.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such job")
	}
	if j.State == schedv1.State_STATE_DONE || j.State == schedv1.State_STATE_FAILED {
		return nil, status.Error(codes.FailedPrecondition, "job is terminal")
	}
	if j.State != schedv1.State_STATE_RUNNING || j.Gen != gen {
		return nil, status.Errorf(codes.FailedPrecondition, "fenced: gen %d is stale (current %d)", gen, j.Gen)
	}
	return j, nil
}

func (f *fakeSched) Heartbeat(_ context.Context, req *schedv1.HeartbeatRequest) (*schedv1.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats.Add(1)
	j, err := f.checkOwner(req.GetId(), req.GetGen())
	if err != nil {
		return nil, err
	}
	j.LeaseUntilMs = time.Now().Add(time.Duration(req.GetLeaseMs()) * time.Millisecond).UnixMilli()
	return &schedv1.HeartbeatResponse{LeaseUntilMs: j.LeaseUntilMs}, nil
}

func (f *fakeSched) Complete(_ context.Context, req *schedv1.CompleteRequest) (*schedv1.CompleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeGens = append(f.completeGens, req.GetGen())
	if f.unavailableCompletes > 0 {
		f.unavailableCompletes--
		return nil, status.Error(codes.Unavailable, "injected")
	}
	j, err := f.checkOwner(req.GetId(), req.GetGen())
	if err != nil {
		return nil, err
	}
	j.State = schedv1.State_STATE_DONE
	j.Result = req.GetResult()
	f.running--
	if f.acceptedCompletes[j.Id] == nil {
		f.acceptedCompletes[j.Id] = map[uint64]int{}
	}
	f.acceptedCompletes[j.Id][req.GetGen()]++
	return &schedv1.CompleteResponse{}, nil
}

func (f *fakeSched) Fail(_ context.Context, req *schedv1.FailRequest) (*schedv1.FailResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.checkOwner(req.GetId(), req.GetGen())
	if err != nil {
		return nil, err
	}
	f.fails = append(f.fails, req)
	f.running--
	j.LastError = req.GetError()
	if j.Attempts >= f.maxAttempts {
		j.State = schedv1.State_STATE_FAILED
		return &schedv1.FailResponse{Requeued: false}, nil
	}
	j.State = schedv1.State_STATE_PENDING
	j.Gen++
	j.Worker = ""
	return &schedv1.FailResponse{Requeued: true}, nil
}

func (f *fakeSched) Status(_ context.Context, req *schedv1.StatusRequest) (*schedv1.StatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[req.GetId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such job")
	}
	return &schedv1.StatusResponse{Job: proto.Clone(j).(*schedv1.Job)}, nil
}

// reassign is what the reaper does when a lease lapses: the job goes back
// to PENDING with a new gen. The old holder does not know.
func (f *fakeSched) reassign(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	if j.State == schedv1.State_STATE_RUNNING {
		f.running--
	}
	j.State = schedv1.State_STATE_PENDING
	j.Gen++
	j.Worker = ""
}

func (f *fakeSched) job(id uint64) *schedv1.Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	return proto.Clone(f.jobs[id]).(*schedv1.Job)
}

func (f *fakeSched) allTerminal() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, j := range f.jobs {
		if j.State != schedv1.State_STATE_DONE && j.State != schedv1.State_STATE_FAILED {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------

type harness struct {
	t      *testing.T
	fake   *fakeSched
	client schedv1.SchedulerClient
	log    *slog.Logger
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	fake := newFakeSched()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	schedv1.RegisterSchedulerServer(gs, fake)
	go gs.Serve(lis)
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close(); gs.Stop() })
	return &harness{
		t:      t,
		fake:   fake,
		client: schedv1.NewSchedulerClient(cc),
		log:    slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func (h *harness) submit(payload string) uint64 {
	resp, err := h.client.Submit(context.Background(), &schedv1.SubmitRequest{Payload: []byte(payload)})
	if err != nil {
		h.t.Fatal(err)
	}
	return resp.GetId()
}

// start runs w in the background and returns a stop func that cancels it
// and waits for Run to return (so no goroutine logs after the test ends).
func (h *harness) start(w *Worker) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				h.t.Errorf("Run returned %v, want nil on clean stop", err)
			}
		case <-time.After(10 * time.Second):
			h.t.Fatal("Run did not return after cancel")
		}
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", d, what)
}

// ---------------------------------------------------------------------------

// (a) Happy path: claim -> heartbeat -> complete, with the result stored
// under the claimed gen.
func TestHappyPath(t *testing.T) {
	h := newHarness(t)
	id := h.submit("hello")

	var sawGen atomic.Uint64
	w := New(h.client, func(ctx context.Context, job *schedv1.Job) ([]byte, error) {
		sawGen.Store(job.GetGen())
		time.Sleep(250 * time.Millisecond) // long enough for a few heartbeats
		return []byte("echo:" + string(job.GetPayload())), nil
	}, Options{Name: "w-a", Lease: 150 * time.Millisecond, PollInterval: 10 * time.Millisecond, Logger: h.log})
	stop := h.start(w)

	waitFor(t, 5*time.Second, "job DONE", func() bool { return h.fake.job(id).State == schedv1.State_STATE_DONE })
	stop()

	j := h.fake.job(id)
	if got := string(j.Result); got != "echo:hello" {
		t.Errorf("result = %q, want echo:hello", got)
	}
	if j.Worker != "w-a" {
		t.Errorf("worker = %q, want w-a", j.Worker)
	}
	if hb := h.fake.heartbeats.Load(); hb < 2 {
		t.Errorf("heartbeats = %d, want >= 2 (lease 150ms, interval 50ms, handler 250ms)", hb)
	}
	if n := h.fake.acceptedCompletes[id][sawGen.Load()]; n != 1 {
		t.Errorf("accepted completes for gen %d = %d, want 1", sawGen.Load(), n)
	}
	if s := w.Stats(); s != (Stats{Claimed: 1, Completed: 1}) {
		t.Errorf("stats = %+v, want Claimed=1 Completed=1", s)
	}
}

// (b) The zombie: the job is reassigned while the handler is mid-run. The
// next heartbeat is fenced, the handler's context must be cancelled within
// about two heartbeat intervals, and no Complete under the stale gen may
// ever be accepted. The worker then (legitimately) claims the same job
// under the new gen and completes it: at-least-once execution, exactly-once
// commit.
func TestZombieFencedMidRun(t *testing.T) {
	h := newHarness(t)
	id := h.submit("zombie")
	const lease = 300 * time.Millisecond
	hbInterval := lease / 3

	started := make(chan uint64, 4)  // gen of each handler invocation
	cancelled := make(chan error, 4) // context.Cause when a run was cancelled
	w := New(h.client, func(ctx context.Context, job *schedv1.Job) ([]byte, error) {
		started <- job.GetGen()
		if job.GetGen() == 1 {
			// First run: block until the worker cancels us. Then keep
			// "working" a little and return success anyway, the way a real
			// zombie that ignores ctx would; the library must discard it.
			<-ctx.Done()
			cancelled <- context.Cause(ctx)
			return []byte("stale result"), nil
		}
		return []byte("fresh result"), nil
	}, Options{Name: "w-b", Lease: lease, PollInterval: 10 * time.Millisecond, Logger: h.log})
	stop := h.start(w)

	var firstGen uint64
	select {
	case firstGen = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	if firstGen != 1 {
		t.Fatalf("first claim gen = %d, want 1", firstGen)
	}
	// Let at least one successful heartbeat through, then pull the rug.
	waitFor(t, 2*time.Second, "a heartbeat", func() bool { return h.fake.heartbeats.Load() >= 1 })
	reassignedAt := time.Now()
	h.fake.reassign(id)

	select {
	case cause := <-cancelled:
		if !errors.Is(cause, errFenced) {
			t.Errorf("cancel cause = %v, want errFenced", cause)
		}
		if took := time.Since(reassignedAt); took > 2*hbInterval+100*time.Millisecond {
			t.Errorf("handler cancelled %v after reassign; want within ~2 heartbeat intervals (%v)", took, 2*hbInterval)
		} else {
			t.Logf("handler cancelled %v after reassign (heartbeat interval %v)", took, hbInterval)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler context was never cancelled after reassign")
	}

	// The same worker now claims gen 2 and finishes it for real.
	waitFor(t, 5*time.Second, "job DONE under new gen", func() bool { return h.fake.job(id).State == schedv1.State_STATE_DONE })
	stop()

	// The reap bumped gen to 2 and the re-Claim to 3: gen moves on every
	// change of hands, so the new holder is gen 3, and only gen 3 commits.
	final := h.fake.job(id)
	if n := h.fake.acceptedCompletes[id][1]; n != 0 {
		t.Fatalf("stale gen 1 had %d Completes accepted; must be 0", n)
	}
	if final.Gen != 3 {
		t.Errorf("final gen = %d, want 3 (claim=1, reap=2, re-claim=3)", final.Gen)
	}
	if n := h.fake.acceptedCompletes[id][final.Gen]; n != 1 {
		t.Errorf("gen %d accepted completes = %d, want 1", final.Gen, n)
	}
	if got := string(final.Result); got != "fresh result" {
		t.Errorf("stored result = %q, want the new holder's", got)
	}
	s := w.Stats()
	if s.Fenced != 1 {
		t.Errorf("Fenced = %d, want 1 (stats %+v)", s.Fenced, s)
	}
	if s.Completed != 1 || s.Claimed != 2 {
		t.Errorf("stats = %+v, want Claimed=2 Completed=1", s)
	}
}

// (b') The other zombie: no heartbeat catches it, the handler finishes,
// and Complete itself is refused. Must log, count fenced, not retry, not
// crash, and go on to the next job.
func TestZombieAtComplete(t *testing.T) {
	h := newHarness(t)
	id1 := h.submit("first")
	id2 := h.submit("second")

	release := make(chan struct{})
	w := New(h.client, func(ctx context.Context, job *schedv1.Job) ([]byte, error) {
		if job.GetId() == id1 && job.GetGen() == 1 {
			<-release
		}
		return []byte("r"), nil
	}, Options{Name: "w-b2", Lease: 10 * time.Second, PollInterval: 10 * time.Millisecond, Logger: h.log})
	stop := h.start(w)

	waitFor(t, 5*time.Second, "job1 RUNNING", func() bool { return h.fake.job(id1).State == schedv1.State_STATE_RUNNING })
	h.fake.reassign(id1) // heartbeat interval is 3.3s; the handler will finish first
	close(release)

	// Worker must proceed to job2 and, since job1 is PENDING at gen 2, also
	// re-run job1 under gen 2.
	waitFor(t, 5*time.Second, "both jobs DONE", h.fake.allTerminal)
	stop()

	if n := h.fake.acceptedCompletes[id1][1]; n != 0 {
		t.Fatalf("stale gen accepted %d completes; must be 0", n)
	}
	if n := len(h.fake.completeGens); n != 3 {
		t.Errorf("Complete attempts = %d (%v), want exactly 3: one refused stale, two accepted; a refused Complete must not be retried", n, h.fake.completeGens)
	}
	if s := w.Stats(); s.Fenced != 1 || s.Completed != 2 || s.Claimed != 3 {
		t.Errorf("stats = %+v, want Claimed=3 Completed=2 Fenced=1", s)
	}
	_ = id2
}

// (c) A handler panic is recovered, reported via Fail, and the worker keeps
// going.
func TestHandlerPanicIsFailed(t *testing.T) {
	h := newHarness(t)
	h.fake.maxAttempts = 1 // one panic -> terminal FAILED, so it is not retried
	idBad := h.submit("boom")
	idGood := h.submit("fine")

	w := New(h.client, func(ctx context.Context, job *schedv1.Job) ([]byte, error) {
		if string(job.GetPayload()) == "boom" {
			panic("kaboom")
		}
		return []byte("ok"), nil
	}, Options{Name: "w-c", Lease: time.Second, PollInterval: 10 * time.Millisecond, Logger: h.log})
	stop := h.start(w)

	waitFor(t, 5*time.Second, "both jobs terminal", h.fake.allTerminal)
	stop()

	if st := h.fake.job(idBad).State; st != schedv1.State_STATE_FAILED {
		t.Errorf("bad job state = %v, want FAILED", st)
	}
	if st := h.fake.job(idGood).State; st != schedv1.State_STATE_DONE {
		t.Errorf("good job state = %v, want DONE", st)
	}
	if len(h.fake.fails) != 1 {
		t.Fatalf("Fail calls = %d, want 1", len(h.fake.fails))
	}
	if fr := h.fake.fails[0]; fr.GetId() != idBad || !strings.Contains(fr.GetError(), "panic") || !strings.Contains(fr.GetError(), "kaboom") {
		t.Errorf("Fail request = %+v, want id %d with the panic value in the error", fr, idBad)
	}
	if s := w.Stats(); s != (Stats{Claimed: 2, Completed: 1, Failed: 1}) {
		t.Errorf("stats = %+v, want Claimed=2 Completed=1 Failed=1", s)
	}
}

// (d) Concurrency=4 processes 20 jobs with at most 4 in flight.
func TestConcurrencyBound(t *testing.T) {
	h := newHarness(t)
	const n, c = 20, 4
	for i := 0; i < n; i++ {
		h.submit(fmt.Sprintf("job-%d", i))
	}

	var inFlight, maxInFlight atomic.Int32
	w := New(h.client, func(ctx context.Context, job *schedv1.Job) ([]byte, error) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		return job.GetPayload(), nil
	}, Options{Name: "w-d", Concurrency: c, Lease: time.Second, PollInterval: 10 * time.Millisecond, Logger: h.log})
	stop := h.start(w)

	waitFor(t, 10*time.Second, "all 20 jobs DONE", h.fake.allTerminal)
	stop()

	if got := h.fake.maxRunning; got > c {
		t.Errorf("server saw %d RUNNING at once, want <= %d", got, c)
	} else if got < 2 {
		t.Errorf("server saw at most %d RUNNING at once; expected real parallelism with Concurrency=%d", got, c)
	} else {
		t.Logf("max RUNNING at once: %d (handler-side max %d)", got, maxInFlight.Load())
	}
	if hm := maxInFlight.Load(); hm > c {
		t.Errorf("handler saw %d in flight, want <= %d", hm, c)
	}
	if s := w.Stats(); s.Completed != n || s.Claimed != n || s.Fenced != 0 || s.Failed != 0 {
		t.Errorf("stats = %+v, want Claimed=Completed=%d", s, n)
	}
}

// (e) Transient Unavailable on Complete is retried with the SAME gen, and
// the job ends DONE exactly once.
func TestTransientCompleteRetried(t *testing.T) {
	h := newHarness(t)
	h.fake.unavailableCompletes = 2
	id := h.submit("flaky")

	w := New(h.client, func(ctx context.Context, job *schedv1.Job) ([]byte, error) {
		return []byte("ok"), nil
	}, Options{Name: "w-e", Lease: time.Second, PollInterval: 10 * time.Millisecond, Logger: h.log})
	stop := h.start(w)
	waitFor(t, 5*time.Second, "job DONE", func() bool { return h.fake.job(id).State == schedv1.State_STATE_DONE })
	stop()

	if got := h.fake.completeGens; len(got) != 3 || got[0] != 1 || got[1] != 1 || got[2] != 1 {
		t.Errorf("Complete attempts (gens) = %v, want [1 1 1]: retries reuse the claimed gen", got)
	}
	if s := w.Stats(); s != (Stats{Claimed: 1, Completed: 1}) {
		t.Errorf("stats = %+v, want Claimed=1 Completed=1", s)
	}
}
