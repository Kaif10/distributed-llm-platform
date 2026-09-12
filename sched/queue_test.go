package sched

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedv1 "dsys/gen/sched/v1"
)

var chaosEvents = flag.Int("chaos-events", 1000, "minimum pauses+crashes for TestExactlyOnceUnderChaos")

// ---------------------------------------------------------------------------
// memKV: an in-memory linearizable KV. One mutex makes every operation
// atomic, which is exactly the guarantee the real store gives (through
// Raft) and exactly what the scheduler's CAS discipline relies on.
// ---------------------------------------------------------------------------

type memKV struct {
	mu   sync.Mutex
	m    map[string][]byte
	gets atomic.Int64
	cas  atomic.Int64
}

func newMemKV() *memKV { return &memKV{m: make(map[string][]byte)} }

func (k *memKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	k.gets.Add(1)
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
	k.cas.Add(1)
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.m[key]
	var matches bool
	if expectAbsent {
		matches = !ok
	} else {
		matches = ok && bytes.Equal(cur, expected)
	}
	if matches {
		k.m[key] = bytes.Clone(value)
		return true, bytes.Clone(cur), nil
	}
	return false, bytes.Clone(cur), nil
}

// fakeClock lets tests move time without sleeping.
type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000)
	return c
}
func (c *fakeClock) now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

func newTestQueue(t *testing.T, opts Options) (*Queue, *memKV, *fakeClock) {
	t.Helper()
	kv := newMemKV()
	clk := newFakeClock()
	opts.Clock = clk.now
	if opts.Prefix == "" {
		opts.Prefix = "t"
	}
	return New(kv, opts), kv, clk
}

var bg = context.Background()

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func TestSubmitClaimComplete(t *testing.T) {
	q, _, _ := newTestQueue(t, Options{})
	id, err := q.Submit(bg, []byte("hello"), "")
	if err != nil || id != 1 {
		t.Fatalf("submit: id=%d err=%v", id, err)
	}
	id2, _ := q.Submit(bg, []byte("second"), "")
	if id2 != 2 {
		t.Fatalf("second id=%d", id2)
	}

	job, err := q.Claim(bg, "w1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if job.Id != 1 || job.State != schedv1.State_STATE_RUNNING || job.Gen != 1 || job.Worker != "w1" || job.Attempts != 1 {
		t.Fatalf("claimed: %+v", job)
	}
	if string(job.Payload) != "hello" {
		t.Fatalf("payload %q", job.Payload)
	}

	if err := q.Complete(bg, 1, job.Gen, []byte("done")); err != nil {
		t.Fatal(err)
	}
	// Idempotent retry of the same Complete succeeds.
	if err := q.Complete(bg, 1, job.Gen, []byte("done")); err != nil {
		t.Fatalf("retry complete: %v", err)
	}
	st, _ := q.Status(bg, 1)
	if st.State != schedv1.State_STATE_DONE || string(st.Result) != "done" {
		t.Fatalf("status %+v", st)
	}

	// Next claim gets job 2 and, in passing, advances head past 1.
	job2, err := q.Claim(bg, "w2", 0)
	if err != nil || job2.Id != 2 {
		t.Fatalf("claim2: %+v %v", job2, err)
	}
	head, tail, _ := q.Stats(bg)
	if head != 2 || tail != 2 {
		t.Fatalf("head=%d tail=%d", head, tail)
	}
	if _, err := q.Claim(bg, "w3", 0); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob, got %v", err)
	}
}

func TestQueueFull(t *testing.T) {
	q, _, _ := newTestQueue(t, Options{MaxQueue: 2})
	if _, err := q.Submit(bg, nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Submit(bg, nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Submit(bg, nil, ""); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	// Finishing a job and letting head advance frees a slot.
	j, _ := q.Claim(bg, "w", 0)
	_ = q.Complete(bg, j.Id, j.Gen, nil)
	_, _ = q.Claim(bg, "w", 0) // advances head past the DONE job, claims job 2
	if _, err := q.Submit(bg, nil, ""); err != nil {
		t.Fatalf("after drain: %v", err)
	}
}

func TestIdempotentSubmit(t *testing.T) {
	q, _, _ := newTestQueue(t, Options{})
	a, err := q.Submit(bg, []byte("x"), "order-42")
	if err != nil {
		t.Fatal(err)
	}
	b, err := q.Submit(bg, []byte("x"), "order-42")
	if err != nil || a != b {
		t.Fatalf("a=%d b=%d err=%v", a, b, err)
	}
	c, _ := q.Submit(bg, []byte("y"), "order-43")
	if c == a {
		t.Fatal("different keys must get different ids")
	}
	if _, tail, _ := q.Stats(bg); tail != 2 {
		t.Fatalf("tail=%d want 2 (duplicate must not allocate)", tail)
	}
}

// ---------------------------------------------------------------------------
// Fencing: the whole point of the phase.
// ---------------------------------------------------------------------------

func TestFencingAfterReap(t *testing.T) {
	q, _, clk := newTestQueue(t, Options{DefaultLease: time.Second, MaxAttempts: 10})
	_, _ = q.Submit(bg, []byte("job"), "")
	old, err := q.Claim(bg, "zombie", 0)
	if err != nil {
		t.Fatal(err)
	}

	// The zombie is paused; time passes past its lease; the reaper acts.
	clk.advance(2 * time.Second)
	n, err := q.Reap(bg)
	if err != nil || n != 1 {
		t.Fatalf("reap: n=%d err=%v", n, err)
	}
	st, _ := q.Status(bg, old.Id)
	if st.State != schedv1.State_STATE_PENDING || st.Gen != old.Gen+1 {
		t.Fatalf("after reap: %+v", st)
	}

	// A new worker takes it under gen+2.
	fresh, err := q.Claim(bg, "fresh", 0)
	if err != nil || fresh.Gen != old.Gen+2 {
		t.Fatalf("fresh claim: %+v %v", fresh, err)
	}

	// The zombie wakes up. Every write it attempts is refused.
	if _, err := q.Heartbeat(bg, old.Id, old.Gen, 0); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie heartbeat: %v", err)
	}
	if err := q.Complete(bg, old.Id, old.Gen, []byte("ZOMBIE RESULT")); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie complete: %v", err)
	}
	if _, err := q.Fail(bg, old.Id, old.Gen, "x"); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie fail: %v", err)
	}

	// The legitimate holder completes; the recorded result is theirs.
	if err := q.Complete(bg, fresh.Id, fresh.Gen, []byte("real")); err != nil {
		t.Fatal(err)
	}
	st, _ = q.Status(bg, old.Id)
	if string(st.Result) != "real" || st.Worker != "fresh" {
		t.Fatalf("final: %+v", st)
	}
	// And the zombie completing AFTER the real one is still refused (DONE
	// under a different gen).
	if err := q.Complete(bg, old.Id, old.Gen, []byte("late zombie")); !errors.Is(err, ErrFenced) {
		t.Fatalf("late zombie: %v", err)
	}
}

// Claimers reap in passing: no separate reaper is required for liveness.
func TestClaimReclaimsExpiredLease(t *testing.T) {
	q, _, clk := newTestQueue(t, Options{DefaultLease: time.Second, MaxAttempts: 10})
	_, _ = q.Submit(bg, nil, "")
	a, _ := q.Claim(bg, "a", 0)
	if _, err := q.Claim(bg, "b", 0); !errors.Is(err, ErrNoJob) {
		t.Fatalf("live lease must not be stolen: %v", err)
	}
	clk.advance(1500 * time.Millisecond)
	b, err := q.Claim(bg, "b", 0)
	if err != nil || b.Id != a.Id || b.Gen != a.Gen+1 || b.Worker != "b" || b.Attempts != 2 {
		t.Fatalf("reclaim: %+v %v", b, err)
	}
	if _, err := q.Heartbeat(bg, a.Id, a.Gen, 0); !errors.Is(err, ErrFenced) {
		t.Fatalf("a should be fenced: %v", err)
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	q, _, clk := newTestQueue(t, Options{DefaultLease: time.Second})
	_, _ = q.Submit(bg, nil, "")
	j, _ := q.Claim(bg, "w", 0)
	clk.advance(800 * time.Millisecond)
	until, err := q.Heartbeat(bg, j.Id, j.Gen, 0)
	if err != nil {
		t.Fatal(err)
	}
	if until.Sub(clk.now()) != time.Second {
		t.Fatalf("lease until %v, now %v", until, clk.now())
	}
	clk.advance(800 * time.Millisecond) // 1.6s since claim, 0.8s since heartbeat
	if n, _ := q.Reap(bg); n != 0 {
		t.Fatalf("reaped a live lease: %d", n)
	}
}

func TestMaxAttemptsViaExpiry(t *testing.T) {
	q, _, clk := newTestQueue(t, Options{DefaultLease: time.Second, MaxAttempts: 2})
	_, _ = q.Submit(bg, nil, "")
	for attempt := 1; attempt <= 2; attempt++ {
		j, err := q.Claim(bg, "w", 0)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if j.Attempts != uint32(attempt) {
			t.Fatalf("attempts=%d", j.Attempts)
		}
		clk.advance(2 * time.Second)
		_, _ = q.Reap(bg)
	}
	st, _ := q.Status(bg, 1)
	if st.State != schedv1.State_STATE_FAILED {
		t.Fatalf("after exhausting attempts: %+v", st)
	}
	if _, err := q.Claim(bg, "w", 0); !errors.Is(err, ErrNoJob) {
		t.Fatalf("failed job must not be claimable: %v", err)
	}
}

func TestFailRequeuesThenFails(t *testing.T) {
	q, _, _ := newTestQueue(t, Options{MaxAttempts: 2})
	_, _ = q.Submit(bg, nil, "")
	j, _ := q.Claim(bg, "w", 0)
	requeued, err := q.Fail(bg, j.Id, j.Gen, "boom")
	if err != nil || !requeued {
		t.Fatalf("fail1: %v %v", requeued, err)
	}
	if err := q.Complete(bg, j.Id, j.Gen, nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("old gen after Fail must be fenced: %v", err)
	}
	j2, _ := q.Claim(bg, "w", 0)
	if j2.Gen != j.Gen+2 || j2.Attempts != 2 {
		t.Fatalf("j2 %+v", j2)
	}
	requeued, err = q.Fail(bg, j2.Id, j2.Gen, "boom again")
	if err != nil || requeued {
		t.Fatalf("fail2: %v %v", requeued, err)
	}
	st, _ := q.Status(bg, 1)
	if st.State != schedv1.State_STATE_FAILED || st.LastError != "boom again" {
		t.Fatalf("final %+v", st)
	}
	if _, err := q.Heartbeat(bg, 1, j2.Gen, 0); !errors.Is(err, ErrTerminal) {
		t.Fatalf("heartbeat on FAILED: %v", err)
	}
}

// A Submit that crashed after writing its record but before bumping tail
// must not lose that job or block later submits.
func TestSubmitRepairsAfterCrash(t *testing.T) {
	q, kv, _ := newTestQueue(t, Options{})
	_, _ = q.Submit(bg, []byte("a"), "") // id 1, tail 1
	// Simulate the crash: record 2 exists, tail still 1.
	orphan := &schedv1.Job{Id: 2, Payload: []byte("orphan"), State: schedv1.State_STATE_PENDING}
	if ok, _ := q.casJob(bg, 2, nil, orphan); !ok {
		t.Fatal("setup")
	}
	if _, tail, _ := q.Stats(bg); tail != 1 {
		t.Fatalf("setup tail=%d", tail)
	}
	id, err := q.Submit(bg, []byte("c"), "")
	if err != nil || id != 3 {
		t.Fatalf("submit after orphan: id=%d err=%v", id, err)
	}
	if _, tail, _ := q.Stats(bg); tail != 3 {
		t.Fatalf("tail=%d want 3", tail)
	}
	// All three are claimable, in order.
	for want := uint64(1); want <= 3; want++ {
		j, err := q.Claim(bg, "w", 0)
		if err != nil || j.Id != want {
			t.Fatalf("claim %d: %+v %v", want, j, err)
		}
	}
	_ = kv
}

func TestLeaderLease(t *testing.T) {
	q, _, clk := newTestQueue(t, Options{})
	ttl := 3 * time.Second
	if ok, _ := q.tryLead(bg, "A", ttl); !ok {
		t.Fatal("A should acquire")
	}
	if ok, _ := q.tryLead(bg, "B", ttl); ok {
		t.Fatal("B must not acquire while A's lease is live")
	}
	if ok, _ := q.tryLead(bg, "A", ttl); !ok {
		t.Fatal("A should renew")
	}
	clk.advance(4 * time.Second)
	if ok, _ := q.tryLead(bg, "B", ttl); !ok {
		t.Fatal("B should take over an expired lease")
	}
	holder, _, _ := q.Leader(bg)
	if holder != "B" {
		t.Fatalf("leader=%q", holder)
	}
}

// ---------------------------------------------------------------------------
// The Phase 4 exit criterion: random pauses past the lease and outright
// crashes, thousands of them, and every job is completed exactly once.
// ---------------------------------------------------------------------------

func TestExactlyOnceUnderChaos(t *testing.T) {
	const (
		workers = 8
		lease   = 100 * time.Millisecond
	)
	q, _, clk := newTestQueue(t, Options{DefaultLease: lease, MaxAttempts: 1_000_000, MaxQueue: 1 << 20, ScanLimit: 4096})

	// Enough jobs that, at the event rates below, we comfortably exceed the
	// requested number of pauses+crashes.
	nJobs := *chaosEvents * 2
	for i := 0; i < nJobs; i++ {
		if _, err := q.Submit(bg, []byte(fmt.Sprint(i)), ""); err != nil {
			t.Fatal(err)
		}
	}

	var (
		completions sync.Map // id -> *atomic.Int32 successful Complete calls
		pauses      atomic.Int64
		crashes     atomic.Int64
		fenced      atomic.Int64
		done        atomic.Int64
		stop        = make(chan struct{})
		wg          sync.WaitGroup
	)
	countFor := func(id uint64) *atomic.Int32 {
		v, _ := completions.LoadOrStore(id, new(atomic.Int32))
		return v.(*atomic.Int32)
	}

	// Time marches on regardless of what workers do: this is what turns a
	// real-time pause into an expired lease.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				clk.advance(time.Duration(5+rand.Intn(20)) * time.Millisecond)
			}
		}
	}()

	// The reaper.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
				if _, err := q.Reap(bg); err != nil {
					t.Error(err)
				}
			}
		}
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			name := fmt.Sprintf("w%d", w)
			idle := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				job, err := q.Claim(bg, name, 0)
				if errors.Is(err, ErrNoJob) {
					idle++
					if idle > 200 {
						return // queue drained
					}
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				idle = 0

				switch x := r.Float64(); {
				case x < 0.25:
					// Crash: claimed it, never heard from again.
					crashes.Add(1)
					continue
				case x < 0.55:
					// Pause past the lease (the fake clock keeps moving),
					// then wake up and try to commit anyway.
					pauses.Add(1)
					time.Sleep(time.Duration(15+r.Intn(30)) * time.Millisecond)
				default:
					// Healthy: heartbeat once, do a little work, complete.
					if _, err := q.Heartbeat(bg, job.Id, job.Gen, 0); errors.Is(err, ErrFenced) {
						fenced.Add(1)
						continue
					}
				}
				err = q.Complete(bg, job.Id, job.Gen, []byte(name))
				switch {
				case err == nil:
					countFor(job.Id).Add(1)
					done.Add(1)
				case errors.Is(err, ErrFenced):
					fenced.Add(1)
				default:
					t.Errorf("complete: %v", err)
				}
			}
		}(w)
	}

	// Wait for every job to reach DONE.
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		head, tail, _ := q.Stats(bg)
		if head > tail {
			break
		}
		// Not all workers may have advanced head; check the records.
		all := true
		for id := head; id <= tail; id++ {
			st, err := q.Status(bg, id)
			if err != nil || st.State != schedv1.State_STATE_DONE {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	events := pauses.Load() + crashes.Load()
	t.Logf("jobs=%d completes=%d pauses=%d crashes=%d fenced=%d gets=%d cas=%d",
		nJobs, done.Load(), pauses.Load(), crashes.Load(), fenced.Load(), 0, 0)

	if events < int64(*chaosEvents) {
		t.Fatalf("only %d chaos events, wanted >= %d; raise job count", events, *chaosEvents)
	}
	if fenced.Load() == 0 {
		t.Fatal("no worker was ever fenced; the scenario did not exercise the zombie path")
	}
	// The property: every job DONE, each completed by exactly one accepted
	// Complete, and no job completed twice.
	multi := 0
	missing := 0
	for id := uint64(1); id <= uint64(nJobs); id++ {
		st, err := q.Status(bg, id)
		if err != nil || st.State != schedv1.State_STATE_DONE {
			missing++
			continue
		}
		if c := countFor(id).Load(); c != 1 {
			multi++
			t.Errorf("job %d: %d accepted Completes", id, c)
		}
	}
	if missing > 0 {
		t.Fatalf("%d jobs never reached DONE", missing)
	}
	if multi > 0 {
		t.Fatalf("%d jobs completed more than once: exactly-once commit is broken", multi)
	}
	if done.Load() != int64(nJobs) {
		t.Fatalf("accepted completes=%d, jobs=%d", done.Load(), nJobs)
	}
}
