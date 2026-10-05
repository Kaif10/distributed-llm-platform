package sched

import (
	"errors"
	"fmt"
	"testing"
	"time"

	schedv1 "dsys/gen/sched/v1"
)

// ---------------------------------------------------------------------------
// Regression: head-of-line stall with the default ScanLimit.
// ---------------------------------------------------------------------------

// TestLongRunningHeadDoesNotStallClaims pins head with one long-running,
// heartbeating job and then pushes more short jobs than the DEFAULT
// ScanLimit through the queue. head can never move past a live job, so a
// scan that always starts at head spends its whole budget re-reading the
// pinned job and the DONE records behind it, and the jobs past head +
// ScanLimit are never claimed. Every one of them must be.
func TestLongRunningHeadDoesNotStallClaims(t *testing.T) {
	q, _, clk := newTestQueue(t, Options{}) // default ScanLimit (256)
	const short = 300
	if q.Options().ScanLimit >= short {
		t.Fatalf("test needs more jobs than ScanLimit=%d", q.Options().ScanLimit)
	}

	longID, _ := q.Submit(bg, []byte("long"), "")
	long, err := q.Claim(bg, "slow-worker", 0)
	if err != nil || long.Id != longID {
		t.Fatalf("claim long: %+v %v", long, err)
	}
	for i := 0; i < short; i++ {
		if _, err := q.Submit(bg, []byte(fmt.Sprint(i)), ""); err != nil {
			t.Fatal(err)
		}
	}

	completed := map[uint64]bool{}
	for iter := 0; len(completed) < short && iter < 20*short; iter++ {
		clk.advance(100 * time.Millisecond)
		if _, err := q.Heartbeat(bg, long.Id, long.Gen, 0); err != nil {
			t.Fatalf("long job lost its lease: %v", err)
		}
		job, err := q.Claim(bg, "fast-worker", 0)
		if errors.Is(err, ErrNoJob) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if job.Id == long.Id {
			t.Fatal("claimed the long job while its lease was live")
		}
		if err := q.Complete(bg, job.Id, job.Gen, nil); err != nil {
			t.Fatal(err)
		}
		completed[job.Id] = true
	}
	if len(completed) != short {
		t.Fatalf("only %d of %d short jobs were ever claimed behind a live head job (ScanLimit=%d)",
			len(completed), short, q.Options().ScanLimit)
	}
	if j, _ := q.Status(bg, long.Id); j.State != schedv1.State_STATE_RUNNING || j.Gen != long.Gen {
		t.Fatalf("long job disturbed: %+v", j)
	}
}

// Admission must bound LIVE work, not the head-to-tail span. One long job
// pins head (head only moves past terminal jobs in order), so a span-based
// bound refused new jobs once MaxQueue more had been submitted, even though
// every one of them had already finished.
func TestAdmissionCountsLiveJobsNotPinnedSpan(t *testing.T) {
	const maxQueue = 16
	q, _, clk := newTestQueue(t, Options{MaxQueue: maxQueue})
	if _, err := q.Submit(bg, []byte("long"), ""); err != nil {
		t.Fatal(err)
	}
	long, err := q.Claim(bg, "slow-worker", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10*maxQueue; i++ {
		clk.advance(10 * time.Millisecond)
		if _, err := q.Heartbeat(bg, long.Id, long.Gen, 0); err != nil {
			t.Fatalf("long job lost its lease: %v", err)
		}
		if _, err := q.Submit(bg, []byte(fmt.Sprint(i)), ""); err != nil {
			t.Fatalf("submit %d with only the long job live: %v", i, err)
		}
		job, err := q.Claim(bg, "fast-worker", 0)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if err := q.Complete(bg, job.Id, job.Gen, nil); err != nil {
			t.Fatalf("complete %d: %v", i, err)
		}
	}
}

// ...and it must still refuse when live work genuinely reaches MaxQueue.
func TestAdmissionStillBoundsLiveWork(t *testing.T) {
	const maxQueue = 8
	q, _, _ := newTestQueue(t, Options{MaxQueue: maxQueue})
	for i := 0; i < maxQueue; i++ {
		if _, err := q.Submit(bg, []byte(fmt.Sprint(i)), ""); err != nil {
			t.Fatalf("submit %d of %d: %v", i, maxQueue, err)
		}
	}
	if _, err := q.Submit(bg, []byte("one too many"), ""); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("with %d live jobs: err = %v, want ErrQueueFull", maxQueue, err)
	}
}
