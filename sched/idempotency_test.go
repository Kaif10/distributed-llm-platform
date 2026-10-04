package sched

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedv1 "dsys/gen/sched/v1"
)

// ---------------------------------------------------------------------------
// Regression: idempotent Submit under a partial failure.
// ---------------------------------------------------------------------------

// faultKV wraps memKV and fails the next CAS on one key with an error,
// standing in for a KV call that times out after the earlier writes of the
// same Submit already committed. before, if set, runs ahead of every CAS so
// a test can arm the fault on the Nth write to a key.
type faultKV struct {
	*memKV
	mu      sync.Mutex
	failKey string
	before  func(key string)
}

func (f *faultKV) failNextCAS(key string) {
	f.mu.Lock()
	f.failKey = key
	f.mu.Unlock()
}

func (f *faultKV) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	if f.before != nil {
		f.before(key)
	}
	f.mu.Lock()
	fail := f.failKey != "" && key == f.failKey
	if fail {
		f.failKey = ""
	}
	f.mu.Unlock()
	if fail {
		return false, nil, errors.New("injected: kv unavailable")
	}
	return f.memKV.CAS(ctx, key, expected, expectAbsent, value)
}

func newFaultQueue(t *testing.T) (*Queue, *faultKV, *fakeClock) {
	t.Helper()
	kv := &faultKV{memKV: newMemKV()}
	clk := newFakeClock()
	return New(kv, Options{Prefix: "t", Clock: clk.now}), kv, clk
}

// drainPayloads claims and completes everything runnable and returns how
// many claimed jobs carried each payload.
func drainPayloads(t *testing.T, q *Queue) map[string]int {
	t.Helper()
	seen := map[string]int{}
	for {
		j, err := q.Claim(bg, "w", 0)
		if errors.Is(err, ErrNoJob) {
			return seen
		}
		if err != nil {
			t.Fatal(err)
		}
		seen[string(j.Payload)]++
		if err := q.Complete(bg, j.Id, j.Gen, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// TestIdempotentSubmitRetryAfterTailBumpFails: the job record is written,
// then the tail bump fails, so Submit returns an error to a client that
// will retry with the same key. Whether the client retries at once or only
// after the reservation has expired, exactly one job must exist and run,
// and the retry must return its id.
func TestIdempotentSubmitRetryAfterTailBumpFails(t *testing.T) {
	for _, wait := range []time.Duration{0, idemPlaceholderTTL + time.Second} {
		t.Run(fmt.Sprintf("retry-after-%v", wait), func(t *testing.T) {
			q, kv, clk := newFaultQueue(t)
			kv.failNextCAS(q.keyTail())
			if _, err := q.Submit(bg, []byte("charge-card"), "order-7"); err == nil {
				t.Fatal("setup: injected tail-bump failure did not surface")
			}
			clk.advance(wait)

			ctx, cancel := context.WithTimeout(bg, 3*time.Second)
			defer cancel()
			id, err := q.Submit(ctx, []byte("charge-card"), "order-7")
			if err != nil {
				t.Fatalf("retry with the same key failed: %v", err)
			}
			again, err := q.Submit(ctx, []byte("charge-card"), "order-7")
			if err != nil || again != id {
				t.Fatalf("second retry: id=%d err=%v, want %d", again, err, id)
			}
			// A later plain Submit repairs tail over anything orphaned, so
			// an orphaned duplicate would now be inside the scan window.
			if _, err := q.Submit(bg, []byte("other"), ""); err != nil {
				t.Fatal(err)
			}

			seen := drainPayloads(t, q)
			if seen["charge-card"] != 1 {
				t.Fatalf("one idempotency key produced %d runnable jobs, want exactly 1", seen["charge-card"])
			}
			if j, err := q.Status(bg, id); err != nil || j.State != schedv1.State_STATE_DONE {
				t.Fatalf("the id the retry returned is not the job that ran: %+v %v", j, err)
			}
		})
	}
}

// TestIdempotentSubmitRetryAfterBindFails covers the other window: the job
// record is written but binding the idempotency key to it fails. The
// orphaned record must never run alongside the retry's job.
func TestIdempotentSubmitRetryAfterBindFails(t *testing.T) {
	q, kv, clk := newFaultQueue(t)
	// The first CAS on the idem key is the reservation; fail the second
	// (the bind), which comes after the job record is written.
	idemKey := q.keyIdem("order-8")
	var calls atomic.Int32
	kv.before = func(key string) {
		if key == idemKey && calls.Add(1) == 2 {
			kv.failNextCAS(idemKey)
		}
	}
	if _, err := q.Submit(bg, []byte("ship-parcel"), "order-8"); err == nil {
		t.Fatal("setup: injected bind failure did not surface")
	}
	clk.advance(idemPlaceholderTTL + time.Second)

	ctx, cancel := context.WithTimeout(bg, 3*time.Second)
	defer cancel()
	id, err := q.Submit(ctx, []byte("ship-parcel"), "order-8")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := q.Submit(bg, []byte("other"), ""); err != nil {
		t.Fatal(err)
	}
	seen := drainPayloads(t, q)
	if seen["ship-parcel"] != 1 {
		t.Fatalf("one idempotency key produced %d runnable jobs, want exactly 1", seen["ship-parcel"])
	}
	if j, _ := q.Status(bg, id); j.State != schedv1.State_STATE_DONE {
		t.Fatalf("retry's id %d is not the job that ran: %+v", id, j)
	}

	// The failed attempt's staged record must not pin head forever: once
	// it is older than stagedTTL a scan declares it abandoned.
	clk.advance(stagedTTL + time.Second)
	for i := 0; i < 2; i++ { // one pass to expire it, one to walk head past
		if _, err := q.Reap(bg); err != nil {
			t.Fatal(err)
		}
	}
	if head, tail, _ := q.Stats(bg); head <= tail {
		for id := head; id <= tail; id++ {
			j, _ := q.Status(bg, id)
			t.Logf("id %d: %v", id, j)
		}
		t.Fatalf("head=%d tail=%d: an abandoned staged record is pinning head", head, tail)
	}
}
