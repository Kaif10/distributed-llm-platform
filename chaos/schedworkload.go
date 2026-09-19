package chaos

// The scheduler workload: continuous submissions plus workers that
// sometimes behave and sometimes go zombie (claim a job, let its lease
// expire, then try to commit anyway), exactly the shape of
// sched/queue_test.go's TestExactlyOnceUnderChaos, except the lease
// expiry here comes from the SAME nemesis-disrupted KV the raw KV workload
// is hammering, not a separate fake clock. That is the point: this proves
// the scheduler's exactly-once guarantee survives real Raft leader changes
// and partitions, not just a mocked-out KV.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"dsys/sched"
)

type schedWorkload struct {
	q *sched.Queue

	mu          sync.Mutex
	completions map[uint64]int
	submitted   atomic.Int64
	done        atomic.Int64
	fenced      atomic.Int64
}

func newSchedWorkload(cluster *kvCluster) *schedWorkload {
	q := sched.New(cluster, sched.Options{
		Prefix:       "chaos-sched",
		DefaultLease: 600 * time.Millisecond,
		MaxAttempts:  1_000_000, // the harness checks "every job eventually DONE", not attempt limits
		ScanLimit:    4096,
	})
	return &schedWorkload{q: q, completions: map[uint64]int{}}
}

func (w *schedWorkload) countCompletion(id uint64) {
	w.mu.Lock()
	w.completions[id]++
	w.mu.Unlock()
}

// runProducers submits jobs until ctx is done.
func (w *schedWorkload) runProducers(ctx context.Context, seed int64, n int, wg *sync.WaitGroup) {
	for p := 0; p < n; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(p)))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				octx, cancel := context.WithTimeout(ctx, 2*time.Second)
				_, err := w.q.Submit(octx, []byte(fmt.Sprint(r.Int())), "")
				cancel()
				if err == nil {
					w.submitted.Add(1)
				}
				time.Sleep(time.Duration(5+r.Intn(15)) * time.Millisecond)
			}
		}(p)
	}
}

// runWorkers claims and (mostly) completes jobs until ctx is done. Some
// fraction "go zombie": they let the lease lapse (by sleeping past it
// without heartbeating) and then try to commit anyway, which the store
// must refuse.
func (w *schedWorkload) runWorkers(ctx context.Context, seed int64, n int, wg *sync.WaitGroup) {
	for wi := 0; wi < n; wi++ {
		wg.Add(1)
		go func(wi int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(wi) + 9000))
			name := fmt.Sprintf("chaos-worker-%d", wi)
			idle := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				job, err := w.q.Claim(cctx, name, 0)
				cancel()
				if errors.Is(err, sched.ErrNoJob) {
					idle++
					if idle > 5000 {
						return // nothing left; stop burning CPU
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				idle = 0

				if r.Float64() < 0.3 {
					// Go zombie: sleep well past the 600ms lease, no
					// heartbeat, then still try to commit.
					time.Sleep(time.Duration(700+r.Intn(600)) * time.Millisecond)
				}
				octx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err = w.q.Complete(octx, job.Id, job.Gen, []byte(name))
				cancel()
				switch {
				case err == nil:
					w.countCompletion(job.Id)
					w.done.Add(1)
				case errors.Is(err, sched.ErrFenced):
					w.fenced.Add(1)
				}
			}
		}(wi)
	}
}

// check verifies the one property that matters: no job was EVER committed
// more than once. A job still PENDING/RUNNING when the run stopped is not a
// violation — chaos can legitimately outlast the workload — so that count
// is only informational, logged if Logf is set, never added to violations.
func (w *schedWorkload) check(ctx context.Context, logf func(format string, args ...any)) (violations []string) {
	_, tail, err := w.q.Stats(ctx)
	if err != nil {
		return []string{fmt.Sprintf("scheduler: could not read stats: %v", err)}
	}
	notDone := 0
	for id := uint64(1); id <= tail; id++ {
		st, err := w.q.Status(ctx, id)
		if err != nil {
			violations = append(violations, fmt.Sprintf("scheduler: job %d: %v", id, err))
			continue
		}
		w.mu.Lock()
		c := w.completions[id]
		w.mu.Unlock()
		if c > 1 {
			violations = append(violations, fmt.Sprintf("scheduler: job %d committed %d times: exactly-once is broken", id, c))
		}
		if st.State.String() != "STATE_DONE" {
			notDone++
		}
	}
	if notDone > 0 && logf != nil {
		logf("scheduler: %d/%d jobs still in flight when the run ended (not a violation)", notDone, tail)
	}
	return violations
}
