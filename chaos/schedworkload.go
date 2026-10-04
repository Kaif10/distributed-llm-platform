package chaos

// The scheduler workload: continuous submissions (plain, and idempotent
// with deliberate duplicate submits of the same key) plus workers that
// sometimes behave and sometimes go zombie (claim a job, let its lease
// expire, then try to commit anyway), the shape of sched/queue_test.go's
// TestExactlyOnceUnderChaos, except that lease expiry here comes from the
// SAME nemesis-disrupted KV the raw KV workload is hammering, reached
// through the production client stack (sched/kvadapter over kv/client), and
// the queue runs with its production ScanLimit.
//
// What check() asserts, after the run has healed:
//
//  1. Liveness: every job id a Submit returned reaches DONE within
//     defaultDrainBudget (30s), with zombie-free drain workers and the reaper running.
//     DONE is the ONLY allowed terminal state for those jobs: MaxAttempts
//     is set out of reach on purpose, because the attempt budget is a
//     policy (covered by sched's own tests), and with 30% zombie claims a
//     production budget of 3 would legitimately FAIL jobs and blur this
//     check.
//  2. Exactly-once commit: no job has more than one accepted Complete, and
//     a DONE job with one accepted Complete holds exactly that call's
//     result and gen.
//  3. Idempotency: every Submit of one key returned the same id, and at
//     most one record carrying that key's payload ever became runnable
//     (PENDING/RUNNING/DONE). Records an idempotent Submit staged and never
//     published (state UNSPECIFIED) are allowed: they can never run, and
//     they only turn FAILED after sched's stagedTTL (60s), longer than any
//     run here.
//
// The non-vacuity counters (fenced zombie commits, duplicate submits) are
// reported; TestManySeeds requires them to be non-zero across its battery.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	schedv1 "dsys/gen/sched/v1"
	"dsys/sched"
)

// defaultDrainBudget bounds how long, after the run heals, every submitted job has
// to reach DONE.
const defaultDrainBudget = 30 * time.Second

type schedWorkload struct {
	q      *sched.Queue
	faults injectedFaults
	pl     *panicLog

	reaperStop  context.CancelFunc
	drainBudget time.Duration

	mu          sync.Mutex
	completions map[uint64][]string        // accepted Complete results, per job id
	accepted    map[uint64]bool            // every id a Submit returned
	idemIDs     map[string]map[uint64]bool // idempotency key -> ids its Submits returned
	idemCalls   map[string]int             // Submit calls made per key (for the injected bug)

	submitted    atomic.Int64 // successful Submit calls
	idemKeys     atomic.Int64 // distinct idempotency keys used
	idemDupCalls atomic.Int64 // Submit calls that repeated an already-used key
	done         atomic.Int64
	fenced       atomic.Int64
	zombieClaims atomic.Int64
}

func newSchedWorkload(kv sched.KV, faults injectedFaults, drain time.Duration, pl *panicLog) *schedWorkload {
	if drain <= 0 {
		drain = defaultDrainBudget
	}
	q := sched.New(kv, sched.Options{
		Prefix:       "chaos-sched",
		DefaultLease: 600 * time.Millisecond,
		// Out of reach on purpose: see the file comment (DONE is the only
		// allowed terminal state for a submitted job).
		MaxAttempts: 1_000_000,
		// ScanLimit left at 0: the production default (256).
	})
	return &schedWorkload{
		q: q, faults: faults, pl: pl, drainBudget: drain,
		completions: map[uint64][]string{},
		accepted:    map[uint64]bool{},
		idemIDs:     map[string]map[uint64]bool{},
		idemCalls:   map[string]int{},
	}
}

// start launches producers, workers and the reaper. Producers and workers
// stop with runCtx; the reaper keeps going until check() has finished, so
// expired leases keep being reclaimed during the drain.
func (w *schedWorkload) start(ctx, runCtx context.Context, seed int64, producers, workers int, wg *sync.WaitGroup) {
	reaperCtx, cancel := context.WithCancel(ctx)
	w.reaperStop = cancel
	w.pl.goWG(nil, "sched-reaper", func() { sched.RunReaper(reaperCtx, w.q, "chaos-reaper", 200*time.Millisecond) })
	w.runProducers(runCtx, seed, producers, wg)
	w.runWorkers(runCtx, seed+1, workers, 0.3, "chaos-worker", wg)
}

func (w *schedWorkload) recordSubmit(key string, id uint64) {
	w.submitted.Add(1)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.accepted[id] = true
	if key != "" {
		if w.idemIDs[key] == nil {
			w.idemIDs[key] = map[uint64]bool{}
		}
		w.idemIDs[key][id] = true
	}
}

// submitKeyed submits payload under key. It is called several times per key
// (concurrently and sequentially) to model a client retrying a Submit whose
// outcome it did not see.
func (w *schedWorkload) submitKeyed(ctx context.Context, key string) {
	w.mu.Lock()
	w.idemCalls[key]++
	first := w.idemCalls[key] == 1
	w.mu.Unlock()
	if first {
		w.idemKeys.Add(1)
	} else {
		w.idemDupCalls.Add(1)
	}
	sendKey := key
	if !first && w.faults.schedForgetIdemKeyOnRetry {
		sendKey = "" // injected bug: a client library that drops the key when it retries
	}
	octx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	id, err := w.q.Submit(octx, []byte("idem|"+key), sendKey)
	if err == nil {
		w.recordSubmit(key, id)
	}
}

// runProducers submits jobs until ctx is done. About 30% are idempotent:
// each such key is submitted once, raced by a concurrent duplicate, and
// then retried sequentially once more, as a client that lost the reply
// would.
func (w *schedWorkload) runProducers(ctx context.Context, seed int64, n int, wg *sync.WaitGroup) {
	for p := 0; p < n; p++ {
		w.pl.goWG(wg, fmt.Sprintf("sched-producer-%d", p), func() {
			r := rand.New(rand.NewSource(seed + int64(p)))
			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if r.Float64() < 0.3 {
					key := fmt.Sprintf("p%d-%d", p, i)
					w.pl.goWG(wg, "sched-dup-submit", func() { w.submitKeyed(ctx, key) })
					w.submitKeyed(ctx, key)
					w.submitKeyed(ctx, key) // the "retry"
				} else {
					octx, cancel := context.WithTimeout(ctx, 2*time.Second)
					id, err := w.q.Submit(octx, []byte(fmt.Sprintf("plain|p%d-%d", p, i)), "")
					cancel()
					if err == nil {
						w.recordSubmit("", id)
					}
				}
				time.Sleep(time.Duration(5+r.Intn(15)) * time.Millisecond)
			}
		})
	}
}

// runWorkers claims and (mostly) completes jobs until ctx is done. A
// zombieRate fraction of claims "go zombie": they let the lease lapse (by
// sleeping past it without heartbeating) and then try to commit anyway,
// which the store must refuse.
func (w *schedWorkload) runWorkers(ctx context.Context, seed int64, n int, zombieRate float64, prefix string, wg *sync.WaitGroup) {
	for wi := 0; wi < n; wi++ {
		w.pl.goWG(wg, fmt.Sprintf("%s-%d", prefix, wi), func() {
			r := rand.New(rand.NewSource(seed + int64(wi) + 9000))
			name := fmt.Sprintf("%s-%d", prefix, wi)
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
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if w.faults.schedLoseEveryFifthJob && job.Id%5 == 0 {
					continue // injected bug: the worker silently drops the job (never completes, never fails it)
				}
				if r.Float64() < zombieRate {
					// Go zombie: sleep well past the 600ms lease, no
					// heartbeat, then still try to commit.
					w.zombieClaims.Add(1)
					time.Sleep(time.Duration(700+r.Intn(600)) * time.Millisecond)
				}
				result := fmt.Sprintf("%s/gen%d", name, job.Gen)
				octx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err = w.q.Complete(octx, job.Id, job.Gen, []byte(result))
				cancel()
				switch {
				case err == nil:
					w.mu.Lock()
					w.completions[job.Id] = append(w.completions[job.Id], result)
					w.mu.Unlock()
					w.done.Add(1)
				case errors.Is(err, sched.ErrFenced):
					w.fenced.Add(1)
				}
			}
		})
	}
}

// check drains the queue (bounded by w.drainBudget) and then verifies the
// three properties in the file comment. Call it only after the run's
// producers and workers have stopped and the KV is reachable again.
func (w *schedWorkload) check(ctx context.Context, nWorkers int, logf func(format string, args ...any)) (violations []string) {
	defer w.reaperStop()

	// ---- 1. Liveness: drain with zombie-free workers. --------------------
	w.mu.Lock()
	want := make([]uint64, 0, len(w.accepted))
	for id := range w.accepted {
		want = append(want, id)
	}
	w.mu.Unlock()
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

	drainCtx, stopDrain := context.WithTimeout(ctx, w.drainBudget)
	var dwg sync.WaitGroup
	w.runWorkers(drainCtx, 77, nWorkers, 0, "chaos-drain", &dwg)
	drainStart := time.Now()
	remaining := want
	for len(remaining) > 0 && drainCtx.Err() == nil {
		var still []uint64
		for _, id := range remaining {
			sctx, cancel := context.WithTimeout(drainCtx, 2*time.Second)
			job, err := w.q.Status(sctx, id)
			cancel()
			if err != nil || job.State != schedv1.State_STATE_DONE {
				still = append(still, id)
			}
		}
		remaining = still
		if len(remaining) > 0 {
			select {
			case <-drainCtx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	stopDrain()
	dwg.Wait()
	if len(remaining) == 0 {
		logf("scheduler: all %d submitted jobs DONE %s after the run", len(want), time.Since(drainStart).Round(time.Millisecond))
	} else {
		var ex []string
		for _, id := range remaining[:min(5, len(remaining))] {
			sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			job, err := w.q.Status(sctx, id)
			cancel()
			if err != nil {
				ex = append(ex, fmt.Sprintf("%d: %v", id, err))
			} else {
				ex = append(ex, fmt.Sprintf("%d: %s attempts=%d gen=%d", id, job.State, job.Attempts, job.Gen))
			}
		}
		violations = append(violations, fmt.Sprintf(
			"scheduler: %d/%d submitted jobs not DONE within %s of the run healing (liveness): %s",
			len(remaining), len(want), w.drainBudget, strings.Join(ex, "; ")))
	}

	// ---- 2 & 3. Scan every record. ---------------------------------------
	_, tail, err := w.q.Stats(ctx)
	if err != nil {
		return append(violations, fmt.Sprintf("scheduler: could not read stats: %v", err))
	}
	runnableByKey := map[string][]uint64{}
	w.mu.Lock()
	defer w.mu.Unlock()
	// A staged record can sit just past tail (allocate writes it before
	// anything covers it), so keep reading past tail until the first gap.
	for id := uint64(1); ; id++ {
		job, err := w.q.Status(ctx, id)
		if errors.Is(err, sched.ErrNotFound) {
			if id > tail {
				break
			}
			continue
		}
		if err != nil {
			violations = append(violations, fmt.Sprintf("scheduler: job %d: %v", id, err))
			if id > tail {
				break
			}
			continue
		}
		cs := w.completions[id]
		if len(cs) > 1 {
			violations = append(violations, fmt.Sprintf("scheduler: job %d committed %d times (%v): exactly-once is broken", id, len(cs), cs))
		}
		if len(cs) == 1 && job.State == schedv1.State_STATE_DONE {
			if got := string(job.Result); got != cs[0] {
				violations = append(violations, fmt.Sprintf("scheduler: job %d: the accepted Complete wrote %q but the record holds %q", id, cs[0], got))
			}
		}
		if job.State == schedv1.State_STATE_FAILED && w.accepted[id] {
			violations = append(violations, fmt.Sprintf("scheduler: submitted job %d FAILED (%s); DONE is the only allowed terminal state here", id, job.LastError))
		}
		if key, ok := strings.CutPrefix(string(job.Payload), "idem|"); ok {
			staged := job.State == schedv1.State_STATE_UNSPECIFIED ||
				(job.State == schedv1.State_STATE_FAILED && strings.Contains(job.LastError, "abandoned"))
			if !staged {
				runnableByKey[key] = append(runnableByKey[key], id)
			}
		}
	}
	for key, ids := range w.idemIDs {
		if len(ids) > 1 {
			violations = append(violations, fmt.Sprintf("scheduler: idempotency key %q: its Submits returned %d different ids %v", key, len(ids), keys(ids)))
		}
	}
	for key, ids := range runnableByKey {
		if len(ids) > 1 {
			violations = append(violations, fmt.Sprintf("scheduler: idempotency key %q produced %d runnable jobs %v: exactly one allowed", key, len(ids), ids))
			continue
		}
		for got := range w.idemIDs[key] {
			if got != ids[0] {
				violations = append(violations, fmt.Sprintf("scheduler: idempotency key %q: Submit returned id %d but the runnable job is %d", key, got, ids[0]))
			}
		}
	}
	for key, ids := range w.idemIDs {
		if len(runnableByKey[key]) == 0 {
			violations = append(violations, fmt.Sprintf("scheduler: idempotency key %q: Submit returned %v but no runnable job carries it", key, keys(ids)))
		}
	}
	return violations
}

func keys(m map[uint64]bool) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
