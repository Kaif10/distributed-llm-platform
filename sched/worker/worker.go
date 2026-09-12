// Package worker is the client-side half of the sched job queue: a pull-model
// worker that claims jobs, keeps their leases alive, runs a Handler, and
// commits the outcome, all while respecting the fencing token.
//
// Read sched/api.go's package doc first. This package exists to honour its
// three rules on the worker side:
//
//   - Leases (liveness). A claimed job is held under a lease that expires. A
//     heartbeat goroutine renews it every Lease/3 for as long as the handler
//     runs, so a live worker keeps its job and a dead one loses it.
//
//   - Fencing (safety). Every Heartbeat, Complete and Fail carries the gen the
//     worker was given at Claim. If the scheduler answers FailedPrecondition
//     the job has changed hands: this worker is a zombie. It cancels the
//     handler's context, discards whatever the handler produces, and NEVER
//     retries the write with the same or a fresh gen. A retry could not
//     succeed (the CAS on the KV would refuse it) and re-Claiming to "fix" a
//     failed heartbeat would hand the same job to the same process under a
//     new gen while the old attempt is still running.
//
//   - Effectively-once (the handler author's problem). The scheduler
//     guarantees Complete succeeds for at most one gen per job, so the
//     RESULT is recorded once. It does not, and cannot, guarantee the handler
//     EXECUTES once: a worker paused past its lease will run the job to the
//     end even though its Complete will be refused, and the next holder runs
//     it again. Handlers whose side effects must not repeat must therefore
//     be idempotent, keyed by job.Id, exactly as sched/api.go's
//     "Effectively-once" section says. This library cannot do that for you.
//
// Transient trouble (Unavailable, DeadlineExceeded) on any RPC is retried
// with backoff against the SAME (id, gen). A stuck heartbeat that outlives
// the lease simply turns into a fenced heartbeat on the next attempt, which
// is the correct outcome: the job was reaped, and the worker finds out.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	schedv1 "dsys/gen/sched/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler runs one job. ctx is cancelled when the worker is stopping OR when
// the worker has been fenced off the job; in both cases the return value is
// discarded, so a handler that respects ctx wastes the least work. The
// returned result is stored in the job record by Complete; a non-nil err is
// reported via Fail (which requeues the job or, after MaxAttempts, fails it).
//
// A handler may run more than once for the same job (see the package doc).
// Make side effects idempotent on job.Id.
type Handler func(ctx context.Context, job *schedv1.Job) (result []byte, err error)

// Options configures a Worker. The zero value is usable.
type Options struct {
	// Name is the worker id sent in Claim and recorded on the job. Default
	// "<hostname>-<pid>".
	Name string
	// Lease is the lease requested at Claim and on every Heartbeat.
	// Heartbeats are sent every Lease/3. Default 10s.
	Lease time.Duration
	// Concurrency is how many jobs run in parallel; each slot is its own
	// claim/run loop. Default 1.
	Concurrency int
	// PollInterval is the initial backoff when Claim finds nothing runnable.
	// It doubles on each empty poll up to 2s and resets on a successful
	// claim. Default 200ms.
	PollInterval time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger

	// SuppressHeartbeat makes the worker claim jobs and then never renew
	// their leases. It exists ONLY for the "pause a worker past its lease"
	// experiment in cmd/worker (a handler that outlives its lease with no
	// heartbeats gets fenced at Complete). Never set it in real use.
	SuppressHeartbeat bool
}

// Stats is a snapshot of the worker's counters.
type Stats struct {
	// Claimed: jobs handed to this worker by Claim.
	Claimed uint64
	// Completed: jobs whose Complete was accepted.
	Completed uint64
	// Failed: jobs whose Fail was accepted (handler error or panic).
	Failed uint64
	// Fenced: jobs this worker lost while holding them, detected either by a
	// heartbeat (handler cancelled mid-run) or by a refused Complete/Fail
	// (the zombie case: work finished, result thrown away).
	Fenced uint64
}

// Worker pulls jobs from a scheduler and runs them through a Handler.
type Worker struct {
	client schedv1.SchedulerClient
	h      Handler
	opts   Options
	log    *slog.Logger

	claimed, completed, failed, fenced atomic.Uint64
}

const (
	maxPollInterval = 2 * time.Second
	// attemptTimeout bounds a single RPC attempt so a hung scheduler
	// surfaces as DeadlineExceeded and is retried, rather than hanging the
	// heartbeat loop past the lease.
	attemptTimeout = 2 * time.Second
	// reportTimeout bounds the Complete/Fail retry budget once the handler
	// has returned. It is decoupled from the run context so a worker that
	// is shutting down still gets to report the jobs it finished.
	reportTimeout = 10 * time.Second
	backoffMin    = 20 * time.Millisecond
	backoffMax    = 500 * time.Millisecond
)

// errFenced is the cancellation cause attached to a handler context when a
// heartbeat learns the job has been reassigned.
var errFenced = errors.New("worker: fenced: job reassigned while running")

// New builds a Worker. client is any schedv1.SchedulerClient (typically
// schedv1.NewSchedulerClient(conn)).
func New(client schedv1.SchedulerClient, h Handler, opts Options) *Worker {
	if opts.Name == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "worker"
		}
		opts.Name = host + "-" + strconv.Itoa(os.Getpid())
	}
	if opts.Lease <= 0 {
		opts.Lease = 10 * time.Second
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 200 * time.Millisecond
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Worker{
		client: client,
		h:      h,
		opts:   opts,
		log:    opts.Logger.With("worker", opts.Name),
	}
}

// Stats returns a snapshot of the counters.
func (w *Worker) Stats() Stats {
	return Stats{
		Claimed:   w.claimed.Load(),
		Completed: w.completed.Load(),
		Failed:    w.failed.Load(),
		Fenced:    w.fenced.Load(),
	}
}

// Run claims and processes jobs until ctx is done, then waits for in-flight
// handlers to return and their outcomes to be reported, and returns nil.
// Handlers see ctx cancellation through their own context; a handler that
// returns an error because of it has its job Fail-ed (requeued) so the job
// is picked up again promptly instead of waiting out the lease.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < w.opts.Concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			w.loop(ctx, slot)
		}(i)
	}
	wg.Wait()
	return nil
}

// loop is one claim slot: claim, run, repeat, with exponential backoff while
// the queue is empty.
func (w *Worker) loop(ctx context.Context, slot int) {
	log := w.log.With("slot", slot)
	backoff := w.opts.PollInterval
	for ctx.Err() == nil {
		var resp *schedv1.ClaimResponse
		err := w.rpc(ctx, func(c context.Context) error {
			var err error
			resp, err = w.client.Claim(c, &schedv1.ClaimRequest{
				Worker:  w.opts.Name,
				LeaseMs: w.opts.Lease.Milliseconds(),
			})
			return err
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Non-transient claim failure (misconfiguration, Internal, ...).
			// Nothing to fence here: we hold no job. Log and keep polling.
			log.Error("claim failed", "err", err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxPollInterval)
			continue
		}
		if !resp.GetFound() {
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxPollInterval)
			continue
		}
		backoff = w.opts.PollInterval
		w.claimed.Add(1)
		w.runJob(ctx, log, resp.GetJob())
	}
}

// runJob owns one claimed job from Claim to Complete/Fail. The invariants,
// in the order they matter:
//
//  1. The handler runs under a child context. A heartbeat goroutine renews
//     the lease every Lease/3 with the (id, gen) from Claim.
//  2. If a Heartbeat returns FailedPrecondition the job is no longer ours.
//     The handler's context is cancelled IMMEDIATELY: any further work would
//     produce a result the scheduler will refuse, so it is pure waste. The
//     handler's eventual return value is discarded and the job is counted as
//     fenced.
//  3. On handler success, Complete(id, gen, result). If that returns
//     FailedPrecondition we are a zombie: we did the work, but the lease
//     lapsed (a pause, a partition, a slow heartbeat) and someone else now
//     owns the job. Log at Warn, count fenced, and MOVE ON. Do not retry
//     Complete (the CAS would refuse it again), do not re-Claim, do not
//     crash. This one branch is what keeps a stale worker from ever
//     overwriting a live one's result.
//  4. On handler error, Fail(id, gen, err); identical fenced handling.
//  5. Transient RPC errors are retried against the same (id, gen).
func (w *Worker) runJob(ctx context.Context, log *slog.Logger, job *schedv1.Job) {
	id, gen := job.GetId(), job.GetGen()
	log = log.With("job", id, "gen", gen)

	hctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// Heartbeat goroutine. It stops when hctx is cancelled, which happens
	// when the handler returns (below), when the worker is stopping, or
	// when it fences itself.
	var fencedByHeartbeat atomic.Bool
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		interval := w.opts.Lease / 3
		if interval <= 0 {
			interval = time.Millisecond
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-hctx.Done():
				return
			case <-t.C:
			}
			if w.opts.SuppressHeartbeat {
				continue // experiment mode: let the lease lapse on purpose
			}
			err := w.rpc(hctx, func(c context.Context) error {
				_, err := w.client.Heartbeat(c, &schedv1.HeartbeatRequest{
					Id: id, Gen: gen, LeaseMs: w.opts.Lease.Milliseconds(),
				})
				return err
			})
			switch {
			case err == nil:
			case hctx.Err() != nil:
				return // handler finished or worker stopping; not our problem
			case isFenced(err):
				// Rule 2: we lost the job. Stop the handler now.
				fencedByHeartbeat.Store(true)
				log.Warn("fenced by heartbeat: job reassigned, cancelling handler", "err", status.Convert(err).Message())
				cancel(errFenced)
				return
			default:
				// A hard, non-transient error other than fencing (NotFound,
				// Internal...). Keep the handler going and try again next
				// tick; if the lease really lapses the next answer is
				// FailedPrecondition and we take the branch above.
				log.Error("heartbeat failed", "err", err)
			}
		}
	}()

	result, herr := w.callHandler(hctx, job)
	cancel(nil) // stop heartbeating; the outcome is decided
	<-hbDone

	if fencedByHeartbeat.Load() {
		w.fenced.Add(1)
		log.Info("discarding handler result: fenced mid-run", "handler_err", herr)
		return
	}

	// Reporting uses a context detached from ctx's cancellation (but not
	// its values) so a worker that is shutting down can still record the
	// outcome of the job it just finished, bounded by reportTimeout.
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer rcancel()

	if herr == nil {
		err := w.rpc(rctx, func(c context.Context) error {
			_, err := w.client.Complete(c, &schedv1.CompleteRequest{Id: id, Gen: gen, Result: result})
			return err
		})
		switch {
		case err == nil:
			w.completed.Add(1)
			log.Debug("completed")
		case isFenced(err):
			// Rule 3: the zombie case. Work done, result refused. Move on.
			w.fenced.Add(1)
			log.Warn("zombie: Complete refused, job was reassigned while we ran it; result discarded",
				"err", status.Convert(err).Message())
		default:
			log.Error("complete failed", "err", err)
		}
		return
	}

	err := w.rpc(rctx, func(c context.Context) error {
		_, err := w.client.Fail(c, &schedv1.FailRequest{Id: id, Gen: gen, Error: herr.Error()})
		return err
	})
	switch {
	case err == nil:
		w.failed.Add(1)
		log.Info("failed", "handler_err", herr)
	case isFenced(err):
		// Rule 4: same as Complete. Our failure report is stale too.
		w.fenced.Add(1)
		log.Warn("zombie: Fail refused, job was reassigned while we ran it",
			"handler_err", herr, "err", status.Convert(err).Message())
	default:
		log.Error("fail report failed", "err", err, "handler_err", herr)
	}
}

// callHandler runs the handler and converts a panic into an error so one
// bad job cannot take the worker down; the job is reported via Fail and
// the loop continues.
func (w *Worker) callHandler(ctx context.Context, job *schedv1.Job) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("handler panic", "job", job.GetId(), "panic", r, "stack", string(debug.Stack()))
			result, err = nil, fmt.Errorf("handler panic: %v", r)
		}
	}()
	return w.h(ctx, job)
}

// rpc runs fn with a per-attempt timeout, retrying transient failures
// (Unavailable, DeadlineExceeded) with exponential backoff until ctx is
// done. fn must close over a request built once, so every attempt presents
// the same (id, gen): a retry is the same logical write, never a new claim.
func (w *Worker) rpc(ctx context.Context, fn func(ctx context.Context) error) error {
	backoff := backoffMin
	for {
		actx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := fn(actx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || !isTransient(err) {
			return err
		}
		if !sleepCtx(ctx, backoff) {
			return err
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// isFenced reports whether err is the scheduler saying "your gen is stale
// or the job is terminal": FailedPrecondition. Either way the job is no
// longer ours to write.
func isFenced(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

// isTransient reports whether err is worth retrying with the same request.
func isTransient(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// sleepCtx sleeps for d or until ctx is done; false means ctx won.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
