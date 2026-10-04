package sched

// Queue: the job queue's core logic. Every method is a small state machine
// over records in the KV, driven entirely by read-then-CAS. Read this file
// with one question in mind for each CAS: "what does it mean if this CAS
// fails, and is that outcome safe?" — the answer is always "someone else
// moved first, re-read and reconsider", never "corrupt state".
//
// # Key layout (all under Options.Prefix)
//
//	/tail        uint64  number of ids ever assigned; job ids are 1..tail
//	/head        uint64  lowest id that may still be non-terminal
//	/job/<id>    Job     the record; <id> is zero-padded so keys sort
//	/idem/<key>  id or placeholder, for Submit idempotency keys
//	/cursor      uint64  where Claim's second scan window resumes (a hint)
//	/leader      leader lease for the reaper (leader.go)
//
// # The scan
//
// Claim and Reap walk ids from head toward tail, reading one record per
// step. There is no secondary index of "pending jobs", so a Claim costs
// O(distance to the first runnable job) reads, each a linearizable read
// through the KV. head is advanced lazily past terminal jobs by whoever
// notices, which keeps the scan short in steady state: a healthy queue's
// prefix is DONE jobs (skipped by head) followed by RUNNING ones (skipped by
// the scan) followed by PENDING ones (claimed). ScanLimit bounds the worst
// case per window; because one long-running job pins head, Claim also scans
// a second window from a rotating cursor so the queue behind a pinned head
// stays reachable (see Claim). The indexed alternative (a per-state list, or a pending counter
// per worker pool) is the natural exercise; it trades this simplicity for a
// second key that must be kept consistent with the record.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	schedv1 "dsys/gen/sched/v1"
	"dsys/obs"
)

// tracer is package-scoped so every Queue method shares one Tracer instance;
// it is backed by whatever TracerProvider obs.InitTracing installed (or the
// global no-op default if tracing was never initialised), so these spans are
// free to leave in place in tests and binaries that don't pass
// -otlp-endpoint.
var tracer = obs.Tracer("sched")

// endSpan records err on span (if non-nil) before ending it. A nil err
// leaves the span's default (unset) status, matching the OTel convention
// that "unset" means success.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	span.End()
}

// Queue is safe for concurrent use; it holds no mutable state of its own.
type Queue struct {
	kv   KV
	opts Options
}

// New returns a Queue over kv. Several Queues (in several processes) over
// the same kv and prefix are the same queue.
func New(kv KV, opts Options) *Queue {
	return &Queue{kv: kv, opts: opts.withDefaults()}
}

// Options returns the effective options.
func (q *Queue) Options() Options { return q.opts }

// ---------------------------------------------------------------------------
// keys and encodings
// ---------------------------------------------------------------------------

func (q *Queue) keyTail() string         { return q.opts.Prefix + "/tail" }
func (q *Queue) keyHead() string         { return q.opts.Prefix + "/head" }
func (q *Queue) keyJob(id uint64) string { return fmt.Sprintf("%s/job/%020d", q.opts.Prefix, id) }
func (q *Queue) keyIdem(k string) string { return q.opts.Prefix + "/idem/" + k }
func (q *Queue) keyLeader() string       { return q.opts.Prefix + "/leader" }
func (q *Queue) keyCursor() string       { return q.opts.Prefix + "/cursor" }
func (q *Queue) nowMs() int64            { return q.opts.Clock().UnixMilli() }
func (q *Queue) leaseOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return q.opts.DefaultLease
	}
	return d
}

func encU64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func decU64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// readCounter returns a counter's value and the raw bytes to CAS against.
// An absent counter reads as def with nil raw (CAS with expectAbsent).
func (q *Queue) readCounter(ctx context.Context, key string, def uint64) (val uint64, raw []byte, err error) {
	b, found, err := q.kv.Get(ctx, key)
	if err != nil {
		return 0, nil, err
	}
	if !found {
		return def, nil, nil
	}
	return decU64(b), b, nil
}

// casCounter moves a counter from the raw value we read to v.
func (q *Queue) casCounter(ctx context.Context, key string, raw []byte, v uint64) (bool, error) {
	swapped, _, err := q.kv.CAS(ctx, key, raw, raw == nil, encU64(v))
	return swapped, err
}

// readJob returns the decoded record and the raw bytes to CAS against.
func (q *Queue) readJob(ctx context.Context, id uint64) (*schedv1.Job, []byte, error) {
	raw, found, err := q.kv.Get(ctx, q.keyJob(id))
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, ErrNotFound
	}
	var j schedv1.Job
	if err := proto.Unmarshal(raw, &j); err != nil {
		return nil, nil, fmt.Errorf("sched: corrupt job %d: %w", id, err)
	}
	return &j, raw, nil
}

// casJob replaces a record. expected is the raw bytes we read (never a
// re-marshal of the decoded record: the comparison must be against exactly
// what is stored, and marshaling is not guaranteed byte-stable across
// producers).
func (q *Queue) casJob(ctx context.Context, id uint64, expected []byte, next *schedv1.Job) (bool, error) {
	b, err := proto.Marshal(next)
	if err != nil {
		return false, err
	}
	swapped, _, err := q.kv.CAS(ctx, q.keyJob(id), expected, expected == nil, b)
	return swapped, err
}

func terminal(s schedv1.State) bool {
	return s == schedv1.State_STATE_DONE || s == schedv1.State_STATE_FAILED
}

// ---------------------------------------------------------------------------
// Submit
// ---------------------------------------------------------------------------

// Submit enqueues payload and returns its id. With a non-empty
// idempotencyKey, a repeat Submit returns the original id.
//
// Order of writes matters for crash safety. The record is written FIRST
// (CAS expect-absent at tail+1) and tail is bumped SECOND. A crash between
// the two leaves a record at tail+1 that tail does not yet cover; the next
// Submit's expect-absent CAS at that id fails, sees a real record there,
// repairs tail past it, and moves on. The reverse order (bump tail, then
// write) would instead leave a permanent hole that every scan has to step
// over, with no way to tell "not written yet" from "never will be".
func (q *Queue) Submit(ctx context.Context, payload []byte, idempotencyKey string) (id uint64, err error) {
	ctx, span := tracer.Start(ctx, "sched.Submit", trace.WithAttributes(
		attribute.Bool("idempotent", idempotencyKey != ""),
	))
	defer func() { endSpan(span, err) }()

	var release func(uint64) error
	if idempotencyKey != "" {
		id, rel, err := q.claimIdem(ctx, idempotencyKey)
		if err != nil {
			return 0, err
		}
		if rel == nil {
			return id, nil // already submitted under this key
		}
		release = rel
	}

	for {
		tail, tailRaw, err := q.readCounter(ctx, q.keyTail(), 0)
		if err != nil {
			return 0, err
		}
		head, _, err := q.readCounter(ctx, q.keyHead(), 1)
		if err != nil {
			return 0, err
		}
		// live ids are head..tail; refuse once that window is full.
		if tail >= head && tail-head+1 >= q.opts.MaxQueue {
			return 0, ErrQueueFull
		}

		id := tail + 1
		job := &schedv1.Job{
			Id:          id,
			Payload:     payload,
			State:       schedv1.State_STATE_PENDING,
			SubmittedMs: q.nowMs(),
		}
		swapped, err := q.casJob(ctx, id, nil, job)
		if err != nil {
			return 0, err
		}
		if !swapped {
			// Someone already wrote id: a concurrent Submit that won, or a
			// Submit that crashed after writing and before bumping tail.
			// Either way tail should be at least id; repair and retry.
			_, _ = q.casCounter(ctx, q.keyTail(), tailRaw, id)
			continue
		}
		// Bump tail to cover our id. If this CAS loses, someone else has
		// already moved tail at least as far (repair path above), so re-read
		// and only push if it is still behind.
		if ok, err := q.casCounter(ctx, q.keyTail(), tailRaw, id); err != nil {
			return 0, err
		} else if !ok {
			for {
				t, raw, err := q.readCounter(ctx, q.keyTail(), 0)
				if err != nil {
					return 0, err
				}
				if t >= id {
					break
				}
				if ok, err := q.casCounter(ctx, q.keyTail(), raw, id); err != nil {
					return 0, err
				} else if ok {
					break
				}
			}
		}
		if release != nil {
			if err := release(id); err != nil {
				return 0, err
			}
		}
		return id, nil
	}
}

// Idempotency keys use the lease trick a second time: the key is first
// reserved with a placeholder that expires, so a Submit that crashes after
// reserving does not block the key forever, and only then bound to an id.
const idemPlaceholderTTL = 30 * time.Second

// claimIdem returns (id, nil, nil) if key is already bound, or
// (0, release, nil) if the caller now holds the reservation and must call
// release(id) after allocating.
func (q *Queue) claimIdem(ctx context.Context, key string) (uint64, func(uint64) error, error) {
	k := q.keyIdem(key)
	for {
		raw, found, err := q.kv.Get(ctx, k)
		if err != nil {
			return 0, nil, err
		}
		if found {
			s := string(raw)
			if id, ok := strings.CutPrefix(s, "id|"); ok {
				n, _ := strconv.ParseUint(id, 10, 64)
				return n, nil, nil
			}
			if until, ok := strings.CutPrefix(s, "pending|"); ok {
				u, _ := strconv.ParseInt(until, 10, 64)
				if u > q.nowMs() {
					// Another Submit with this key is in flight; wait for it.
					select {
					case <-ctx.Done():
						return 0, nil, ctx.Err()
					case <-time.After(20 * time.Millisecond):
					}
					continue
				}
				// Stale reservation: its owner died. Steal it.
			}
		}
		mine := []byte("pending|" + strconv.FormatInt(q.nowMs()+idemPlaceholderTTL.Milliseconds(), 10))
		swapped, _, err := q.kv.CAS(ctx, k, raw, !found, mine)
		if err != nil {
			return 0, nil, err
		}
		if !swapped {
			continue
		}
		release := func(id uint64) error {
			// Bind the key. If our placeholder was stolen meanwhile (we were
			// slower than the TTL), the thief's Submit also produced a job;
			// two jobs for one key is the documented failure mode of an
			// expired reservation, and preferable to a stuck key.
			_, _, err := q.kv.CAS(ctx, k, mine, false, []byte("id|"+strconv.FormatUint(id, 10)))
			return err
		}
		return 0, release, nil
	}
}

// ---------------------------------------------------------------------------
// Claim
// ---------------------------------------------------------------------------

// Claim hands the caller a runnable job under a fresh lease, or ErrNoJob.
// Runnable means PENDING, or RUNNING with an expired lease (the claimer
// reaps it in passing; it does not have to wait for the reaper).
//
// # Two windows, and why one is not enough
//
// head only moves past a terminal PREFIX, so a single long-running job
// (heartbeating, perfectly healthy) pins head for as long as it runs. Every
// job submitted behind it completes, but stays inside [head, tail] as a DONE
// record. A scan that only ever started at head would spend its whole
// ScanLimit budget re-reading the pinned job and those DONE records, and
// once more than ScanLimit of them pile up, every job past head+ScanLimit is
// unreachable: Claim returns ErrNoJob forever with runnable work queued. The
// same happens with ScanLimit live leases at the front of the queue.
//
// So Claim scans two windows of at most ScanLimit ids each:
//
//  1. [head, head+ScanLimit): oldest first, exactly as before. This is
//     where requeued jobs (Fail, an expired lease) and the oldest PENDING
//     work are, and it is the only place head is advanced.
//  2. Only if window 1 found nothing and did not reach tail: a window
//     starting at the cursor (/cursor), a shared hint that sweeps the rest
//     of [head, tail] ScanLimit ids per Claim and wraps back to the end of
//     window 1 when it passes tail.
//
// The invariant that gives liveness: every id in [head, tail] is examined
// either by every Claim (window 1) or by the sweep, which advances on every
// Claim that reaches window 2 and wraps, so a runnable job anywhere in the
// queue is found within ceil((tail-head)/ScanLimit)+1 consecutive Claims that
// come back empty from window 1. The cursor is ONLY a hint: it is clamped
// into the window on read, updated with a best-effort CAS whose loss is
// ignored, and nothing is ever claimed because of it, only because of the
// record's own CAS. So a stale, lost, or concurrently overwritten cursor can
// cost an extra Claim's worth of latency, never a lost job, a double claim,
// or a fencing violation: exactly-once still rests entirely on casJob.
func (q *Queue) Claim(ctx context.Context, worker string, lease time.Duration) (job *schedv1.Job, err error) {
	ctx, span := tracer.Start(ctx, "sched.Claim", trace.WithAttributes(attribute.String("worker", worker)))
	defer func() {
		// ErrNoJob is the normal "queue empty" outcome, not a failure; don't
		// mark the span as errored for it (mirrors grpcserver's ErrNoJob ->
		// ClaimResponse{Found:false} treatment).
		if errors.Is(err, ErrNoJob) {
			span.End()
			return
		}
		endSpan(span, err)
	}()

	lease = q.leaseOrDefault(lease)
	head, headRaw, err := q.readCounter(ctx, q.keyHead(), 1)
	if err != nil {
		return nil, err
	}
	tail, _, err := q.readCounter(ctx, q.keyTail(), 0)
	if err != nil {
		return nil, err
	}

	// Window 1: from head.
	job, next, err := q.claimFrom(ctx, worker, lease, head, tail, true, head, headRaw)
	if job != nil || err != nil {
		return job, err
	}
	if next > tail {
		return nil, ErrNoJob // window 1 already covered the whole queue
	}
	windowEnd := next

	// Window 2: from the cursor, clamped into (window 1, tail].
	cursor, cursorRaw, err := q.readCounter(ctx, q.keyCursor(), 0)
	if err != nil {
		return nil, err
	}
	start := cursor
	if start < windowEnd || start > tail {
		start = windowEnd
	}
	job, next, err = q.claimFrom(ctx, worker, lease, start, tail, false, 0, nil)
	if err != nil {
		return nil, err
	}
	// Persist where the sweep got to; 0 (past tail) means "wrap", which the
	// clamp above turns into "start right after window 1" next time.
	if next > tail {
		next = 0
	}
	if next != cursor {
		_, _ = q.casCounter(ctx, q.keyCursor(), cursorRaw, next)
	}
	if job != nil {
		return job, nil
	}
	return nil, ErrNoJob
}

// claimFrom scans at most ScanLimit ids starting at from (and not past tail)
// and claims the first runnable one. next is the first id it did not
// examine. With advanceHead, head/headRaw are the head counter as read by
// the caller (headRaw nil if absent) and the scan advances head past a
// terminal record found exactly at head; window 2 never touches head.
func (q *Queue) claimFrom(ctx context.Context, worker string, lease time.Duration, from, tail uint64, advanceHead bool, head uint64, headRaw []byte) (job *schedv1.Job, next uint64, err error) {
	scanned := 0
	id := from
	for ; id <= tail && scanned < q.opts.ScanLimit; id++ {
		scanned++
		job, raw, err := q.readJob(ctx, id)
		if errors.Is(err, ErrNotFound) {
			continue // written-but-not-yet-covered gap, or repaired-over id
		}
		if err != nil {
			return nil, id, err
		}
		now := q.nowMs()

		switch {
		case terminal(job.State):
			// Advance head past a terminal prefix, best effort. Only the
			// exact head may move: id == head with headRaw the bytes we
			// read, otherwise someone else already moved it.
			if advanceHead && id == head {
				if ok, _ := q.casCounter(ctx, q.keyHead(), headRaw, id+1); ok {
					head = id + 1
					headRaw = encU64(head)
				}
			}
			continue

		case job.State == schedv1.State_STATE_PENDING,
			job.State == schedv1.State_STATE_RUNNING && job.LeaseUntilMs < now:
			if job.State == schedv1.State_STATE_RUNNING && job.Attempts >= q.opts.MaxAttempts {
				// Expired for the last time: fail it rather than run again.
				next := proto.Clone(job).(*schedv1.Job)
				next.State = schedv1.State_STATE_FAILED
				next.Gen++
				next.LastError = "lease expired; attempts exhausted"
				next.LeaseUntilMs = 0
				_, _ = q.casJob(ctx, id, raw, next)
				continue
			}
			next := proto.Clone(job).(*schedv1.Job)
			next.State = schedv1.State_STATE_RUNNING
			next.Gen++ // the fence: every previous holder is now stale
			next.Worker = worker
			next.Attempts++
			next.LeaseUntilMs = now + lease.Milliseconds()
			swapped, err := q.casJob(ctx, id, raw, next)
			if err != nil {
				return nil, id, err
			}
			if swapped {
				return next, id + 1, nil
			}
			// Lost the race for this id to another claimer. Move on; it
			// is theirs now and the next PENDING one may be free.
			continue

		default:
			continue // RUNNING under a live lease
		}
	}
	return nil, id, nil
}

// ---------------------------------------------------------------------------
// Worker-side mutations: all fenced by gen.
// ---------------------------------------------------------------------------

// checkHolder validates that the caller, presenting gen, still owns job.
func checkHolder(job *schedv1.Job, gen uint64) error {
	if job.State == schedv1.State_STATE_FAILED {
		return ErrTerminal
	}
	if job.State == schedv1.State_STATE_DONE {
		if job.Gen == gen {
			return nil // our own earlier Complete; caller may treat as success
		}
		return ErrFenced
	}
	if job.State != schedv1.State_STATE_RUNNING || job.Gen != gen {
		return ErrFenced
	}
	return nil
}

// Heartbeat extends the lease and returns the new deadline.
func (q *Queue) Heartbeat(ctx context.Context, id, gen uint64, lease time.Duration) (until time.Time, err error) {
	ctx, span := tracer.Start(ctx, "sched.Heartbeat", trace.WithAttributes(
		attribute.Int64("id", int64(id)), attribute.Int64("gen", int64(gen)),
	))
	defer func() { endSpan(span, err) }()

	lease = q.leaseOrDefault(lease)
	for {
		job, raw, err := q.readJob(ctx, id)
		if err != nil {
			return time.Time{}, err
		}
		if err := checkHolder(job, gen); err != nil {
			return time.Time{}, err
		}
		if job.State == schedv1.State_STATE_DONE {
			return time.Time{}, ErrTerminal
		}
		next := proto.Clone(job).(*schedv1.Job)
		next.LeaseUntilMs = q.nowMs() + lease.Milliseconds()
		swapped, err := q.casJob(ctx, id, raw, next)
		if err != nil {
			return time.Time{}, err
		}
		if swapped {
			return time.UnixMilli(next.LeaseUntilMs), nil
		}
		// Record changed under us: most likely the reaper fenced us. Loop;
		// the re-read will say so.
	}
}

// Complete records result and marks the job DONE. Calling it again with the
// same gen is a no-op success, so a worker whose first call's reply was lost
// can safely retry.
func (q *Queue) Complete(ctx context.Context, id, gen uint64, result []byte) (err error) {
	ctx, span := tracer.Start(ctx, "sched.Complete", trace.WithAttributes(
		attribute.Int64("id", int64(id)), attribute.Int64("gen", int64(gen)),
	))
	defer func() { endSpan(span, err) }()

	for {
		job, raw, err := q.readJob(ctx, id)
		if err != nil {
			return err
		}
		if err := checkHolder(job, gen); err != nil {
			return err
		}
		if job.State == schedv1.State_STATE_DONE {
			return nil // idempotent retry
		}
		next := proto.Clone(job).(*schedv1.Job)
		next.State = schedv1.State_STATE_DONE
		next.Result = result
		next.LeaseUntilMs = 0
		swapped, err := q.casJob(ctx, id, raw, next)
		if err != nil {
			return err
		}
		if swapped {
			return nil
		}
	}
}

// Fail reports a failed attempt. The job is requeued (gen advanced, so the
// caller is fenced from any further writes) unless attempts are exhausted,
// in which case it becomes FAILED.
func (q *Queue) Fail(ctx context.Context, id, gen uint64, reason string) (requeued bool, err error) {
	ctx, span := tracer.Start(ctx, "sched.Fail", trace.WithAttributes(
		attribute.Int64("id", int64(id)), attribute.Int64("gen", int64(gen)), attribute.String("reason", reason),
	))
	defer func() { endSpan(span, err) }()

	for {
		job, raw, err := q.readJob(ctx, id)
		if err != nil {
			return false, err
		}
		if err := checkHolder(job, gen); err != nil {
			return false, err
		}
		if job.State == schedv1.State_STATE_DONE {
			return false, ErrTerminal
		}
		next := proto.Clone(job).(*schedv1.Job)
		next.Gen++
		next.Worker = ""
		next.LeaseUntilMs = 0
		next.LastError = reason
		requeue := job.Attempts < q.opts.MaxAttempts
		if requeue {
			next.State = schedv1.State_STATE_PENDING
		} else {
			next.State = schedv1.State_STATE_FAILED
		}
		swapped, err := q.casJob(ctx, id, raw, next)
		if err != nil {
			return false, err
		}
		if swapped {
			return requeue, nil
		}
	}
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// Status returns the job record.
func (q *Queue) Status(ctx context.Context, id uint64) (*schedv1.Job, error) {
	job, _, err := q.readJob(ctx, id)
	return job, err
}

// Stats returns head and tail.
func (q *Queue) Stats(ctx context.Context) (head, tail uint64, err error) {
	head, _, err = q.readCounter(ctx, q.keyHead(), 1)
	if err != nil {
		return 0, 0, err
	}
	tail, _, err = q.readCounter(ctx, q.keyTail(), 0)
	return head, tail, err
}

// ---------------------------------------------------------------------------
// Reap
// ---------------------------------------------------------------------------

// Reap returns expired RUNNING jobs to PENDING (advancing gen, which is what
// fences their previous holder) or FAILs them if attempts are exhausted,
// and advances head past terminal jobs. It is safe to run from any number
// of processes concurrently: every step is a CAS, and a lost CAS just means
// another reaper (or a claimer) already did that step.
func (q *Queue) Reap(ctx context.Context) (reclaimedOut int, err error) {
	ctx, span := tracer.Start(ctx, "sched.Reap")
	defer func() { endSpan(span, err) }()

	head, headRaw, err := q.readCounter(ctx, q.keyHead(), 1)
	if err != nil {
		return 0, err
	}
	tail, _, err := q.readCounter(ctx, q.keyTail(), 0)
	if err != nil {
		return 0, err
	}
	reclaimed := 0
	scanned := 0
	for id := head; id <= tail && scanned < q.opts.ScanLimit; id++ {
		scanned++
		job, raw, err := q.readJob(ctx, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return reclaimed, err
		}
		if terminal(job.State) {
			if id == head {
				if ok, _ := q.casCounter(ctx, q.keyHead(), headRaw, id+1); ok {
					head = id + 1
					headRaw = encU64(head)
				}
			}
			continue
		}
		if job.State != schedv1.State_STATE_RUNNING || job.LeaseUntilMs >= q.nowMs() {
			continue
		}
		next := proto.Clone(job).(*schedv1.Job)
		next.Gen++
		next.Worker = ""
		next.LeaseUntilMs = 0
		if job.Attempts >= q.opts.MaxAttempts {
			next.State = schedv1.State_STATE_FAILED
			next.LastError = "lease expired; attempts exhausted"
		} else {
			next.State = schedv1.State_STATE_PENDING
			next.LastError = "lease expired"
		}
		if ok, err := q.casJob(ctx, id, raw, next); err != nil {
			return reclaimed, err
		} else if ok {
			reclaimed++
		}
	}
	return reclaimed, nil
}
