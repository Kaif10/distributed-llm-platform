// Package sched is a distributed job queue built entirely out of the KV
// store's compare-and-swap. The scheduler process holds no job state of its
// own: every job, counter, and lease lives in the KV, and every mutation is
// a CAS against the record's previous value. That is what lets any number
// of scheduler replicas serve any request, and what lets a scheduler crash
// at any instant without losing or duplicating a job.
//
// # Leases versus locks, and why a lock is not enough
//
// A worker that claims a job holds it under a lease: permission that
// expires. If the worker dies, or hangs, or is paused by the OS for longer
// than the lease, the reaper hands the job to someone else. A plain lock
// cannot do that: nobody can tell a dead lock-holder from a slow one, so a
// lock held by a dead worker is held forever.
//
// But a lease alone is not safe either. A worker paused past its lease does
// not know it was paused. It wakes up, finishes the job, and writes its
// result, overwriting whatever the new holder did. That is the zombie
// problem, and it is why every worker-side write carries a fencing token:
//
//	gen, the job's generation, increments each time the job changes hands.
//	A worker may only write while presenting the gen it was given at Claim.
//	The KV's CAS rejects any write whose expected record has a stale gen.
//
// Leases give liveness (work does not get stuck). Fencing gives safety (a
// stale holder cannot corrupt anything). Clock skew, GC pauses, and network
// delay can all make a lease expire "wrongly"; none of them can make two
// holders both commit, because only one of them holds the current gen.
//
// # Effectively-once
//
// A job may EXECUTE more than once: a worker can run it to completion, be
// declared dead a moment before it calls Complete, and the next holder runs
// it again. What is exactly-once is the COMMIT: Complete succeeds for at
// most one gen per job, so the recorded result is written once. Handlers
// whose side effects must not repeat therefore need to be idempotent (keyed
// by job id), exactly the way Phase 1's clients needed RequestMeta. There is
// no distributed queue that gives exactly-once execution; anyone who says
// otherwise is describing at-least-once execution plus idempotent effects.
//
// # Admission control
//
// The queue is bounded. Submit is refused with ErrQueueFull once tail-head
// exceeds MaxQueue, and the gRPC layer turns that into ResourceExhausted
// with a Retry-After hint. Shedding load at the door, while the client can
// still do something about it, beats accepting work that will time out.
package sched

import (
	"context"
	"errors"
	"time"
)

// KV is the subset of the store the scheduler needs. Every operation must
// be linearizable: the whole design leans on CAS observing the latest
// committed value. Implemented by an adapter over the kvv1 gRPC client
// (flat raftkv or sharded shardkv, both work) and by an in-memory fake in
// tests.
//
// Implementations retry transient failures internally and must attach a
// stable RequestMeta to each logical mutation so a retry is deduplicated by
// the store rather than applied twice (see kv/store's package doc). A CAS
// that returns swapped=false with err=nil is a normal outcome, not an error:
// someone else got there first, and current is what they wrote.
type KV interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Put(ctx context.Context, key string, value []byte) error
	CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (swapped bool, current []byte, err error)
}

// Errors returned by Queue. The gRPC layer maps them to status codes.
var (
	// ErrQueueFull: admission refused. Retry after backing off.
	ErrQueueFull = errors.New("sched: queue is full")
	// ErrNoJob: nothing runnable right now.
	ErrNoJob = errors.New("sched: no runnable job")
	// ErrFenced: the presented gen is stale; the job belongs to someone
	// else now. Stop working on it and discard any result.
	ErrFenced = errors.New("sched: fenced: job has been reassigned")
	// ErrNotFound: no such job id.
	ErrNotFound = errors.New("sched: job not found")
	// ErrTerminal: the job is already DONE or FAILED.
	ErrTerminal = errors.New("sched: job is already terminal")
)

// Options configures a Queue.
type Options struct {
	// Prefix namespaces every key, e.g. "sched". Two queues with different
	// prefixes share a KV without interfering.
	Prefix string
	// MaxQueue bounds tail-head. 0 means 1024.
	MaxQueue uint64
	// DefaultLease is used when a claimer passes 0. 0 means 10s.
	DefaultLease time.Duration
	// MaxAttempts before a job goes FAILED instead of being requeued.
	// 0 means 3.
	MaxAttempts uint32
	// Clock supplies "now". Tests inject a fake to drive lease expiry
	// without sleeping. nil means time.Now.
	//
	// This is the scheduler's clock, not the KV's: lease expiry is judged
	// by whichever scheduler replica happens to look. Replicas whose clocks
	// disagree will disagree about expiry by that skew, which is a liveness
	// concern (a lease reaped early or late), never a safety one (fencing).
	Clock func() time.Time
	// ScanLimit caps how many job records one Claim or Reap will read while
	// scanning from head. 0 means 256. A scan is O(runnable-distance) reads
	// through the KV; see docs/phase4.md for why this is acceptable here
	// and what the indexed alternative looks like.
	ScanLimit int
}

func (o Options) withDefaults() Options {
	if o.Prefix == "" {
		o.Prefix = "sched"
	}
	if o.MaxQueue == 0 {
		o.MaxQueue = 1024
	}
	if o.DefaultLease <= 0 {
		o.DefaultLease = 10 * time.Second
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = 3
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.ScanLimit <= 0 {
		o.ScanLimit = 256
	}
	return o
}
