// Package chaos is a whole-stack, seed-driven simulation harness. It wires
// a real KV cluster (Raft, over a seeded raft/simnet.Net), a real
// sched.Queue on top of it, and optionally a real gateway.Server with mock
// inference workers, all in one process, then drives them with a nemesis
// (a scheduled sequence of partitions, crashes, and pauses) and a workload
// (concurrent clients), everything derived from one seed.
//
// # What "deterministic" means here, precisely
//
// Two things ARE fully pinned by the seed, independent of timing: what keys
// and values the workload generates (each client goroutine has its own
// seeded rand.Rand, drawn in a fixed order relative to its own iteration
// count), and which RPCs raft/simnet drops, delays, or reorders
// (raft/simnet.NewSeeded). Those are pure functions of the seed and nothing
// else, proven by raft/simnet's own TestSeededFaultsAreReproducible.
//
// The nemesis's choice of event is a DIFFERENT case, and worth being exact
// about, because it looks like it should be just as pinned and is not.
// chooseEvent (nemesis.go) is a pure function of two inputs: a *rand.Rand
// draw, and the cluster's CURRENT state (which nodes are partitioned or
// crashed right now, needed to enforce the majority bound — see chooseEvent's
// doc comment). Given the SAME state and the same rand.Rand position, it
// always picks the same thing: TestChooseEventDeterministicGivenFixedState
// proves exactly that in isolation. But "current state" in a live run
// depends on real goroutine timing — how fast a Pause's auto-heal or a
// Crash's restart actually lands — which is not seeded and should not be:
// the majority bound must reflect the cluster's REAL condition, not a
// simulated count that could drift from reality and let a genuine
// below-quorum situation slip through unchecked. So two live runs of the
// same seed reliably agree on the FIRST nemesis event (nothing has happened
// yet, so there is no live state to diverge on), and usually track closely
// for a while after, but can and do diverge in the specific sequence of
// event kinds once enough real time has passed for recovery timing to
// differ between runs. TestFirstNemesisEventIsReproducible checks the part
// that is actually guaranteed; TestManySeeds' battery of fixed seeds is what
// gives broad coverage despite that, not exact replay.
//
// What none of this controls is wall-clock scheduling generally: Raft's
// election and heartbeat timers still fire on real time, and goroutines
// still run on the real Go scheduler. A true FoundationDB/TigerBeetle-style
// simulator virtualizes time itself and single-threads the whole system so
// that a seed reproduces bit-for-bit identical execution forever. This
// harness does not go that far — see docs/phase6.md for what that would
// take, and for why "pin everything, including the safety checks, to a
// seed" is not simply a stronger version of this design but a different,
// harder one: a virtualized clock lets the majority-bound check itself be
// replayed exactly, because "current state" stops depending on the real
// scheduler at all.
//
// Also NOT pinned by the seed: which KV mutation replies ClientReplyLoss
// drops. The coin is seeded, but flips are taken in whatever order
// concurrent replies arrive.
//
// # The client path is the production one
//
// Every replica is served over a loopback gRPC listener, and every KV
// caller in the harness goes through kv/client: each raw-KV workload
// goroutine owns one Session (one client id, increasing request ids across
// all of its operations and retries), and the scheduler and gateway each
// use sched/kvadapter's pool of Sessions, exactly as their cmd/ binaries
// do. kv/client's retry loop, leader-hint jumps and per-attempt timeouts
// therefore run under every fault, and Gets are served by the replicas'
// ReadIndex path. Nemesis faults act on Raft peer traffic (simnet); the
// only client-hop fault is ClientReplyLoss. See kvcluster.go.
//
// # Invariants checked
//
//   - KV linearizability: every Get/Put/CAS the workload issued is checked
//     with Porcupine against a single-object key-value model. An
//     inconclusive Porcupine run (time budget exhausted) is reported in
//     Report.KVCheck, not counted as a violation; TestManySeeds fails if it
//     happens on more than 2 of its 12 seeds.
//   - KV client discipline: a workload operation may only fail with a
//     transient code (Unavailable, DeadlineExceeded, Aborted, Canceled, or
//     its own deadline), and no mutation may be refused as stale while its
//     caller is still waiting. Report.KVRedeliveries counts mutations
//     resent with an already-delivered (client_id, request_id), i.e. how
//     often the store's dedup path actually ran.
//   - Scheduler (production ScanLimit, ~30% of submits idempotent, each key
//     submitted three times, twice concurrently): after the run heals,
//     every id a Submit returned reaches DONE within 30s (DONE is the only
//     allowed terminal state; see schedworkload.go); no job has more than
//     one accepted Complete, and a DONE job holds its accepted Complete's
//     result; every Submit of one key returned the same id and at most one
//     record per key ever became runnable. TestManySeeds additionally
//     requires fenced zombie commits and duplicate submits to have happened.
//   - Gateway (IncludeGateway): every successful response, cached or not,
//     equals the full ground-truth answer for its tenant+prompt; failures
//     during chaos carry only retryable codes (Unavailable,
//     DeadlineExceeded, ResourceExhausted, Canceled); no Generate outlives
//     its deadline by more than 2s (stalled readers included); a 40-request
//     batch after the heal succeeds at >= 95%; no worker generation is
//     still running 3s after traffic stops. A worker failing with Internal
//     is mapped to Unavailable by the gateway, so only the post-heal bound
//     sees it.
//   - No panics in any goroutine the harness starts: each one recovers and
//     records its own panic as a violation (a recover only works on its own
//     goroutine). Panics in library-owned goroutines crash the process. A
//     goroutine that blocks forever is NOT detected as a violation; it
//     hangs Run's final wait, so only `go test`'s own timeout catches it.
//
// A failed run's Report names the seed and lists every nemesis event with
// the workload op count at which it fired; rerunning `simrun -seed <n>`
// replays the same workload and first nemesis event (see above for why
// the rest of the schedule is best-effort, not exact).
package chaos

import (
	"time"
)

// NemesisEvent is one scheduled disruption.
type NemesisEventKind int

const (
	EventPartition NemesisEventKind = iota // disconnect one node
	EventHeal                              // reconnect it
	EventCrash                             // kill and restart one node from its persisted state
	EventPause                             // block a node's goroutines for a duration (simulates a GC pause / SIGSTOP)
)

func (k NemesisEventKind) String() string {
	switch k {
	case EventPartition:
		return "partition"
	case EventHeal:
		return "heal"
	case EventCrash:
		return "crash"
	case EventPause:
		return "pause"
	default:
		return "unknown"
	}
}

// Scenario configures one chaos run.
type Scenario struct {
	Seed     int64
	Duration time.Duration

	NumKVNodes int // Raft replicas in the KV cluster. Default 5.
	NumClients int // concurrent KV/scheduler workload clients. Default 6.

	// NemesisInterval is the mean number of workload operations between
	// nemesis events (an event happens roughly every this-many ops, jittered
	// by the seed). 0 disables the nemesis (a "quiet" run, useful as a
	// control). Events target a uniformly random live node.
	NemesisInterval int
	// PauseDuration bounds how long an EventPause holds a node's apply loop.
	// Default 200ms.
	PauseDuration time.Duration

	// IncludeScheduler runs a sched.Queue workload over the same KV
	// concurrently with the raw KV workload. false (the zero value) skips
	// it; simrun's -scheduler flag defaults it to true.
	IncludeScheduler bool
	// IncludeGateway additionally wires a gateway.Server and mock inference
	// workers, and sends it concurrent traffic (uncached, cache-on, slow
	// readers, stalled readers). It has no linearizable model, but every
	// successful answer is checked against the exact ground truth for its
	// tenant+prompt; see gatewaymix.go for the full list. Default false.
	IncludeGateway bool
	NumWorkers     int // inference workers when IncludeGateway. Default 3.

	// ClientReplyLoss is the probability that a successful KV mutation's
	// reply is dropped on its way from the replica back to kv/client (the
	// client sees Unavailable after the write was applied). It forces the
	// production retry path to resend the SAME RequestMeta and the store to
	// answer it from its dedup table. 0 disables it.
	ClientReplyLoss float64

	// Logf receives a line per nemesis event and per check result. nil
	// discards it (used by the meta-tests, which run many scenarios).
	Logf func(format string, args ...any)

	// faults injects KNOWN BUGS into harness-side fakes (never into library
	// code). Only this package's tests set it, to prove each check can
	// fail; a zero value means a clean run.
	faults injectedFaults
	// schedDrain overrides the scheduler's post-run liveness budget (30s by
	// default) so the liveness-check test does not wait the full bound.
	schedDrain time.Duration
}

// injectedFaults are deliberate bugs planted in harness-owned code paths,
// used by the "checker catches X" tests. Each one models a real class of
// bug the corresponding check exists to catch.
type injectedFaults struct {
	// kvFreshMeta: every delivered KV mutation gets a brand-new identity,
	// i.e. a client that rebuilds its RequestMeta per retry. With reply
	// loss, one logical Put/CAS is applied more than once.
	kvFreshMeta bool
	// kvSharedSession: every KV workload goroutine shares ONE kv/client
	// Session, breaking the one-outstanding-mutation-per-identity rule, so
	// a later request id can overtake an earlier one.
	kvSharedSession bool
	// schedForgetIdemKeyOnRetry: a Submit retry drops its idempotency key,
	// so a retried submit creates a second job.
	schedForgetIdemKeyOnRetry bool
	// schedLoseEveryFifthJob: workers silently drop every job whose id is a
	// multiple of 5 (claim it, never complete or fail it), so those jobs
	// never reach DONE.
	schedLoseEveryFifthJob bool
	// gwWorkerInternal: inference workers fail this fraction of
	// generations with codes.Internal.
	gwWorkerInternal float64
	// gwWorkerTruncate: inference workers end every stream one token
	// early, still marked done (a short answer that looks complete).
	gwWorkerTruncate bool
	// gwCacheTruncate: the cache stores only half of every answer.
	gwCacheTruncate bool
	// gwCacheIgnoreTenant: the cache drops the gateway's tenant qualifier
	// from its key, so one tenant's answer is served to another.
	gwCacheIgnoreTenant bool
	// gwPlainSendError: during the chaos phase, the client stream's Send
	// fails normal-mode requests with a plain (non-gRPC-status) error,
	// which the gateway passes through and so surfaces as Unknown.
	gwPlainSendError bool
}

func (s Scenario) withDefaults() Scenario {
	if s.Duration <= 0 {
		s.Duration = 5 * time.Second
	}
	if s.NumKVNodes <= 0 {
		s.NumKVNodes = 5
	}
	if s.NumClients <= 0 {
		s.NumClients = 6
	}
	if s.PauseDuration <= 0 {
		s.PauseDuration = 200 * time.Millisecond
	}
	if s.NumWorkers <= 0 {
		s.NumWorkers = 3
	}
	return s
}

// Event is one nemesis action taken during a run, recorded for the report.
type Event struct {
	Index int // ops elapsed when this fired, for correlating with a failure
	Kind  NemesisEventKind
	Node  int
	At    time.Time
}

// Report summarizes one run.
type Report struct {
	Seed     int64
	Scenario Scenario
	Events   []Event
	KVOps    int
	// KVCheck is Porcupine's verdict on the KV history: "ok", "illegal",
	// or "unknown" (its time budget ran out: inconclusive, which is
	// reported but not counted as a violation).
	KVCheck string
	// KVRetries is kv/client's own retry counter (every client family the
	// harness built), KVRedeliveries how many mutation deliveries carried a
	// (client_id, request_id) the replicas had already been sent, and
	// KVRepliesLost how many applied mutations had their reply dropped by
	// ClientReplyLoss. KVRedeliveries > 0 is the evidence that the store's
	// dedup path actually ran in this run.
	KVRetries      uint64
	KVRedeliveries int64
	KVRepliesLost  int64
	// KVIdentities is how many distinct client ids sent mutations: one per
	// workload client plus each kvadapter pool's sessions, NOT one per
	// mutation.
	KVIdentities int
	// SchedJobs is how many Submit calls returned an id. SchedDone counts
	// accepted Completes, SchedFenced zombie Completes the queue refused,
	// SchedIdemKeys distinct idempotency keys and SchedIdemDupSubmits the
	// Submit calls that repeated an already-used key.
	SchedJobs           int
	SchedDone           int64
	SchedFenced         int64
	SchedIdemKeys       int64
	SchedIdemDupSubmits int64
	// GatewayReq counts every Generate call (chaos phase plus the post-heal
	// final batch) and GatewayErrors the failed ones. GatewayCacheHits
	// counts successful responses served from the cache, GatewayStalled
	// requests whose client never read the stream, and GatewayFinalOK of
	// GatewayFinalTotal the post-heal batch's successes.
	GatewayReq        int
	GatewayErrors     int
	GatewayCacheHits  int
	GatewayStalled    int
	GatewayFinalOK    int
	GatewayFinalTotal int
	Violations        []string // empty means the run passed
	Elapsed           time.Duration
}

// Passed reports whether no invariant was violated.
func (r *Report) Passed() bool { return len(r.Violations) == 0 }
