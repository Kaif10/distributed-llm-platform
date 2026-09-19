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
// # Invariants checked
//
//   - KV linearizability: every Get/Put/CAS the workload issued is checked
//     with Porcupine against a single-object key-value model.
//   - Scheduler exactly-once: every submitted job reaches DONE and is
//     completed by exactly one accepted Complete call.
//   - No panics, no goroutine ever blocks past the run's deadline.
//
// A failed run's Report names the seed and the operation index nearest the
// failure; rerunning `simrun -seed <n>` reproduces it (modulo the honesty
// note above).
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
	// concurrently with the raw KV workload. Default true.
	IncludeScheduler bool
	// IncludeGateway additionally wires a gateway.Server and mock inference
	// workers, and sends it concurrent traffic. It is checked more lightly
	// (no panics, sane hedge/cache counters) since it has no linearizable
	// KV-shaped model of its own. Default false.
	IncludeGateway bool
	NumWorkers     int // inference workers when IncludeGateway. Default 3.

	// Logf receives a line per nemesis event and per check result. nil
	// discards it (used by the meta-tests, which run many scenarios).
	Logf func(format string, args ...any)
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
	Seed       int64
	Scenario   Scenario
	Events     []Event
	KVOps      int
	SchedJobs  int
	GatewayReq int
	Violations []string // empty means the run passed
	Elapsed    time.Duration
}

// Passed reports whether no invariant was violated.
func (r *Report) Passed() bool { return len(r.Violations) == 0 }
