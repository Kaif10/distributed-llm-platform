package chaos

// The Phase 6 exit criterion: run the harness across many fixed seeds and
// require zero violations, plus a direct test of the "seed reproduces the
// scenario" property itself.

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"dsys/kv/raftkv"
	"dsys/raft/simnet"
	"dsys/sched/kvadapter"
)

func report(t *testing.T, rep *Report) {
	t.Helper()
	t.Logf("seed=%d kvOps=%d kvCheck=%s kvRetries=%d redeliveries=%d repliesLost=%d identities=%d schedJobs=%d gatewayReq=%d events=%d elapsed=%s",
		rep.Seed, rep.KVOps, rep.KVCheck, rep.KVRetries, rep.KVRedeliveries, rep.KVRepliesLost, rep.KVIdentities,
		rep.SchedJobs, rep.GatewayReq, len(rep.Events), rep.Elapsed.Round(time.Millisecond))
	for _, v := range rep.Violations {
		t.Errorf("violation: %s", v)
	}
	// One identity per logical caller, never one per mutation: the KV
	// workload's sessions plus at most two kvadapter pools.
	if max := rep.Scenario.NumClients + 2*kvadapter.DefaultSessions; rep.KVIdentities > max {
		t.Errorf("%d distinct client ids sent mutations (> %d): identities are being minted per operation", rep.KVIdentities, max)
	}
}

// runExpectingViolation runs sc and fails unless at least one violation
// contains one of wants. It is how each "checker catches X" test proves a
// check can actually fail, against a deliberately injected bug.
func runExpectingViolation(t *testing.T, sc Scenario, wants ...string) *Report {
	t.Helper()
	rep, err := Run(context.Background(), sc)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range rep.Violations {
		for _, want := range wants {
			if strings.Contains(v, want) {
				t.Logf("caught as expected: %s", firstLine(v))
				return rep
			}
		}
	}
	t.Fatalf("injected bug NOT detected: want a violation containing one of %q, got %q", wants, rep.Violations)
	return rep
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// TestCheckerCatchesDuplicateApply proves the KV linearizability check can
// see a broken dedup path. The injected bug rewrites every delivered
// mutation's RequestMeta to a fresh identity (a client that rebuilds its
// request per retry); with reply loss forcing retries of already-applied
// writes, one logical Put/CAS is applied several times, which Porcupine
// must flag. The control (same reply loss, no bug) must pass, which is what
// shows the dedup path of the real kv/client + store is what saves it.
func TestCheckerCatchesDuplicateApply(t *testing.T) {
	sc := Scenario{
		Seed: 3, Duration: 3 * time.Second, NumKVNodes: 3, NumClients: 6,
		ClientReplyLoss: 0.3,
	}
	t.Run("control", func(t *testing.T) {
		rep, err := Run(context.Background(), sc)
		if err != nil {
			t.Fatal(err)
		}
		report(t, rep)
		if rep.KVCheck != "ok" {
			t.Fatalf("control run: kv check %q, want ok", rep.KVCheck)
		}
		if rep.KVRedeliveries == 0 {
			t.Fatal("control run: no mutation was ever redelivered; the dedup path did not run")
		}
	})
	t.Run("injected", func(t *testing.T) {
		bad := sc
		bad.faults.kvFreshMeta = true
		runExpectingViolation(t, bad, "NOT linearizable")
	})
}

// TestCheckerCatchesSharedIdentity proves the harness notices when the
// one-outstanding-mutation-per-identity rule is broken: every KV workload
// goroutine shares ONE kv/client Session, so a later request id overtakes
// an earlier outstanding one. Two outcomes are possible and both must be
// reported:
//
//   - the earlier request reaches a replica after the later one applied and
//     is refused up front as stale (FailedPrecondition): the "stale"
//     violation;
//   - both were already in the Raft log, so the store SKIPS the earlier one
//     at apply time (store.Machine.Apply) and raftkv acknowledges that
//     skipped entry to its waiting caller as a success with a zero result.
//     The caller then believes a write happened that never did, which only
//     the linearizability check can see.
//
// The second path is the one this test hits in practice; it is reachable
// only by a client that breaks kv/client's documented rule, which is why
// production code is safe, but it is also why the harness must never run
// two concurrent callers on one identity.
func TestCheckerCatchesSharedIdentity(t *testing.T) {
	sc := Scenario{Seed: 5, Duration: 2 * time.Second, NumKVNodes: 3, NumClients: 6}
	sc.faults.kvSharedSession = true
	runExpectingViolation(t, sc, "stale", "NOT linearizable")
}

// TestQuietRun is the control: no nemesis at all. If this ever fails, the
// bug is in the harness or the workload, not in fault tolerance.
func TestQuietRun(t *testing.T) {
	rep, err := Run(context.Background(), Scenario{
		Seed: 1, Duration: 3 * time.Second, NumKVNodes: 3, NumClients: 4,
		NemesisInterval: 0, IncludeScheduler: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	report(t, rep)
}

// TestManySeeds is the fuzz-with-reproducible-seeds exit criterion: a
// spread of fixed seeds, KV + scheduler chaos, zero violations.
func TestManySeeds(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	var redelivered, unknown int64
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(seedName(seed), func(t *testing.T) {
			rep, err := Run(context.Background(), Scenario{
				Seed: seed, Duration: 4 * time.Second, NumKVNodes: 5, NumClients: 6,
				NemesisInterval: 25, PauseDuration: 150 * time.Millisecond,
				IncludeScheduler: true, ClientReplyLoss: 0.05,
			})
			if err != nil {
				t.Fatal(err)
			}
			report(t, rep)
			redelivered += rep.KVRedeliveries
			if rep.KVCheck == "unknown" {
				unknown++
			}
		})
	}
	// Non-vacuity across the battery: the production client really did
	// resend applied mutations (dedup path exercised), and Porcupine
	// reached a verdict on most seeds rather than timing out.
	if redelivered == 0 {
		t.Error("no KV mutation was redelivered in any seed: the dedup path never ran")
	}
	if unknown > 2 {
		t.Errorf("Porcupine was inconclusive on %d/12 seeds: the KV check is mostly vacuous", unknown)
	}
}

// TestGatewayMixSurvivesChaos proves the LLM-serving layer composes into
// the same run: no panics, requests keep completing while the KV layer is
// being disrupted underneath it.
func TestGatewayMixSurvivesChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	rep, err := Run(context.Background(), Scenario{
		Seed: 999, Duration: 4 * time.Second, NumKVNodes: 5, NumClients: 6,
		NemesisInterval: 30, IncludeScheduler: true, IncludeGateway: true, NumWorkers: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	report(t, rep)
	if rep.GatewayReq == 0 {
		t.Fatal("gateway mix issued zero requests; it did not actually run")
	}
}

// TestChooseEventDeterministicGivenFixedState is the real proof of
// determinism, isolated from real-time scheduling entirely: chooseEvent's
// OWN decision, given a FIXED cluster state (which nodes are currently
// partitioned/crashed) and a *rand.Rand at a known position, is a pure
// function of that state and that draw. Two rand.Rand seeded identically,
// against the same held-still state, must pick identically, every time.
//
// This replaced an earlier test (TestSeedReproducesNemesisSchedule) that
// tried to prove determinism at the WHOLE-HARNESS level by comparing two
// live runs' full event sequences, and was too strong a claim: chooseEvent
// deliberately consults LIVE state (isPartitioned/isCrashed) to enforce the
// majority bound — see chooseEvent's doc comment — and live state depends on
// real goroutine timing (how fast a Pause's auto-heal or a Crash's restart
// actually lands), which is not, and should not be, seeded. A run that
// happens to check eligibility a few milliseconds earlier or later than
// another can see a different down-count and legitimately choose
// differently. That is a deliberate trade-off (correctness against the
// LIVE cluster beats a reproducibility guarantee that would have to ignore
// reality to hold), not a bug, and this test proves the part that actually
// is guaranteed: the decision FUNCTION, not the live schedule it is fed.
func TestChooseEventDeterministicGivenFixedState(t *testing.T) {
	net := simnet.New(5)
	c, err := newKVCluster(5, net, raftkv.Config{}, kvClusterOptions{seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.killAll()
	// Hold a fixed, hand-picked mix of down nodes for the whole test: one
	// partitioned, one crashed, three free. chooseEvent must never observe
	// this state change.
	c.partition(1)
	c.crash(3)

	draw := func(seed int64, n int) []nemesisEvent {
		r := rand.New(rand.NewSource(seed))
		out := make([]nemesisEvent, 0, n)
		for i := 0; i < n; i++ {
			ev := chooseEvent(c, r, 150*time.Millisecond)
			if ev == nil {
				t.Fatalf("draw %d: no eligible event against a fixed state; should always find one here", i)
			}
			out = append(out, *ev)
		}
		return out
	}

	for _, seed := range []int64{1, 2, 3, 42, 7} {
		a, b := draw(seed, 100), draw(seed, 100)
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("seed %d draw %d: %+v vs %+v — chooseEvent is not a pure function of (state, rand)", seed, i, a[i], b[i])
			}
		}
	}
}

// TestFirstNemesisEventIsReproducible checks the one whole-harness
// reproducibility claim that is actually always true: the FIRST event a
// live run's nemesis fires is decided against the initial state (nothing
// down yet), which never depends on timing, so it is fully pinned by the
// seed. Anything after that is best-effort (see
// TestChooseEventDeterministicGivenFixedState's comment for why).
func TestFirstNemesisEventIsReproducible(t *testing.T) {
	first := func() Event {
		rep, err := Run(context.Background(), Scenario{
			Seed: 7, Duration: 2 * time.Second, NumKVNodes: 3, NumClients: 4,
			NemesisInterval: 15, IncludeScheduler: false,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Events) == 0 {
			t.Fatal("no nemesis events fired; nothing to compare")
		}
		return rep.Events[0]
	}
	a, b := first(), first()
	if a.Kind != b.Kind || a.Node != b.Node {
		t.Fatalf("seed 7's first event differed: %s node %d vs %s node %d", a.Kind, a.Node, b.Kind, b.Node)
	}
	t.Logf("seed 7: first event reproduced exactly: %s node %d", a.Kind, a.Node)
}

func seedName(s int64) string {
	digits := [...]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	if s == 0 {
		return "seed0"
	}
	out := ""
	for s > 0 {
		out = digits[s%10] + out
		s /= 10
	}
	return "seed" + out
}
