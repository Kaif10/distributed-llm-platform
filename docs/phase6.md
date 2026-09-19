# Phase 6 — Verification, chaos, observability

Code: `chaos/` (`api.go`, `harness.go`, `nemesis.go`, `kvcluster.go`, `schedworkload.go`,
`gatewaymix.go`, `chaos_test.go`), `raft/simnet/simnet.go` (`NewSeeded`/`intn`),
`raft/simnet/seed_test.go`, `cmd/simrun`, `obs/` (`tracing.go`, `grpc.go`, `metrics.go`),
`docker-compose.yml`, `docker/Dockerfile.services`, `docker/Dockerfile.worker`,
`scripts/chaos_docker.sh`, `docker-compose.observability.yml`, `docker/observability/`,
`k6/llm_load_test.js`.
Run: `make test-chaos` (`Makefile:64-67`), `make simrun` (`:69-71`), `make chaos-docker`
(`:73-76`, real containers), `docker compose -f docker-compose.observability.yml up -d`
(traces/metrics/dashboard).

Build note. This phase adds no new storage engine, no new consensus code, no new RPC surface —
every invariant it checks was built in Phases 1-5. What it adds is the harness that proves those
invariants hold under simultaneous, seeded, adversarial conditions, and the observability to watch
it happen. Three files carry the intellectual weight and were written and rewritten by hand:
`chaos/nemesis.go`'s `chooseEvent` (the majority bound, §4 and war story 1), `chaos/harness.go`'s
`Run` (the exact post-run sequence, §4 and war story 1), and `chaos/api.go`'s package doc (the
precise statement of what "deterministic" means, §3 and war story 2 — this was rewritten late,
after the first version overclaimed). Everything else — `kvcluster.go`, `schedworkload.go`,
`gatewaymix.go`, `obs/`, the Docker chaos script, the k6 script — is plumbing wired against those
three and reviewed against them.

## 1. What Phase 6 builds and why

The roadmap's Phase 6 (`ROADMAP.md:115-119`) asks for three things: a deterministic simulation
harness that runs the whole cluster in one process with a simulated network, seed-reproducible
failures, "FoundationDB/TigerBeetle style"; chaos against a real Docker Compose stack (`tc netem`
partitions, clock skew, disk-full); and OpenTelemetry traces plus a Prometheus/Grafana dashboard
plus k6 load tests. The exit criterion is one sentence: **"one command reproduces any failure from
a seed."**

Read literally, the roadmap's phrasing ("FoundationDB/TigerBeetle style") promises more than this
phase delivers, and the honest thing to do is say so up front rather than let the rest of the
document quietly redefine "deterministic" downward. `chaos/api.go`'s package doc — specifically its
"What deterministic means here, precisely" section (`chaos/api.go:8-48`) — is the authoritative,
precise statement of the gap, and §3 below walks it in full. The short version: the workload and
the network-fault injection *are* fully pinned by the seed; the nemesis's live decision-making is
pinned only given a state that itself depends on real wall-clock timing. That is materially weaker
than true deterministic simulation, and materially more useful than an unseeded chaos test, and the
difference between those two claims is worth being able to state exactly in an interview.

**Three pieces, in dependency order.**

1. **The in-process chaos harness** (`chaos/`). One process hosts a real 5-node Raft-replicated KV
   cluster (`kv/raftkv`) over a seeded `raft/simnet.Net`, a real `sched.Queue` on top of it, and
   optionally a real `gateway.Server` with mock inference workers — all wired together, then driven
   by a nemesis (partitions, crashes, pauses, scheduled and bounded by quorum) and a workload
   (concurrent KV, scheduler, and gateway clients), everything keyed off one `int64` seed
   (`chaos/api.go:1-6`).
2. **Chaos against real containers** (`docker-compose.yml`, `scripts/chaos_docker.sh`). The same
   scenario shape — partition, crash, network degradation — but against actual separate OS
   processes in actual separate containers talking over an actual Docker bridge network, which is
   the only way to find the two bugs neither an in-process harness nor an in-memory fake can produce
   (war stories 3 and 4).
3. **Observability** (`obs/`, `docker-compose.observability.yml`, `k6/`). Traces and metrics wired
   into the gateway and scheduler so that a chaos run, or a load test, is something you can *look
   at* — a trace tree in Jaeger, counters in Prometheus, a dashboard in Grafana — not just a pass/
   fail line in test output.

The three compose in one direction: the in-process harness is what makes seeds and reproducibility
possible at all (§2); the Docker layer is what proves the same invariants survive real process
boundaries, real container lifecycles, and real dependency installs; observability is what makes
either one legible to a human watching it happen, and to k6 driving it under load. §3 gives the
full, precise account of what "deterministic" buys in piece 1; §6 and §7 cover pieces 2 and 3.

## 2. Why seed `raft/simnet` first

Everything downstream needed one thing that did not exist before this phase: a way to make
`raft/simnet.Net`'s fault injection reproducible. `raft/simnet.go` decides, for every RPC, whether
to drop it, delay it, or reorder its reply — and before Phase 6 every one of those decisions came
from `math/rand`'s global source, seeded from wall-clock time (`simnet.go:44-48`, the old `New`).
Two runs of the same test picked different faults. There is no way to build a seed-reproducible
chaos harness on top of a network layer that is not itself seed-reproducible — this was the
necessary first step, and a small one in code size, out of proportion to how load-bearing it is.

**The refactor.** One field, `rng *rand.Rand` (`simnet.go:35-36`), and one method, `intn`
(`simnet.go:79-85`), guarded by its own mutex separate from the network's state mutex. Every site
that used to call `rand.Intn` — the "drop this request", "delay this reply", "reorder with
probability 2/3" decisions inside `call` (`simnet.go:246-288`) — now calls `sn.intn(...)` instead.
`NewSeeded(n, seed)` (`simnet.go:65-77`) constructs the RNG from a caller-supplied seed; `New(n)`
(`simnet.go:46-48`) is kept, unchanged in signature, and now just calls `NewSeeded(n,
time.Now().UnixNano())` — the historical "every run picks different faults" behavior, preserved
exactly, for every one of the dozens of existing tests across four phases that call `New` and have
no idea a seed even exists.

**Why backward compatibility was non-negotiable, not a nicety.** By the time this phase starts,
`simnet.New` is called from Raft's own test suite, `kv/raftkv`'s tests, `shardkv`'s tests, and
`sched`'s `TestOverRaftKVWithLeaderLoss` — four phases of tests that predate this one and were not
going to be rewritten to thread a seed through. The refactor had exactly one new field and one new
constructor; every existing call site, every existing test, needed to keep compiling and keep
passing unmodified. The full-repo regression run after this change (§8) is the proof it held:
`dsys/raft` at ~269 seconds, zero regressions across every downstream consumer. This is the same
discipline Phase 3's sharding work and Phase 4's scheduler applied when building on top of
Phases 1-2 without touching their guarantees — add underneath, verify nothing above moved.

**`TestSeededFaultsAreReproducible`** (`raft/simnet/seed_test.go:11-30`) is the whole mechanism the
chaos harness leans on, proven in isolation from Raft entirely: construct two `Net`s from the same
seed, draw 500 `intn` calls from each, require byte-for-byte identical sequences. `intn` is the
*one* place this package reads randomness (`simnet.go:79-80`), so this one test is a complete proof
that seeding it governs every fault-injection decision the network makes.
`TestDifferentSeedsDiffer` (`seed_test.go:33-45`) is the complementary sanity check — two different
seeds must not agree more than twice in 50 draws, catching a seeding bug that accidentally makes
every seed produce the same sequence. `TestUnseededNetStillWorks` (`seed_test.go:50-55`) pins the
"still works with no seed" contract directly, on top of every pre-existing test in the repo already
exercising it as a side effect.

**State the distinction once, cleanly, because it recurs at every layer above this one.** A seed
reproduces the same fault-injection *decisions* — which RPC gets dropped, which reply gets delayed
and by how long, in what order relative to the sequence of calls made. It does not reproduce the
same wall-clock *interleaving* of when those decisions land relative to everything else running:
Raft's election and heartbeat timers still fire on the real clock, delayed replies still sleep on
real `time.After`, and goroutines still run on the real Go scheduler, whose relative speeds shift
with machine load, GC pauses, and `-race`'s instrumentation overhead. `NewSeeded`'s own doc comment
says this precisely (`simnet.go:55-64`): "two runs of the same seed make the same fault DECISIONS,
but a slow CI machine can still deliver them in a different order relative to, say, a Raft election
timer that fires on real time." Keep that sentence in mind through §3 and war story 2 — it is the
seed's entire honest scope, and everything built on top of `simnet` inherits exactly this shape and
no more.

## 3. What "deterministic" means here, precisely

This is `chaos/api.go`'s "What deterministic means here, precisely" section (`api.go:8-48`), walked
in full, because it is the intellectual core of the phase and the section most likely to come up,
almost verbatim, in a systems interview about testing distributed systems.

**Tier 1 — the workload: fully pinned.** Each client goroutine in the harness's KV workload
(`chaos/harness.go:78-132`) owns its own `*rand.Rand`, seeded from the scenario seed plus its client
id (`harness.go:82`, `sc.Seed + int64(cid) + 1`), and draws from it in a fixed order relative to its
own iteration count — which key, which operation (get/put/cas), which values. Nothing about this
depends on timing: the *sequence of decisions* a client makes is a pure function of the seed and how
many iterations it has completed, independent of how fast or slow those iterations actually run.
Same shape for the scheduler workload's producers and workers (`chaos/schedworkload.go:56`, `:84`,
seeded from `sc.Seed+2`/`sc.Seed+3` plus an offset) and the gateway mix's traffic generator
(`chaos/gatewaymix.go:113`, `sc.Seed+5000+c`).

**Tier 2 — network faults: fully pinned.** Every drop, delay, and reorder decision `raft/simnet`
makes is a pure function of the seed and nothing else, proven directly by §2's
`TestSeededFaultsAreReproducible`. This is inherited, not reimplemented, by the chaos harness
because `harness.go:55` constructs the cluster's network with `simnet.NewSeeded(sc.NumKVNodes,
sc.Seed)`.

**Tier 3 — the nemesis's decision FUNCTION: pinned given fixed state, proven by unit test.** This is
where it gets subtle, and where an earlier version of this codebase got it wrong (war story 2).
`chooseEvent` (`chaos/nemesis.go:43-95`) is a pure function of exactly two inputs: a `*rand.Rand`
draw, and the cluster's *current* state — which nodes are partitioned or crashed right now
(`nemesis.go:19-22`). Given the same state and the same `*rand.Rand` position, it always picks the
same thing. That is a real, useful, and narrower claim than "the nemesis is deterministic," and
`TestChooseEventDeterministicGivenFixedState` (`chaos_test.go:101-132`) proves exactly that claim in
isolation, holding the state still by hand (`c.partition(1); c.crash(3)`) so no live timing is
involved at all.

**Tier 4 — the live nemesis schedule: best-effort, first event only guaranteed.** "Current state" in
a *live* run is not itself seeded, and — this is the point the section insists on — it *should not
be*: the majority bound (§4) exists to protect Raft's real quorum guarantee, and it can only do that
by consulting the cluster's real condition, not a simulated count that could drift from reality and
let a genuine below-quorum situation through unchecked. Real condition depends on real timing — how
fast a `Pause`'s 150ms auto-heal or a `Crash`'s 300-1000ms restart actually lands, which itself
varies with goroutine scheduling and, materially, with `-race`'s instrumentation overhead. So: two
live runs of the same seed reliably agree on the *first* nemesis event, because nothing has happened
yet and there is no live state to diverge on (proven by `TestFirstNemesisEventIsReproducible`,
`chaos_test.go:140-159`); they usually track closely for a while after; and they can, and do,
diverge in the specific sequence of event kinds once enough real time has passed for recovery timing
to differ between runs. `TestManySeeds`' battery of twelve fixed seeds is what gives broad coverage
despite that — reproducibility of the *scenario shape*, not exact replay of the *event sequence*.

**Contrast with FoundationDB/TigerBeetle-style deterministic simulation.** Those simulators
virtualize time itself and single-thread the whole system under test, so a seed reproduces bit-for-
bit identical execution forever — the same event happens at the same virtual tick on every replay,
with no dependency on the real scheduler at all (`simnet.go:56-58`, `api.go:40-48`). Getting there
from here is not a matter of seeding one more `rand.Rand`; it requires two structural changes this
harness does not make: **virtualize the clock** (Raft's election/heartbeat timers, the nemesis's
`time.Sleep`-based auto-heal/restart, and every `time.After` in `simnet.call` would all need to read
from a fake, advanceable clock instead of the wall clock — see Phase 5's `Options.Clock` pattern,
generalized to *drive* time forward rather than merely being read), and **single-thread the system**
(so that "what happens next" is a deterministic function of a scheduler *you* control picking the
next event off a priority queue, not the real Go runtime's goroutine scheduler picking whatever is
ready). That second change is the harder one: it typically means the system under test cannot use
real goroutines and real channels the way this whole codebase does, but must run as a state machine
stepped by a simulated event loop — which is why FoundationDB and TigerBeetle build their core
logic to be run either under real I/O or under a simulator, as a first-class design constraint from
day one, not bolted on after. That is a fundamentally different, harder design than seeding one
network's fault injection on top of an otherwise-ordinary concurrent Go program, and §9 and Exercise
(a) come back to what a partial step in that direction would look like here.

## 4. Walk the harness in request order

`Run` (`chaos/harness.go:46-259`) wires and drives everything for one `Scenario` (`chaos/api.go:92-
141`) and returns a `Report` (`api.go:151-165`).

**The KV workload** (`harness.go:73-132`) builds a Porcupine operation history: `sc.NumClients`
goroutines, each looping get/put/cas against five fixed keys (`"a".."e"`) until the run's context is
done, recording `porcupine.Operation{ClientId, Input, Output, Call, Return}` for every call
(`:123-128`). This is deliberately the same model shape as Phase 2's and Phase 3's own
linearizability tests (`kvInput`/`kvOutput`/`kvModel`, referenced but not redefined here) — duplicated
rather than imported, on purpose, because this harness's job is to catch a regression in *any* layer
including the model itself; sharing the exact same model file with the code under test would let a
bug in the model hide a bug in the store. A failed operation is recorded with `Return` pushed far
into the future (`opStart.Add(sc.Duration + 10*time.Second)`, `:121`) rather than at the real failure
time — Porcupine's convention for "this call never returned," which is the correct way to represent
an operation a client gave up on without falsely claiming it completed at some specific instant.

**The nemesis goroutine's trigger is op-count-driven, not time-driven** (`:162-196`). It does not
fire on a wall-clock ticker; it polls `opCount` (incremented once per completed KV operation, across
all clients) every 15ms and fires once the count crosses a randomly-jittered threshold
(`sc.NemesisInterval/2 + nr.Intn(sc.NemesisInterval+1)`, `:173`, re-rolled after each event, `:183`).
This ties nemesis pressure to how much work the system is actually getting done rather than to a
fixed clock — a cluster that is currently struggling (fewer ops/second because of an earlier fault)
automatically gets nemesis events spaced further apart in wall time, which is closer to "roughly one
disruption per so-much-work" than a fixed-interval timer would be.

**`chooseEvent`'s majority bound, worked example.** With `NumKVNodes = 5`, majority is `5/2 + 1 = 3`
(`nemesis.go:61`). `roomToTakeDown = c.n - majority - down = 5 - 3 - down`: with nothing down yet,
`roomToTakeDown = 2` — Partition, Pause, and Crash are all eligible, because taking one more node
down would still leave 5 - (down+1) = at least 3 up. Once two nodes are already down (`down = 2`,
`roomToTakeDown = 0`), Partition/Pause/Crash all disappear from the option list (`:72-80`) — only
Heal remains eligible, and only for nodes that are partitioned-but-not-crashed. Crash and
Partition/Pause **share one budget** (`:37-38`, `:62`): a crashed node and a partitioned node both
count against the same `down`, so the nemesis cannot, say, partition two nodes and then also crash a
third when that would leave only two of five reachable. If no option is eligible at all — every free
node already accounted for and nothing healable — `chooseEvent` returns `nil` and the harness simply
skips that tick (`:81-83`, `harness.go:185-187`) rather than blocking or forcing an event; there is
always a next tick.

**`apply`'s `pending *sync.WaitGroup` mechanism** (`nemesis.go:110-133`). `Heal` and `Partition` are
synchronous and instantaneous from the nemesis's point of view. `Pause` and `Crash` are not: each
schedules its own recovery (`c.heal` after `PauseDuration`, `c.start` after a random 300-1000ms) on
a separate goroutine and returns immediately — the nemesis loop is never blocked waiting for a
disruption to finish playing out, which matters because nothing stops it from stacking several
crashes in quick succession before earlier restarts have even fired. `pending.Add(1)` is called
*before* launching that goroutine and `pending.Done()` inside it, so the harness can later ask "have
every one of these scheduled recoveries actually landed yet" with `pending.Wait()` — a real
synchronization primitive instead of a guess.

**The post-run sequence, and why the order is exact** (`harness.go:198-221`):

```
runCtx expires  ->  wg.Wait()  ->  pending.Wait()  ->  heal-everything loop  ->  waitClusterAvailable  ->  Porcupine check  ->  scheduler check
```

1. **`wg.Wait()`** — every workload goroutine (KV clients, scheduler producers/workers, gateway
   traffic, the nemesis loop itself) has returned. The workload has genuinely stopped generating new
   load and new nemesis decisions.
2. **`pending.Wait()`** — every *already-scheduled* Pause auto-heal and Crash auto-restart has
   actually executed its `c.heal`/`c.start` call. This is not redundant with step 1: the nemesis
   goroutine that scheduled a delayed recovery has already exited by the time `wg.Wait()` returns
   (scheduling a goroutine and returning is instant); the recovery itself is still sleeping.
   Skipping this step, or replacing it with a fixed sleep, is exactly the bug war story 1 found.
3. **Heal-everything loop** (`:207-211`) — unconditionally reconnect any node still marked
   partitioned. Standard Jepsen practice: heal the network before checking anything, rather than
   trusting the nemesis to have gotten around to it. Given the majority bound (§ above) and step 2,
   this should rarely find work — but a Pause whose auto-heal goroutine is *mid-sleep* right when
   `pending.Wait()` returns (impossible by construction, since `pending.Wait()` blocks until it's
   done — but any future bound-logic edge case) is defended against here rather than assumed away.
4. **`waitClusterAvailable`** (`:27-44`) — poll a trivial `Get` every 50ms, up to a 15-second budget,
   until the cluster actually answers. Its own doc comment states why this replaced a fixed sleep: a
   fixed sleep either wastes time on a quiet run or is too short the one time several nodes went
   down together and Raft genuinely needs longer to re-elect and catch up — and under `-race`, how
   much longer varies with the race detector's own instrumentation overhead, which is not something
   a constant can absorb for every seed at once. Polling gets each seed exactly the time it needs,
   no more.
5. **Two independently-timed check phases** — Porcupine's linearizability check gets its own fresh
   20-second deadline (`porcupine.CheckOperationsVerbose(kvModel, history, 20*time.Second)`,
   `:237`), created at the moment it is called; the scheduler check gets its own fresh 30-second
   `context.WithTimeout(ctx, 30*time.Second)` (`:246`), created immediately before *it* runs, not
   shared with Porcupine's clock. §5's war story 1 is the reason this is two separate deadlines
   instead of one shared budget started earlier — the comment at `:227-236` states the failure mode
   directly: a shared clock lets one phase's slow, unrelated search silently starve the phase after
   it.

**Why this exact order and no other.** Reversing steps 2 and 3 (heal before waiting for pending)
would risk healing a node whose crash-restart goroutine is about to fire `c.start` on a *different*
node's cluster-state bookkeeping mid-heal-loop — not incorrect exactly, but needlessly racy against
work already in flight for no benefit. Running the checks before `waitClusterAvailable` would check
a cluster that is still mid-recovery and blame the harness's impatience on the system. Running
Porcupine after the scheduler check (instead of before) would work equally well *given* the fresh-
deadline fix, since the phases no longer share a clock — the current order is not load-bearing for
correctness once each phase has its own budget, only convention.

**Optional layers, and what each does and does not check.** `IncludeScheduler` (default true) wires
a `schedWorkload` (`chaos/schedworkload.go`) over the *same* nemesis-disrupted KV: producers submit
continuously, workers claim and mostly complete but 30% "go zombie" — sleep well past the 600ms
lease with no heartbeat, then try to commit anyway (`:110-114`), which the fencing check must
refuse. Its `check` (`:134-160`) verifies exactly one property that matters: no job was ever
committed more than once (`completions` map, counted per id), logging — never failing on — jobs
still in flight when the run ended, because chaos legitimately can outlast the workload. This is the
Phase 4 chaos test's exact shape, but now driven by real Raft leader changes instead of a fake clock
(`schedworkload.go:1-10`) — proof the fencing discipline survives consensus-layer disruption, not
just a mocked KV. `IncludeGateway` (default false) additionally wires a real `gateway.Server` and
mock inference workers over loopback gRPC, sharing the same KV for rate-limit buckets and the worker
registry (`chaos/gatewaymix.go`). It has **no linearizable model of its own** — streaming generation
is not a single-object CAS-shaped operation — so its check is deliberately lighter: no panic
anywhere in the request path (`gatewaymix.go:144-149`), full stop. It does not check hedge/cache
correctness under chaos, only survival; that is intentional, not an oversight — proving the whole
platform's layers survive simultaneous chaos is a different, weaker, and still valuable claim from
proving each layer's fine-grained behavior under chaos, which the gateway's own Phase 5 test suite
already does without a live nemesis.

## 5. The four war stories

### 5.1 War story 1 — the reproducibility test that was wrong, not the harness

**Symptom.** `TestManySeeds` failed on seeds 2 and 4 with `context deadline exceeded` reading
scheduler stats.

**First hypothesis (wrong).** Not enough settle time after the run ended. The fix tried was a fixed
sleep between the workload stopping and the checks running. Still failed.

**Second hypothesis (real, but incomplete).** The Porcupine linearizability check has its own
internal search that can spend most of a 20-second budget deciding, and this harness's first version
gave the *whole* check phase one shared 30-second clock, started before Porcupine ran. Porcupine's
own search silently ate most of that shared budget before the scheduler check ever got a turn — a
real bug, and the fix (each check phase gets its own fresh deadline, created immediately before that
phase runs, never one shared clock started earlier — `harness.go:227-236`, §4 step 5) is still in
the code today. But after this fix landed, the same seeds **still failed** — with a *worse* symptom:
49-second timeouts, deterministically, for the same seeds.

**Investigation.** Reproduced directly with `cmd/simrun -seed 2 ...` rather than continuing to guess
at the test harness, and read the printed event log line by line. The nemesis had partitioned
**three of five nodes**, with no `Heal` ever drawn for any of them before the run ended. Two nodes
left connected is not a majority of five. Raft was **behaving correctly**: no amount of patience
recovers a cluster that only has 2 of 5 votes — that cluster will sit unavailable forever, by
design, and the "violation" the test reported was the harness asking an impossible question, not a
bug in Raft or in the scheduler.

**Real fix.** Bound `chooseEvent` so Partition, Pause, and Crash are only eligible when enough nodes
are still up to leave a majority — the `roomToTakeDown` calculation walked through with a worked
example in §4 (`nemesis.go:43-95`) — plus a defensive heal-everything step after the nemesis stops
regardless (`harness.go:207-211`), standard Jepsen practice: heal the network before checking, don't
rely on the nemesis having gotten around to it on its own.

**Measured.** Seed 2, before the fix: 49.48 seconds wall time, and a reported violation
(`scheduler: could not read stats: context deadline exceeded`). After: 5.1 seconds, PASS, with the
harness logging "cluster answered again after 2 probe(s)" — `waitClusterAvailable` doing exactly
what its doc comment says, giving the run only as much extra time as it actually needed.

**General lesson.** A "violation" your own test reports might be your own test asking an impossible
question, not a bug in the system under test. The discipline that catches this is not "trust the
test," it is "reproduce it standalone and read what actually happened" — here, that meant going to
`cmd/simrun`'s printed event log rather than iterating on the test's timeouts, and the answer was
sitting right there once looked at directly: three of five nodes down, no heal, ever.

### 5.2 War story 2 — fixing story 1 broke reproducibility, correctly

**Symptom.** After the majority bound landed, `TestSeedReproducesNemesisSchedule` — a test that
compared two live runs of the same seed and asserted their full event-kind sequences matched — began
failing, with wildly different lengths per run for the nominally same 3-second seed-7 scenario (4, 8,
9, 15 events observed across runs) and mismatches starting as early as the *second* event.

**Why, precisely.** `chooseEvent` now consults live state — `isPartitioned`/`isCrashed` — to compute
`roomToTakeDown` (§4). That state's *timing* is not seeded and, per the argument in §3, should not
be: how fast a `Pause`'s 150ms auto-heal or a `Crash`'s 300-1000ms restart actually lands varies with
real goroutine scheduling, and materially with `-race`'s instrumentation overhead run to run.
Pinning that timing to make the schedule reproducible would mean the majority-bound safety check
stops reflecting the cluster's *real* condition — which is precisely the property that made the
majority bound correct in the first place. This is a genuine, correct trade-off — correctness
against live state, over reproducibility of a schedule that would have to ignore reality to be
reproducible — not a regression introduced by the fix.

**Fix.** Delete the whole-harness sequence-matching test and replace it with two narrower, honestly
true claims, each pinned to the layer of the system that actually supports it (§3's tier structure):

- `TestChooseEventDeterministicGivenFixedState` (`chaos_test.go:101-132`) — `chooseEvent` *is* a pure
  function of `(fixed state, rand)`, proven directly by holding real state still with
  `c.partition(1)`/`c.crash(3)` and drawing 100 events from two `*rand.Rand`s seeded identically, for
  five different seeds. No live timing is involved anywhere in this test.
- `TestFirstNemesisEventIsReproducible` (`:140-159`) — the one whole-harness claim that is always
  true: the *first* event a live run's nemesis fires is decided against the initial state (nothing
  down yet), which cannot depend on timing because nothing has happened yet to diverge on. Two live
  runs of seed 7 are required to agree on event kind and node for that first event only.

`chaos/api.go`'s package doc was rewritten at the same time to state this precisely instead of the
earlier, too-strong claim (§3 is that rewrite, walked in full).

**General lesson.** When a fix that makes a system more correct breaks a test asserting something
about the system's *old, less-correct* behavior, the right move is usually to fix the test's claim,
not water down the fix. And it is worth taking the occasion to write down, precisely and in one
place, exactly what the seed guarantees at each layer, because "deterministic" is not a single bit —
it decreases, one layer at a time: workload (fully pinned) > network faults (fully pinned) > nemesis
decision function (pinned given fixed state) > live nemesis schedule (best-effort, first event
only). Say which layer you mean, every time.

### 5.3 War story 3 — a naming collision that broke the whole repo's build

**Symptom.** The Docker engineer working on this phase's compose stack named the Go-services
Dockerfile `docker/Dockerfile.go`. The mentor's own verification run — `go build ./...` — failed
repo-wide with `docker\Dockerfile.go:1:1: illegal character U+0023 '#'`.

**Cause.** Go's own toolchain treats any file with a `.go` suffix anywhere under the module root as
Go source to be compiled, for `go build ./...` and even `go list ./...` — it does not check whether
the file lives inside a package directory that looks like Go code, or whether its content parses as
Go at all until it tries. A Dockerfile's first line is a comment starting with `#`, which is not
valid Go syntax, so the compiler failed on the very first character of a file it had no business
trying to compile. No `go` command scoped by `./...` worked anywhere in the repository — not `go
build`, not `go test`, not `go vet` — until the file was renamed to `docker/Dockerfile.services`
(the name it carries today, `docker-compose.yml:64`).

**Lesson.** A file extension is a contract with every tool that scans by extension, not just the one
tool you had in mind when you chose it. `.go` is claimed, unconditionally, by the entire Go
toolchain, for every directory under the module root, forever — never give a non-Go file a `.go`
suffix inside a Go module, full stop. This generalizes past Go: any project-wide tool that discovers
files by extension (a linter, a formatter, a CI matrix, a build system) makes every extension it
recognizes a small, easy-to-forget landmine for anyone naming a new file without checking what else
in the repo claims that suffix.

### 5.4 War story 4 — a protobuf version mismatch crash-looping every worker container

**Symptom.** Every `worker0`/`worker1`/`worker2` container in the Docker chaos stack crash-looped on
startup.

**Investigation.** `docker compose logs worker0` — read first, not guessed at — showed a Python
`VersionError` raised at import time, inside the generated gRPC stub modules.

**Cause.** `py/requirements.txt`'s install is split into phases specifically to dodge a
`ResolutionImpossible`: pip resolves one `pip install -r requirements.txt` invocation as a single
dependency set, and `opentelemetry-exporter-otlp-proto-grpc` pulls in `opentelemetry-proto`, which
declares `protobuf<6` — a hard conflict against the `protobuf==7.36.1` pin the core dependencies (and
the generated gRPC stubs) need. Splitting into separate `pip install` calls — core deps, then the
`opentelemetry-*` deps, verified in `docker/Dockerfile.worker:36-41` and its accompanying comment —
turns that conflict from a fatal resolution error into a silent one: the second phase's resolver only
considers its own packages against what is *already installed*, and happily downgrades protobuf
(observed: 7.36.1 -> 5.29.6) to satisfy `opentelemetry-proto`'s bound, because from its point of view
nothing else needs the newer version. But something else does: `py/dsys_gateway`'s generated
`*_pb2.py` files were compiled against protobuf 7.35.x gencode, and the protobuf runtime enforces
`runtime >= gencode` — so every worker process crashed at import, before it ever reached
`infer_worker.py`'s own code.

**Fix.** A third pip phase (`Dockerfile.worker:36-41`), grep-derived rather than hardcoded so it
keeps working as `requirements.txt`'s pins change: re-install exactly the `protobuf` line from
`requirements.txt`, with `--no-deps`, after the OTel phase. `--no-deps` matters — it tells pip to
just install the pinned wheel rather than re-litigating the same conflict it just "solved" by
downgrading. This direction (a newer protobuf runtime satisfying older gencode, i.e.
`opentelemetry-proto`'s own bundled stubs) is the one protobuf actually guarantees works; the
downgrade direction is not.

**Lesson.** A two-phase, or any ordered, dependency install is a well-known way to silently downgrade
a package a *later* phase's resolver has no way of knowing is load-bearing somewhere the current
phase can't see. Whenever an install step could plausibly touch a version that matters elsewhere,
re-assert that version explicitly afterward rather than trusting the last resolver to have kept it
consistent — it had no way to know. And practically: a crash-looping container's actual reason is
almost always sitting in its own logs, in full, immediately — reach for `docker compose logs
<service>` before forming any hypothesis about what broke.

## 6. Chaos against real containers

**Architecture.** `docker-compose.yml` (project name `dsys-chaos`, deliberately distinct from
`docker-compose.observability.yml`'s default project name so `docker compose down --remove-orphans`
in one stack cannot delete the other's containers, `docker-compose.yml:25-34`) brings up a 3-node
`raftkv` cluster (`kv0`/`kv1`/`kv2`, each on its own named volume), one `sched`, one `gateway`, and
three Python `infer_worker.py` containers, all built from two images: `dsys-go:local`
(`docker/Dockerfile.services`, one multi-stage build producing every `cmd/` binary, run as
distroless with health checks executed via the repo's own compiled `kvctl`/`schedctl` binaries in
exec form since there is no shell to run a shell-form healthcheck against — `Dockerfile.services:17-
25`) and `dsys-worker:local` (`docker/Dockerfile.worker`, war story 4's three-phase pip install).

**The `chaosbox` netns-sharing trick.** `kv0`/`kv1`/`kv2` are distroless — no shell, no `tc`, nothing
to inject network chaos *from*. `chaosbox` (`docker-compose.yml:136-149`) is a full `nicolaka/
netshoot` image with `network_mode: "service:kv1"`, which makes its network namespace *literally be*
`kv1`'s — same `eth0`, same IP, same routes. `cap_add: [NET_ADMIN]` on `chaosbox`, not on `kv1`,
means the fault-injection capability lives entirely in a throwaway sidecar rather than widening the
attack surface of every production-shaped service container. Running `tc qdisc add dev eth0 root
netem delay 300ms loss 5%` inside `chaosbox` changes `kv1`'s real traffic, verified by measurement
rather than assumed (§8).

**Scenario list, and what each proved** (`scripts/chaos_docker.sh`):

1. **Baseline** — a `kvctl` put/get round trip, a `schedctl submit`, a small `llmbench` run, each
   polled/retried rather than fixed-slept for, since the Python workers need a moment after
   container start to import grpc and complete their first registration.
2. **Partition** — the current Raft leader is found by grepping `docker compose logs`' merged, `-v`
   output for the `"[nX tY leader]"` line every replica prints on heartbeat, disconnected from the
   `dsysnet` bridge with `docker network disconnect`, and the check is that the *remaining* two-node
   set still accepts writes and read-your-writes while the third sits outside the network — a live,
   container-level demonstration of the same majority guarantee war story 1 was really about.
   Healing uses `docker network connect --alias`, explicitly with `--alias` restored, because a plain
   reconnect done by hand does not restore the compose-assigned DNS alias the way `docker compose up`
   does automatically.
3. **Crash** — `docker compose kill kv2` (SIGKILL, harsher than a graceful stop), then `docker compose
   up -d kv2` against the *same* named volume, checking that pre-crash data is still readable both
   from the cluster generally and from `kv2` directly once it rejoins.
4. **Bonus network chaos** — the `chaosbox` netem injection above, measured before/after: a `kvctl`
   round trip to `kv1` went from baseline latency to **1.6s -> 7.9s** under 300ms injected delay plus
   5% loss, confirmed by an explicit numeric comparison (`awk` check that `after_ms > before_ms`)
   rather than eyeballing the log.

**Two scenarios explicitly not achieved, reported honestly rather than faked — framed as scope
boundaries, not failures.**

- **Clock skew.** All containers on one Docker Desktop host share a single kernel and, absent a
  private VM per container, a single wall clock; Linux time namespaces exist but Docker does not put
  container PIDs in a private one by default, so skewing one container's clock from inside Docker's
  isolation model is not possible without skewing the whole host — including the chaos script's own
  clock. The script's comment (`chaos_docker.sh:212-226`) states this plainly and points at the
  actual answer: true clock skew belongs in the in-process Go harness, which can inject a fake clock
  (Phase 4's `Options.Clock` pattern) inside the process instead of fighting Docker for a real one —
  and this repo's `chaos/` package does not currently do that either, which §9 names as a real gap.
- **Disk pressure.** The first attempt used a single-node throwaway "cluster" (`-peers` pointing only
  at itself) as a shortcut to fill a small volume quickly. It never worked: the node's own Raft log
  showed it cycling `candidate -> candidate` forever, term after term, never becoming leader, so
  every `kvctl` write failed with "not leader" and **no disk pressure was ever actually applied**.
  That is a real, separate finding worth stating precisely: a single-node Raft cluster's "majority"
  is one node out of one, which is mathematically a majority and *should* be able to elect itself —
  so something else was wrong in that particular improvised single-node setup, not a general claim
  that one-node Raft clusters cannot work. The team's own footnote in the script
  (`chaos_docker.sh:253-262`) records switching to a 3-node throwaway cluster instead (each on its
  own 8 MiB tmpfs-backed volume, `-maxraftstate 0` so the log cannot compact itself out of trouble),
  which *did* elect a leader and showed real leader-flapping instability under a write hammer against
  a tiny volume — reported as observed behavior, not asserted as a confirmed `ENOSPC` finding, because
  the script's own check (`chaos_docker.sh:295-306`) deliberately enforces only that the experiment
  ran end to end against a real size-capped volume and produced an inspectable result, not that any
  particular failure mode occurred.

## 7. Observability

**Architecture.** Two independent paths, by design, and the design choice is stated directly in
`docker/observability/otel-collector-config.yaml:1-11`: **traces** go gateway/sched/worker -> OTel
SDK -> OTLP/gRPC -> `otel-collector` -> Jaeger (which speaks OTLP natively as of 1.35+, so no
separate Jaeger exporter component is needed in the collector). **Metrics** go the other way:
Prometheus scrapes each Go process's `client_golang` `/metrics` endpoint *directly*
(`docker/observability/prometheus.yml:1-3`), never through the collector. The reason is exactness,
not just convention: routing metrics through the collector as well would create two candidate paths
for the same counter to be observed and would risk double-counting or duplicate-scraping under any
collector restart or pipeline misconfiguration — keeping exactly one producer and one scrape target
per metric is a stronger guarantee than "the collector probably dedupes correctly." `obs.InitTracing`
(`obs/tracing.go:73-106`) makes an empty `-otlp-endpoint` a genuine no-op — no exporter, no
background goroutines, every span dropped immediately — so instrumentation call sites are safe to
leave in every binary and every test regardless of whether a collector is running.

**Span names, verbatim from `obs/tracing.go`'s doc comment (`:13-36`).** Gateway, one trace per
`Generate` call:

```
gateway.Generate                 (root span; attrs: tenant, prompt_chars)
  gateway.ratelimit               (attrs: tenant, limited)
  gateway.cache_lookup            (attrs: cache_hit)
  gateway.route                   (attrs: worker, prefix_routing)
  gateway.attempt                 (attrs: worker, hedge, hedge_won) — one per attempt (primary + any hedge)
```

The Go worker (`dsys/infer/mock`) and the Python worker both use a gRPC client/server whose stats
handler is `obs.GRPCDialOption`/`obs.GRPCServerOption` (`obs/grpc.go`), so their own spans —
`infer.v1.Inference/Generate` on the wire, surfacing as `infer.Generate` on the Go side and
`worker.Generate` on the Python side — nest under `gateway.attempt` automatically via W3C
trace-context propagation over gRPC metadata, with no manual span-linking code anywhere. Scheduler,
one trace per RPC, confirmed against the actual `tracer.Start` call sites in `sched/queue.go`:
`sched.Submit` (`:178`), `sched.Claim` (`:320`), `sched.Heartbeat` (`:426`), `sched.Complete`
(`:461`), `sched.Fail` (`:495`), `sched.Reap` (`:562`).

**Metric names, verbatim from `obs/metrics.go`'s doc comment (`:7-32`), stable by declaration — "the
Grafana dashboard and k6 checks depend on these; do not rename without updating docker/observability/
grafana and k6/*.js".** From the gateway (scraped off `-metrics-addr`, default `:9091`):
`gateway_requests_total{tenant,cached,hedged}`, `gateway_rate_limited_total{tenant}`,
`gateway_ttft_ms` (histogram), `gateway_cache_hit_ratio` (gauge), `gateway_live_workers` (gauge),
`gateway_hedges_launched_total`, `gateway_hedges_won_total`, `gateway_routed_total{worker}`. From the
scheduler (`:9092`): `sched_jobs_submitted_total`, `sched_jobs_completed_total`,
`sched_jobs_failed_total`, `sched_jobs_fenced_total`, `sched_queue_depth` (gauge). Both processes get
the standard `promauto`-registered process/Go collectors for free, against a fresh, non-default
registry each — nothing here fights the default global registry any other package might use.

**The Grafana dashboard** (`docker/observability/grafana/dashboards/llm-serving.json`), ten panels:
request rate by tenant, TTFT p50/p95/p99, cache hit ratio, rate-limited requests/s by tenant, live
workers, hedge launch/win rate, per-worker request distribution, hedged vs. non-hedged requests/s,
scheduler queue depth, and scheduler submitted/completed/failed/fenced per second — each panel a
direct read of one or more of the metric names above (e.g. `gateway_cache_hit_ratio`,
`gateway_live_workers`, `sched_queue_depth` as the simplest single-series examples).

**The k6 load test** (`k6/llm_load_test.js`), using k6's stable `k6/net/grpc` module's server-
streaming support to drive the *real* `gateway.v1.Gateway/Generate` RPC end to end — rate limit,
cache, route, hedge, stream — rather than falling back to a unary stand-in. Traffic mirrors
`scripts/e2e_llm.sh`'s pattern: three tenants, six ~600-character prompt prefixes repeated across
requests so prefix-cache routing and hedging have something real to show, a `ramping-vus` executor
stepping 0 -> 20 over 10s, holding 20 for 30s, ramping back to 0 over 10s (`:53-64`). Thresholds:
`ttft_ms` p95 < 3000ms (a deliberately generous, non-SLO bound, noted as biased toward whatever
trailing `sleep()` the VU happens to be running per the file's own comment on k6's async event
delivery, `:32-40`), `grpc_req_duration` p95 < 5000ms (the trustworthy latency signal, unaffected by
that bias), and `checks` rate > 0.95.

**Result: 552 checks at 100% pass rate; the p95 latency threshold correctly FAILED at ~10.6s.** This
is a good result, not a broken script, and worth being able to defend precisely in an interview: the
run drove up to 20 concurrent VUs against only **two** mock inference workers (a deliberately small
fleet), so `grpc_req_duration`'s p95 threshold failing means the system was doing exactly what a
correctly-functioning rate-limit-free, admission-queue-free serving layer *should* do when demand
exceeds capacity — requests queue behind two workers' worth of throughput and the tail grows, visibly
and measurably, rather than the system silently dropping load or lying about its latency. A threshold
that always passes regardless of load tells you nothing about where the system's capacity actually
runs out; a threshold that fails exactly when you've deliberately under-provisioned is doing its job.
The 552 checks staying at 100% (stream completed without error, got at least one token, stream
reached done) simultaneously proves the *correctness* of every one of those slow requests — nothing
was dropped, corrupted, or silently truncated by the backpressure, it was just slow, which is the
right failure mode for a serving layer under load to have.

## 8. How we know it works

**Tier 1 — the in-process harness.** Proves the invariants (KV linearizability, scheduler exactly-
once, no panics/no goroutine ever blocked past the run's deadline) hold under simultaneous, seeded,
adversarial network and process faults, in seconds, at zero infrastructure cost, under `-race`. It
cannot, by construction, catch a bug in an actual process boundary, an actual container image, or an
actual dependency install — that is what tiers 2 and 3 are for.

- **Exit criterion**: 12 fixed seeds x combined KV+scheduler chaos (5 Raft nodes, 6 clients, a
  nemesis event roughly every 25-30 ops, a mix of partitions/pauses/crashes bound by the majority
  rule), zero violations, entirely under `-race` (`TestManySeeds`, `chaos_test.go:41-59`). Plus a
  gateway-mix composition run proving the whole platform, not just the KV/scheduler pair, survives
  the same chaos at once: seed 999, 4 seconds, 3 workers, zero panics
  (`TestGatewayMixSurvivesChaos`, `:64-79`).
- Full suite, `go test -race -count=1 ./chaos/...`: the chaos package total is **~77 seconds** for
  `TestQuietRun` + `TestManySeeds` + `TestGatewayMixSurvivesChaos` +
  `TestChooseEventDeterministicGivenFixedState` + `TestFirstNemesisEventIsReproducible`.
- A hand-run `simrun -seed 42 -duration 5s -nemesis-interval 20`: **603 KV ops, 21 nemesis events**
  (a mix of partition/heal/crash/pause hitting all 5 nodes) in **5 wall seconds**, PASS.
- The war story 1 measurement, restated as evidence: before the majority-bound fix, seed 2
  reproduced deterministically at **49.48s wall time**, failing with `scheduler: could not read
  stats: context deadline exceeded`. After the fix: **5.1s**, PASS, logging "cluster answered again
  after 2 probe(s)."
- Full repo regression after every Phase 6 change: **18 packages, all `ok`**, including `dsys/raft`
  at **~269 seconds** (the long Raft suite) — unaffected by the `simnet` seeding refactor, zero
  regressions across every downstream consumer: `kv/raftkv`, `shardctrl`, `shardkv`, `sched`, all
  still pass. This is the direct evidence that §2's backward-compatibility requirement held.

**Tier 2 — Docker chaos.** Proves the same class of guarantee (majority survives a minority failure,
data survives a crash and restart) against real OS process boundaries, real container lifecycle
events (SIGKILL, `docker compose up -d`, `docker network disconnect`/`connect`), and a real Docker
bridge network — exactly where war stories 3 and 4 lived, neither of which the in-process harness
could ever have found, because neither is a distributed-systems bug: one is a build-tool naming
collision, the other is a packaging/dependency-resolution bug. `scripts/chaos_docker.sh`'s final
clean run: **14/14 checks passed in ~248s engine time (~4.5 minutes wall)**. Partition (dynamic
leader detection via log grep, disconnect, confirm majority still serves, heal, confirm rejoin),
crash (SIGKILL + `docker compose up -d`, confirm data survives), and the bonus `tc netem` latency/
loss injection via the `chaosbox` sidecar (measured round trip **1.6s -> 7.9s** under injected 300ms
delay + 5% loss) were all verified against live containers, not asserted. Two scenarios explicitly
not achieved and reported honestly rather than faked: clock skew (shared VM clock, §6) and a clean
disk-pressure repro (the single-node-cluster dead end and the 3-node throwaway cluster's real, if
unconfirmed-as-ENOSPC, leader-flapping instability, §6).

**Tier 3 — observability.** Proves the instrumentation is real, not decorative, by driving actual
traffic through actual processes and reading the result back out of actual trace/metric backends.
Jaeger showed a full nested trace `gateway.v1.Gateway/Generate -> gateway.Generate ->
{gateway.ratelimit, gateway.cache_lookup, gateway.route, gateway.attempt} -> infer.v1.Inference/
Generate -> worker.Generate` (the Python worker), plus the `sched.Submit`/`sched.Claim`/
`sched.Complete`/`sched.Reap` spans, all correctly nested via W3C trace-context propagation with no
manual linking code. Non-zero Prometheus counters were confirmed via `curl` on `:9091/metrics` and
`:9092/metrics` after driving traffic with `llmbench`/`schedctl`. The k6 load test (via the official
`grafana/k6` Docker image, k6's stable `k6/net/grpc` module) ran a 0 -> 20 -> 0 VU ramp over ~56s,
producing **552 checks at 100% pass rate**, while the configured p95 latency threshold correctly
**failed (~10.6s)** because only two mock workers served 20 concurrent VUs — a meaningful, expected
threshold failure demonstrating real backpressure under deliberate under-provisioning, not a broken
script (§7).

Each tier proves something the others structurally cannot: tier 1 gives you thousands of seeded
adversarial scenarios per minute but never touches a real process boundary; tier 2 gives you real
process/container/OS-level failure modes but at the cost of minutes per run and no seed-level
reproducibility; tier 3 gives you the ability to *see* what either of the other two is doing, and to
apply load patterns (a VU ramp) that neither tier 1's fixed-concurrency workload nor tier 2's short
scripted scenarios attempt.

## 9. What is NOT implemented, on purpose

- **True deterministic simulation (virtual time, single-threaded execution).** The big one, and §3
  and war story 2 already give the honest account: this harness pins the workload and the network
  faults, and pins the nemesis's decision function given fixed state, but the live nemesis schedule
  and every real timer (Raft's election/heartbeat, the nemesis's own auto-heal/restart sleeps) run on
  the real wall clock and the real Go scheduler. Getting to FoundationDB/TigerBeetle-style bit-for-
  bit replay requires virtualizing the clock and single-threading the system under test — a
  materially harder design commitment, not an incremental addition on top of this one. See Exercise
  (a).
- **No automatic bisection of a failing seed to find a minimal repro.** When `simrun -seed N` fails,
  the repro command it prints (`cmd/simrun/main.go:87-88`) reruns the *entire* scenario — same
  duration, same nemesis interval, same everything — which reproduces the failure (modulo §3's live-
  schedule caveat) but does nothing to shrink it to the smallest interesting case. Exercise (b).
- **No chaos scenario library beyond partition/crash/pause.** No Byzantine faults (a node that lies
  about its state rather than merely going silent or slow), no bit-flip/corruption-style faults on
  stored data or in-flight messages. Every fault this harness injects is a *crash-stop* or *omission*
  fault in the classical sense; nothing here tests behavior against an adversarial or corrupted
  participant. Exercise (c).
- **No alerting configured on the Grafana dashboard.** The ten panels (§7) are read-only visibility;
  nothing pages, emails, or even highlights a threshold breach automatically. Exercise (d).
- **k6 tests only the gateway's gRPC surface**, not the scheduler's. `sched/grpcserver`'s `Submit`/
  `Claim`/`Heartbeat`/`Complete`/`Fail` RPCs have no load-test coverage analogous to
  `k6/llm_load_test.js`. Exercise (e).

## 10. Exercises

**(a) Virtualize time for a true deterministic-simulation upgrade.** Sketch what would need to
change, concretely, starting from `raft.Config`'s `HeartbeatInterval`/`ElectionTimeoutMin/Max`
(`raft/simnet` construction in `harness.go:56-63`): every `time.NewTicker`/`time.After`/`time.Sleep`
call inside `raft`'s election/heartbeat logic, inside `simnet.call`'s delay injection, and inside
`nemesis.apply`'s auto-heal/restart goroutines, would need to read from an injected, advanceable
clock (Phase 4's `Options.Clock` pattern, generalized from "read the time" to "the simulation
explicitly advances virtual time forward as its own primary loop action") rather than the real
`time` package. Then confront the harder half: as long as Raft's own goroutines and `simnet`'s own
goroutines are real OS goroutines scheduled by the real Go runtime, "which one runs next" is still
nondeterministic even with a virtualized clock — true reproducibility needs the whole system stepped
by one simulated event loop you control, which likely means restructuring `raft.Handler`'s dispatch
away from "one goroutine per in-flight RPC" toward something a single-threaded scheduler can drive
deterministically. Estimate how much of `raft/raft.go` and `raft/simnet/simnet.go` that touches.

**(b) Automatic seed minimization/bisection for a failing run.** Given a failing `(seed, Scenario)`,
write a search that shrinks `Scenario.Duration`, `NemesisInterval`, `NumKVNodes`, and `NumClients`
while re-running and checking whether the same class of violation still reproduces, converging on
the smallest scenario that still fails. Decide what "same class of violation" means precisely enough
to automate the comparison (exact violation string? category — "kv" vs "scheduler"? node count
involved?), since §3's honesty section means a shrunk run is not guaranteed to hit the identical
nemesis schedule even at the same seed.

**(c) Add Byzantine or corruption-style faults to the nemesis.** Extend `NemesisEventKind`
(`chaos/api.go:70-75`) with something that does not fit the crash-stop/omission model — a node that
gob-encodes a *corrupted* `AppendEntriesArgs` before sending, or an `EventKind` that flips bits in a
committed log entry on disk. Work out what invariant such a fault would actually test (Raft's
integrity checks around log matching, most likely) and whether it can be injected inside `simnet`'s
existing `gobCopy` step (`simnet.go:182-195`) or needs a new hook.

**(d) Wire Grafana alerting on the metrics that already exist.** `gateway_rate_limited_total`,
`sched_jobs_fenced_total`, and `gateway_ttft_ms`'s p99 (via a Prometheus recording rule) are three
reasonable starting alert conditions with zero new instrumentation required — the metrics already
exist (§7). Add Grafana alert rules and a notification channel, and decide sensible thresholds by
looking at this phase's own measured numbers (§8) as a baseline for "normal."

**(e) Extend k6 to the scheduler's gRPC surface.** Write a `k6/sched_load_test.js` alongside
`k6/llm_load_test.js`, driving `Submit`/`Claim`/`Heartbeat`/`Complete` against a running `sched`
process, with thresholds on `sched_queue_depth` staying bounded and on submit/claim latency. Decide
what "checks" mean for a queue under load the way `llm_load_test.js:106-110` defines them for a
generate stream.

**(f) Run `simrun` for an hour unattended overnight and see whether any seed in a huge range ever
fails.** A simple driver loop over, say, seeds 1-10000 at a few seconds each easily fits in an hour.
Describe what you would do with a failure if one showed up: reproduce it standalone with `simrun
-seed <n>` first (war story 1's exact methodology), read the printed event log before forming any
hypothesis, and only then decide whether it needs Exercise (b)'s bisection — which is not automated
yet, so today that means manually varying duration/nemesis-interval/node count by hand the way war
story 1's investigation did.

## 11. Interview questions with model answers

1. **What is deterministic simulation testing, and why do FoundationDB and TigerBeetle use it?**
   Running the entire system — or a faithful model of it — inside one process, driven by a simulated
   network and a simulated clock, so that a single integer seed reproduces one exact execution byte
   for byte, forever. The payoff is that a rare, timing-dependent distributed-systems bug — the kind
   that takes days to reproduce on real infrastructure — becomes something you can hit thousands of
   times a minute, print counterexamples for, and step through in a debugger like any deterministic
   unit test. FoundationDB and TigerBeetle build their core logic to run under either real I/O or a
   simulator as a first-class constraint from day one, because that constraint (single-threaded,
   simulated time) has to be designed in from the start — bolting it onto an already-concurrent,
   real-time system afterward is a much larger rewrite, which is exactly why this phase's harness
   (§3, §9) does not attempt it and states precisely where it falls short instead.

2. **What's the difference between fuzzing with a seed and true deterministic replay?** Fuzzing with
   a seed reproduces the *inputs* or the *decisions* a pseudo-random generator made — which key, which
   fault, which node — given the same seed and the same call order. It does not control *when* those
   decisions land relative to everything else happening concurrently, because the rest of the system
   still runs on the real clock and the real scheduler. True deterministic replay additionally
   virtualizes time and single-threads execution so that the *interleaving itself* is pinned, not just
   the random choices feeding it. This phase's harness is squarely the first kind — `raft/simnet`'s
   `NewSeeded` (§2) pins fault decisions exactly, and `chaos/api.go`'s honesty section (§3) is the
   precise account of exactly how far that goes and where it stops.

3. **Why does a chaos nemesis need to respect quorum/majority bounds instead of just injecting faults
   freely?** Because a consensus protocol's safety and liveness guarantees have a stated failure
   threshold — Raft tolerates any *minority* of simultaneous node failures, never more — and a
   nemesis that pushes the cluster below that threshold is not testing the system's fault tolerance
   at all, it is testing whether the system correctly refuses to make progress with no quorum, which
   it will, always, by design, no matter how long you wait. War story 1 is the concrete version of
   this: three of five nodes partitioned with no heal ever drawn produced a "violation" that was
   actually Raft behaving exactly as specified. The fix, `chooseEvent`'s `roomToTakeDown` bound
   (§4), makes Partition/Pause/Crash eligible only when enough nodes would remain to keep quorum —
   so every fault the nemesis injects is one the system is actually supposed to survive.

4. **Walk me through how you'd debug a flaky distributed test.** War story 1's methodology, in
   order: don't patch the symptom with a guess (a fixed sleep) — verify the real cause with a
   targeted fix first (each check phase gets its own fresh timeout, since a shared clock lets one
   slow, unrelated phase starve the one after it). If the failure persists after that fix, don't
   assume the first fix was wrong; reproduce the failure *standalone*, outside the test framework,
   with the smallest tool that shows you the raw sequence of events (`cmd/simrun` and its printed
   event log, here). Read that log before forming a new hypothesis. Here it directly showed three of
   five nodes partitioned with no heal ever drawn — the system was behaving correctly, and the real
   bug was in the harness's own fault-injection bound, not the thing under test. The general
   discipline: a "violation" your test reports might be your test asking an impossible question, and
   the only way to find out is to look at what actually happened, not to keep changing timeouts.

5. **How would you extend this harness toward true determinism?** Two structural changes, not one
   incremental tweak (§3, §9, Exercise a): virtualize every clock read in the path — Raft's election/
   heartbeat timers, `simnet`'s delay injection, the nemesis's auto-heal/restart sleeps — behind an
   injected, advanceable clock instead of the real `time` package; and single-thread the system so
   that "what happens next" is chosen by a simulated event loop you control rather than the real Go
   scheduler picking among ready goroutines. The second is the expensive one, because it likely means
   restructuring how Raft and `simnet` dispatch concurrent work, not just adding a fake-clock
   parameter.

6. **Why put traces through a collector but scrape metrics directly?** Traces benefit from a
   collector as a fan-in point — batching spans from multiple services before forwarding to a single
   trace backend (Jaeger here) is exactly what a collector is for, and there is no natural "scrape"
   model for push-based, per-request span data anyway. Metrics are pull-based counters that already
   have a natural single source of truth (the process's own `/metrics` endpoint); routing them
   through the collector as well creates a second path the same number could travel, and any
   collector-side misconfiguration or restart timing risks double-counting or duplicate scraping.
   Keeping exactly one producer and one scrape target per metric (`docker/observability/
   otel-collector-config.yaml:1-11`, `prometheus.yml:1-3`) is a stronger, simpler guarantee than
   trusting a shared pipeline to dedupe correctly.

7. **What does a k6 threshold failure under deliberately under-provisioned workers tell you — is it a
   good or bad test result?** Good, here, and you should be able to say precisely why: the run
   deliberately pointed 20 concurrent VUs at only two mock inference workers, so a rising p95 request
   duration is the correct, expected signature of a serving layer queueing under real backpressure
   rather than silently dropping or corrupting load — confirmed by the 552 checks (stream completed
   without error, first token arrived, stream reached done) staying at 100% throughout. A threshold
   that always passes regardless of how hard you push tells you nothing about where capacity actually
   runs out; a threshold that fails exactly when you've engineered scarcity is doing its job. The
   failure would be *bad* only if it showed up unexpectedly against a fleet sized for the load, or if
   it came with dropped/corrupted requests rather than merely slow ones.

8. **How do you chaos-test a system that runs across multiple hosts versus one that runs in one
   process?** In one process (this phase's `chaos/` harness), you get seed-level reproducibility for
   the parts you can control (workload, network faults, §3's tiers 1-2) and can run thousands of
   scenarios per minute, but you cannot exercise real process-boundary failures — a real SIGKILL, a
   real container restart preserving a real volume, a real dependency-resolution bug in a real image
   build. Across real hosts or containers (this phase's Docker layer, §6), you get exactly those real
   failure modes — and this phase's own two build/packaging war stories (3 and 4) only exist because
   someone tested against real containers — at the cost of losing seed-level determinism (real
   `docker kill` timing, real network stack behavior) and paying minutes instead of seconds per run.
   The right answer for most systems is both, in the tiered order §8 uses them: cheap in-process
   chaos for volume and fast iteration, expensive real-infrastructure chaos for the failure modes only
   real infrastructure produces.

9. **What's the risk of a two-phase (or any ordered) dependency install, and how do you defend
   against it?** Splitting an install into ordered phases to dodge a resolver conflict means each
   later phase resolves only its *own* packages against whatever is already installed — it has no
   visibility into whether downgrading something to satisfy its own constraints breaks a package a
   different part of the system depends on. War story 4 is the concrete case: an OTel dependency
   phase silently downgraded `protobuf` below the version the generated gRPC stubs were compiled
   against, and the runtime's own `runtime >= gencode` enforcement turned that into a crash-loop at
   import time in every worker container. The defense is to re-assert, explicitly and with
   `--no-deps`, the exact version of anything load-bearing immediately after any install step that
   could plausibly have touched it — don't trust the last resolver in the chain to have known what
   the first one needed.

10. **Tell me about a bug you found where your own test, not the system, was wrong.** War story 2.
    After fixing the majority-bound bug (war story 1), a test asserting that the same seed reproduces
    the same *whole-harness nemesis event sequence* started failing unpredictably. The instinct is to
    assume the new fix introduced a regression — but the actual cause was that the fix correctly made
    `chooseEvent` consult *live* cluster state to enforce the majority bound, and live state's timing
    is not, and should not be, seeded: pinning it would mean the safety check stops reflecting the
    cluster's real condition. The test's claim — "the whole live schedule is reproducible" — was
    simply false once the system did the more-correct thing; it had only ever passed by accident,
    before the majority bound existed to make the nemesis's choices depend on real timing at all. The
    fix was to delete that test's claim and replace it with two narrower ones that are actually true
    at the layer they claim to be true at (`TestChooseEventDeterministicGivenFixedState` for the pure
    decision function, `TestFirstNemesisEventIsReproducible` for the one live-schedule guarantee that
    always holds) — and to write down, in the package doc, exactly what degree of determinism exists
    at each layer, so nobody re-adds the stronger, false claim later.

## 12. Your notes

Answer these in your own words, here, before starting Phase 7.

1. `chaos/api.go`'s honesty section states four tiers of decreasing determinism: workload, network
   faults, nemesis decision function, live nemesis schedule. For each tier, name the one concrete
   piece of code that pins it (or fails to), and the one test that proves the claim at that tier —
   then say, in one sentence per tier, what would break if that tier's pinning were removed.
2. War stories 1 and 2 are, underneath the surface narrative, the same lesson applied twice in
   opposite directions: story 1 is "the test was too permissive" (no majority bound) and story 2 is
   "the test was too strict" (assumed a reproducibility guarantee reality couldn't supply). Write one
   paragraph on how you would recognize, in a *new* flaky-test situation you haven't seen before,
   which of these two shapes you're looking at — what evidence points to "the system's real behavior
   changed and the old test's assumption is now false" versus "the harness is asking an impossible
   question."
3. Sketch, concretely, what `waitClusterAvailable` would need to look like as a reusable helper for
   the Docker chaos script (`scripts/chaos_docker.sh`) instead of the in-process harness — what
   changes about "probe the cluster" when the probe is a `docker compose run` subprocess instead of
   an in-process `Get` call, and what would you replace the 50ms poll interval with given that a
   subprocess launch alone costs real wall time.
4. The benchmarking discipline from Phase 5 §9.3 ("a benchmark that cannot distinguish the thing it
   measures is worse than no benchmark") applies to k6's threshold design too. `llm_load_test.js`'s
   p95 threshold is deliberately generous (<3000ms) precisely so a healthy run passes it and an
   under-provisioned run (like the measured 10.6s result) fails it clearly. Write down what threshold
   values would make this test useless in each direction — too loose to ever fail, and too strict to
   ever pass on this mock backend — and where in that range you'd actually set an SLO-shaped
   threshold for a real model server.
5. In one paragraph: this phase's exit criterion is "one command reproduces any failure from a seed."
   Given everything in §3, is that criterion actually met, partially met, or overclaimed by the
   roadmap's original wording? Defend your answer by naming which specific failures `simrun -seed N`
   can and cannot reliably reproduce, and connect it back to war story 2's decision to state the
   scope precisely rather than either over- or under-claim it.

## Reading

- Will Wilson, "Testing Distributed Systems w/ Deterministic Simulation" (Strange Loop 2014) and the
  FoundationDB simulation testing blog series — the design this phase's `raft/simnet` seeding is a
  first, partial step toward, and §3's contrast section is directly answerable from these.
- TigerBeetle's `VOPR` (deterministic simulation fuzzer) design docs — a second, independently-
  arrived-at version of the same idea, useful for seeing what a from-scratch design that commits to
  virtual time and single-threading from day one looks like end to end.
- Kyle Kingsbury's Jepsen reports (any of them, e.g. the etcd or the MongoDB series) — the nemesis
  shape (partition/pause/kill, healed before checking, a linearizability checker run afterward) this
  phase's `chaos/` package deliberately mirrors, including the "heal everything before you check"
  convention `harness.go:201-211` names directly.
- The OpenTelemetry spec's "Trace Context" (W3C) section — what makes `gateway.attempt`'s span
  automatically parent `infer.Generate`/`worker.Generate` across a real gRPC hop with no manual code.
- Grafana k6's `k6/net/grpc` module docs, specifically its server-streaming support — what
  `llm_load_test.js` actually exercises versus a simpler unary load test.
