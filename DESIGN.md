# dsys — design document

A distributed LLM inference platform built from the write-ahead log up: a Raft-replicated, sharded
control plane; a lease-and-fencing job scheduler; a stateless serving gateway with distributed rate
limiting, semantic caching, prefix-aware routing and hedging; and a chaos harness that checks the
whole thing with a linearizability checker.

This consolidates seven phases. Each layer's full reasoning, war stories and test inventory are in
`docs/phase1.md`–`docs/phase6.md`; this document states the architecture, the guarantees and the
decisions, and points there for depth. Numbers are in `BENCHMARKS.md`; the narrative is
`docs/phase7.md`.

---

## 1. What this is, and what it is not

**What this is.** A working distributed system in Go and Python with no consensus libraries. Raft —
elections, replication, persistence, snapshots — is implemented from the paper. On top of it: a
linearizable replicated KV, a shard controller and sharded KV groups with live migration, a job
scheduler built purely from compare-and-swap, and a serving gateway doing admission control,
caching, placement and tail management. It is verified the way distributed systems are supposed to
be: Porcupine against concurrent histories recorded under fault injection, a seeded chaos harness,
exactly-once checks under thousands of injected crashes and pauses, and real-container chaos with
`tc netem`. The whole repository passes `go test -race` clean.

**Two inference backends, and which one produced a number matters.** The default is a mock
(`py/infer_worker.py --backend mock`), which generates pseudo-words from a fixed word list against a
deliberate, written-down latency model: prefill linear in the number of *uncached* prompt
characters, a prefix cache of 64-character blocks, a fixed per-token decode delay, optional injected
tail stalls (the Go twin is `infer/mock/mock.go:8-19`). It is the default because it makes the
benchmarks deterministic and needs no weights, and **every Phase 5 and Phase 6 serving number came
from it.**

The second is real: `--backend hf` (`py/hf_backend.py`) runs an actual HuggingFace causal LM,
default SmolLM2-135M-Instruct, with genuine prefix KV-cache reuse — a request finds the longest
cached block-aligned token prefix, reuses those `past_key_values`, and prefills only the remaining
suffix. That is the mechanism behind vLLM's automatic prefix caching and SGLang's RadixAttention,
and it means a reported `prefix_cache_hit` is computation actually skipped rather than a modelled
flag. It is tested end to end through the gateway and measured in `BENCHMARKS.md` ("Real model"),
where the Phase 5 routing result reproduces against real weights, hot-spot tradeoff included.

So the honest framing is still *LLM serving infrastructure* rather than a model server — the real
path runs one request at a time per worker with no batching and no paged KV sharing, on a 135M model
on CPU. It demonstrates that the premise the mock encodes is true; it is not a throughput claim.

**Not production-grade.** It is built with production *practices* — race-detector-clean tests, chaos
engineering, linearizability checking, distributed tracing, Prometheus metrics, failure testing
against real containers — and has none of the things that make software production software. It has
never served real traffic. There is no authentication and no TLS: every connection is
`insecure.NewCredentials()` and the tenant is a client-supplied string, so any client can spend any
tenant's quota. Raft and shard-transfer traffic does at least have its own listeners, separate from
the client API: they used to share a port, and a forged vote request sent to a client port deposed
the leader. With no authentication anywhere, the peer ports must sit on a private network. No
security review. No multi-machine durability story — every "3-node cluster" here
is three processes fsyncing to one laptop SSD, so one disk failure loses all three replicas. No
deployment, upgrade, on-call or capacity story, no SLOs, and no GC anywhere (Phase 3's shard
snapshots, Phase 4's completed job records and Phase 5's cache entries all leak by design). It has
run for minutes at a time, never days.

**And the benchmarks are single-machine.** One memory-constrained Windows laptop, 8 GB RAM, loopback
or in-process simulation. Absolute numbers are not comparable to any real deployment; the *ratios*
and before/after comparisons are the meaningful part, and `BENCHMARKS.md` is written to be read that
way.

---

## 2. System architecture

```
   clients: py/gateway_client.py, cmd/llmbench, k6/, cmd/kvctl, cmd/schedctl
        │ Generate(prompt) -> stream of Token          │ Submit / Watch
        ▼                                              ▼
 ┌───────────────────────────────────┐      ┌────────────────────────────┐
 │ gateway  (N replicas, STATELESS)  │      │ scheduler (N, STATELESS)   │
 │   gateway/server.go:237           │      │   sched/queue.go           │
 │  1 rate limit   ratelimit/  :268  │      │  Submit / Claim /          │
 │  2 cache        semcache/   :288  │      │  Heartbeat / Complete/Fail │
 │  3 route        router/     :322  │      │  leader-elected reaper     │
 │  4 hedge+stream             :334  │      │   sched/leader.go:89       │
 └────┬──────────────────────┬───────┘      └─────────────┬──────────────┘
      │ Generate (cancellable)│ Get/Put/CAS               │ Get/Put/CAS
      ▼                       │                           │
 ┌──────────────────────┐     │      ┌────────────────────┴───────────────┐
 │ inference workers    │     └─────▶│  kvapi.KV  =  Get, Put, CAS        │
 │  py/infer_worker.py  │            │  kvapi/kvapi.go:17 — the ONE       │
 │  infer/mock (Go)     │            │  interface every layer above the   │
 │  prefix KV cache,    │            │  store programs against            │
 │  prefill, decode,    │            └───────────────┬────────────────────┘
 │  per-token cancel    │      one of these two, interchangeably:
 │  lease-renew into ───┼──────────┐                 │
 │  workers/index       │          ▼                 ▼
 └──────────────────────┘  ┌────────────────┐  ┌──────────────────────────────┐
                           │ raftkv (flat)  │  │ shardkv groups (sharded)     │
  KV keys in use:          │ 3-5 replicas,  │  │ gid=100 {shards…}  Raft grp  │
   ratelimit/<tenant>      │ one Raft group │  │ gid=200 {shards…}  Raft grp  │
   workers/index           │                │  │        ▲ pull shard state    │
   semcache/exact/<sha256> └───────┬────────┘  └────────┼─────────────────────┘
   sched/job/<id>, /head,          │                    │ Query(-1) / Join / Leave
   /tail, /idem/<k>, /leader       │           ┌────────┴─────────────────┐
                                   │           │ shardctrl (own Raft grp) │
                                   │           │ shard -> gid assignment  │
                                   ▼           └──────────────────────────┘
                    ┌─────────────────────────────────────┐
                    │ raft/  — from the paper, no library  │
                    │  election.go replication.go          │
                    │  snapshot.go filepersister.go        │
                    │  raft/simnet (seeded fault network)  │
                    └──────────────────┬──────────────────┘
                                       ▼
                    ┌─────────────────────────────────────┐
                    │ kv/store Machine (log-then-apply)   │
                    │ kv/wal    append-only WAL + CRC32C  │
                    └─────────────────────────────────────┘

 wrapping everything:
   chaos/    in-process harness: real 5-node cluster + scheduler (+gateway) over a seeded
             simnet, nemesis bounded to a minority, Porcupine + exactly-once checks
   scripts/chaos_docker.sh   the same shapes against real containers (partition, SIGKILL, netem)
   obs/      OpenTelemetry traces + Prometheus metrics in gateway and scheduler
```

**WAL and durable store** (`kv/wal`, `kv/store`; depth: `docs/phase1.md`). An append-only file of
length-prefixed, CRC32C-checksummed records with one invariant (`kv/wal/wal.go:8-10`): if `Append`
returned nil, the record survives any crash after that point. Recovery scans (`wal.go:198`) and
truncates to the last good record (`wal.go:96`) — safe only because a torn write can only be the
*last* thing written, so a bad checksum with valid data after it is real data loss and is raised, not
skipped. Above it, a state machine that is a pure function of the log (`kv/store/machine.go:127`),
with the dedup session table (`machine.go:38`) inside it so replay rebuilds it. Group commit
(`kv/store/store.go:259`) lets N writers share one fsync (`wal.go:167`) without ever acking early.

**Raft** (`raft/`; depth: `docs/phase2.md`). Terms, randomized timeouts, the election restriction
(`raft/election.go:92-93`), log matching with conflict-hint backtracking, persistence before every
reply (`raft/raft.go:351`), and snapshots with a sentinel `log[0]` carrying `lastIncludedIndex`/`Term`
so index arithmetic is uniform (`raft/snapshot.go:14`, `:31`, `:47`, `:96`). The rule doing the most
work is `advanceCommitIndex` returning at the first entry not of the current term
(`raft/replication.go:156-158`) — the Figure 8 fix. `docs/phase2.md` §4 catalogues the seven classic
Raft bugs with the line preventing each.

**Replicated KV** (`kv/raftkv`). The *same* `store.Machine`, fed by a Raft log instead of a local
WAL — step 2 of the four-step pattern changed, `Apply` did not. `propose`
(`kv/raftkv/server.go:242`) answers a retry from the session table with no consensus; the applier
(`:173`) wakes the client only if the term at that index still matches what `Start` reported
(`:210-214`), else `ErrLeaderChanged`.

**Sharding** (`shard/`, `shardctrl/`, `shardkv/`; depth: `docs/phase3.md`). Keys map to one of ten
shards (`shard/shard.go:27`, `:33`); a controller — its own Raft group — assigns shards to groups via
a deterministic rebalancer (`shardctrl/machine.go:360`). Each group's log carries three kinds of
entry (`shardkv/raftcommand.go:12-16`) — client ops, config changes, migrations — in one total order,
so "did this write happen before or after the shard moved" has one answer per replica. `applyConfig`
(`shardkv/server.go:264`) freezes a departing shard's data *and dedup sessions* into `outgoing` and
marks an arriving shard `needed`; the gaining group pulls (`:457`) and installs idempotently (`:307`).

**Scheduler** (`sched/`; depth: `docs/phase4.md`). A queue holding no state of its own
(`sched/api.go:1-6`): every record, counter and lease lives in the KV, every mutation a CAS against
the exact bytes read (`sched/queue.go:150`). Workers claim under a lease and carry a fencing token,
`gen`, minted at `:381` and checked by `checkHolder` (`:408`) before every worker write. A
leader-elected reaper (`sched/leader.go:89`) reclaims expired leases — for efficiency only.

**Serving gateway** (`gateway/`, `ratelimit/`, `router/`, `semcache/`; depth: `docs/phase5.md`). Four
stages in a fixed order, each cheaper than the next and each unavoidable (`gateway/api.go:17-40`): a
per-tenant token bucket shared across gateways via CAS (`ratelimit/ratelimit.go:150`), a cache whose
exact index is shared and whose vectors are local (`semcache/semcache.go:7-31`), rendezvous-hash
prefix affinity with load-aware spill (`router/router.go:307`), and hedging with end-to-end
cancellation (`gateway/server.go:373`). N gateways behind one address *are* one gateway.

**Chaos and observability** (`chaos/`, `obs/`, `docker/`; depth: `docs/phase6.md`). One process hosts
a real five-node Raft cluster, a real scheduler and optionally a real gateway with mock workers,
driven by a nemesis bounded so it can never take down a majority (`chaos/nemesis.go:43`, `:61-62`),
checked with Porcupine and an exactly-once check (`chaos/harness.go:237`, `:245-251`).
`scripts/chaos_docker.sh` runs the same shapes against containers. `obs/` wires spans and metrics
into the gateway and scheduler (`obs/tracing.go:13-36`, `obs/metrics.go:3-32`).

---

## 3. The consistency guarantees, per endpoint

The section to read first if you read only one.

| Operation | Guarantee | What enforces it |
|---|---|---|
| `KV.Put` / `Delete` / `CAS` (flat `raftkv`) | **Linearizable.** Takes effect at one point between call and return; an acknowledged write is never lost while a majority survives. | One totally ordered Raft log; commit needs a majority of durable appends; `advanceCommitIndex` counts only current-term entries (`raft/replication.go:156-158`); every node fsyncs before acking (`raft/raft.go:351`). |
| `KV.Get` (flat `raftkv`) | **Linearizable**, not merely "read committed". | The read is proposed as a log entry like a write (`kv/raftkv/server.go:310`). A deposed leader cannot answer from stale local state because its read never commits. Cost: one consensus round and an fsync per node, per read (§4.8). |
| Retry with the same `(client_id, request_id)` | **Effectively-once**: applied at most once; every retry observes that one application's result. | `RequestMeta` inside the logged entry; the session table is updated inside `Apply`, so replay rebuilds it and it survives failover (`kv/store/machine.go:38`, `:100`). Contract: one outstanding request per client identity. |
| KV op on a **sharded** cluster, migration included | **Linearizable for the key's whole lifetime.** Never two owners; never silently unreachable. | Ownership is a pure function of the controller's config; config changes and migrations are entries in the *same* log as client writes (`shardkv/raftcommand.go:12-16`); ownership is re-checked authoritatively at apply time (`shardkv/server.go:238-243`). `ErrWrongGroup` (`FailedPrecondition`, `:617-618`) = re-resolve; `ErrNotReady` (`Unavailable`, `:619-620`) = retry here shortly. |
| Retry whose shard moved mid-flight | **Effectively-once, across the move.** | Dedup sessions belong to the *shard*, not the group: frozen into the outgoing snapshot with the data (`shardkv/server.go:275-282`) and installed by the new owner (`:307`). |
| `shardctrl` Join/Leave/Move/Query | **Linearizable sequence of configurations**, full history queryable. | The controller is its own Raft group; `rebalance` (`shardctrl/machine.go:360`) is deterministic — no map iteration order, no history dependence — so replicas compute byte-identical configs. |
| `sched.Submit` | **At-least-once admission; with an idempotency key, at most one job per key ever runs.** Bounded queue, shed at the door. | An idempotent Submit writes its job STAGED (never claimable), binds the key to that id, then publishes it PENDING and bumps `tail`; a retry that finds the key bound finishes those steps. A half-failed submit can no longer create a second runnable job. Stale staged records are failed after 60 s so they cannot pin `head`. |
| `sched` job execution | **At-least-once.** A job may run more than once. | Nothing prevents it and nothing can: a worker can finish and die before saying so, indistinguishable from never starting (`sched/api.go:30-39`). |
| `sched.Complete` | **Exactly-once commit.** At most one accepted result per job, ever. | The fencing token: `gen` bumped on every change of hands (`sched/queue.go:373`, `:381`, `:512`, `:597`), checked by `checkHolder` (`:408`) before every worker write, made atomic with the write by a CAS against the exact bytes read (`:150`). A same-gen retry of an accepted `Complete` is an idempotent no-op success. |
| End-to-end job side effects | **Effectively-once only if the handler is idempotent on `job.Id`.** | The queue's obligation ends at the commit; the handler's begins. Stated as a disclaimer, not a feature (`sched/api.go:30-39`). |
| `Generate` rate-limit decision | **Never over-admits; may under-admit; fails open when the KV is down (configurable).** | One shared bucket per tenant, updated by CAS. Gateways lease up to 10% of a tenant's burst per CAS and spend it locally (`ratelimit.Options.LeaseFraction`); leased tokens have left the shared bucket, so the fleet can never admit more than the budget, but tokens stranded in another gateway's lease can be refused for up to `LeaseTTL`. Tiny budgets stay exact. A bucket's refill time only moves forward, so clock skew cannot re-credit elapsed time (it once let 4,000 requests through a 220 budget). If the KV is unavailable the limiter fails open (`-rl-fail-open`, default true) so a control-plane outage does not stop inference; contention still fails closed. |
| `Generate` cache lookup | **Exact-match by default; tenant-scoped; a cached answer is always a complete stream.** | Every key is scoped by a hash of the tenant, then looked up as `sha256(normalised prompt)`, a linearizable read. Only a stream that reached `done` is stored, so a truncated answer can never be cached. Near-duplicate matching is opt-in (`-cache-near`): cosine similarity proposes, then a flat 3-character edit budget must confirm. That is lexical, so it cannot see meaning ("cat"/"car" is one edit); earlier, length-scaled versions served "capital of Spain?" the answer for "capital of France?". A cache *failure* never fails the request. |
| `Generate` routing | **Best-effort placement, deterministic given the same live set.** | Rendezvous hashing (`router/router.go:283-284`) — every gateway computes the same answer with no coordination — plus load-aware spill (`:307`). Routing to a dead worker is survivable: the gateway falls back. |
| `Generate` response | **At-least-once execution, exactly-once delivery of one attempt's stream.** | Hedging runs two attempts; the first to produce a token wins and the loser is cancelled (`gateway/server.go:489-495`). A client can never observe an interleaving. Safe only because generation has no side effects. |
| Worker registration | **Best-effort liveness advertisement.** Stale entries are harmless. | A lease renewed by the worker (`py/infer_worker.py:328-329`); no failure detector — absence of renewal *is* the signal. No fence, because a registration grants no exclusive permission and there is nothing to corrupt. |

The distinction that recurs at every layer: **a lease is for liveness, a fence is for safety, and a
CAS is how a fence gets enforced at the resource rather than at the holder.** The job queue needs
both. The rate limiter needs only the fence (a token is *spent*, not held — nothing to reclaim). The
worker registry needs only the lease (nothing is exclusive, so a stale holder can corrupt nothing).

---

## 4. Key design decisions and trade-offs

### 4.1 Raft, not Paxos or Multi-Paxos

**Rejected:** single-decree Paxos (one value, leaderless, two phases) and Multi-Paxos (a
distinguished proposer, gaps and out-of-order acceptance permitted). **Chosen:** Raft from the paper,
no library.

Same fault model and steady-state message complexity; the difference is specification. Multi-Paxos
leaves leader election, log management and reconfiguration underspecified, which is why every
implementation differs. Raft is Multi-Paxos with the decisions made — strong leader, contiguous logs,
log-comparison elections, terms instead of ballots. That matters enormously for a system you intend
to *test*: Figure 2 is a checklist a suite can be written against, and each of the seven classic bugs
maps to a specific line. The cost is real flexibility — no out-of-order commit, all writes through
one leader — which is exactly what Phase 3 then works around by adding more leaders.

### 4.2 Leases over locks — and why a lease alone is not enough

**Rejected:** a plain distributed lock per job; and a longer lease as the fix for zombies.
**Chosen:** a lease for liveness plus a monotonic fencing token for safety.

A lock's holder can die holding it, and nobody can distinguish a dead holder from a slow one. Every
lock service bolts a timeout on for this reason — and the moment it has a timeout it is a lease. But
a lease alone is unsafe: the holder *does not know it was paused*. SIGSTOP, a GC pause, the wrong
side of a partition; the lease lapses without its knowledge; it wakes, finishes, and writes over
whatever the new holder did. A longer lease only makes this rarer — no finite lease survives an
unbounded pause.

The fix is a fencing token checked **at the resource, not at the holder**, because the zombie is by
definition the party that cannot be trusted to know it is stale. `gen` increments on every change of
hands (`sched/queue.go:373`, `:381`, `:512`, `:597`), `checkHolder` (`:408`) rejects an old gen, and
CASing against the exact bytes read (`:150`) makes the check atomic with the write — so a check that
passed against a stale read produces a losing CAS and a re-read, at which point it correctly fails.
Measured: 1314 zombie writes refused across 2000 jobs, exactly 2000 accepted completions.

### 4.3 A CAS-based scheduler over a dedicated queue service

**Rejected:** the scheduler as its own replicated state machine with its own Raft group (the
roadmap's original architecture, `ROADMAP.md:45-49`). **Chosen:** a pure client of the KV holding no
state of its own (`sched/api.go:1-6`).

Two properties fall out free. N schedulers over the same KV and prefix *are* one queue, because
`Queue` has no mutable fields. And a scheduler crash at any instant loses and duplicates nothing,
because there was never state in the process — the interesting crash cases move to *sequences of KV
writes interrupted halfway*, a much smaller and more enumerable surface (Submit's record-then-tail
ordering and its repair path, `sched/queue.go:177`).

The decisive argument is **time**. A replicated-state-machine scheduler cannot read a clock inside
`apply`, because `apply` must be a deterministic function of the log and "now" is not in the log. It
would have to make the leader *propose* time — logging "lease X expired" entries so every replica
expires the same lease at the same index — which is a leader-only clock authority and a second Raft
group to operate. Instead lease expiry is judged by whichever replica looks (`sched/api.go:106`), so
two replicas whose clocks differ by *d* disagree about expiry by *d* — purely a liveness effect (an
extra re-execution, or extra waiting), never a safety one, because safety comes from `gen` and `gen`
does not involve time.

The cost, plainly: a CAS-per-step design over consensus pays consensus latency per step. Submit is
four Raft rounds; Claim is O(distance from `head` to the first runnable job) linearizable reads. A
secondary index would make Claim O(1) and would introduce four new "crashed between two writes"
repair paths. The design chose one crash-repair story over five.

### 4.4 Log-then-apply, reused unchanged at every layer

The four-step pattern — serialize, append and wait for durability, apply, reply — is written once
(`kv/store/store.go:4-9`) and is the shape of everything above it. In Phase 2 only step 2 changes,
from "fsync locally" to "a majority has durably logged it"; `Machine.Apply` is byte-identical. In
Phase 3 the same log carries two more entry kinds so ownership changes are ordered against client
writes. In Phase 4 the *record itself* plays the role of the log: a job's `(state, gen)` carries
enough to recognise a replay with no dedup table at all.

What this buys is determinism. `Apply` may read nothing but the entry and its own state — no clock,
no randomness, no map iteration order. Violate it in Phase 1 and a replay disagrees with live state;
in Phase 2 replicas diverge; in `shardctrl.rebalance` (`shardctrl/machine.go:360`) two controller
replicas produce two different, internally consistent shard assignments for one config number — not
the wrong answer, but two different answers to the same question, agreed upon by nobody.

### 4.5 Rendezvous hashing, not consistent hashing and not hash-mod-n

**Rejected:** `hash(prefix) mod n`; a shared placement table; and (for this layer) a hash ring with
virtual nodes. **Chosen:** highest-random-weight hashing (`router/router.go:283-289`).

Placement needs two properties. Every gateway must compute the same answer for the same prefix and
live set *with no coordination* — anything else sends one prefix to two workers and halves the hit
rate. And a join or leave must move as few prefixes as possible, because a moved prefix is a cold
prefill. `hash mod n` has the first and catastrophically fails the second: from five workers to four,
a key keeps its owner only when `h%5 == h%4`, so ~75% of the keyspace reshuffles because one worker
died. Rendezvous has both — remove a worker and only the prefixes it was *winning* move, each to its
own runner-up, because no other score changed. Measured: 19.4% of 3000 prefixes moved when removing
one of five, all from the removed worker.

Rendezvous over a ring, at this scale: no ring state, no virtual-node tuning, naturally even balance
for a few dozen workers at O(n) per placement. A ring wins when n is large enough that O(n) scoring
hurts. Note Phase 3's *shard* assignment made the opposite choice — a fixed `NShards = 10` with an
explicit controller-computed assignment — because there the assignment must be *agreed*, stored and
migrated against, not merely computed identically.

**What this does not solve** (§8.2, and the project's most valuable result): rendezvous hashing
balances the *keyspace* and guarantees nothing about the traffic behind the keys. So affinity is a
*preference*: `Pick` (`router/router.go:307`) walks workers in score order and takes the first below
`maxInflight`, falling back to the top-scoring worker when all are saturated, because queueing at the
affine worker beats losing the cache.

### 4.6 One shared token bucket in the KV, not per-gateway quotas

**Rejected:** each gateway gets `Rate/N` and enforces locally. **Chosen:** one bucket per tenant in
the KV, read-refill-decrement-CAS (`ratelimit/ratelimit.go:150`, CAS at `:191`).

Dividing by N breaks in two silent ways. *When N changes*: autoscale 4 → 6 and every tenant's
effective limit shifts; roll a deploy and old and new replicas both hold a share for seconds, so the
fleet admits more than the limit. *Under uneven client affinity*: a tenant whose traffic lands on one
replica — sticky LB, one long-lived connection, one region — exhausts "its" 1/N while the other
shares idle, refused at a fraction of its paid rate while the operator sees a system nowhere near
capacity. One shared bucket has neither problem and introduces exactly one race, which the CAS
settles: the loser re-reads and finds nothing left.

Refill is computed lazily by the reader as a pure function of `(last stored state, now)`
(`ratelimit/ratelimit.go:138`) rather than by a ticker, because **no process owns the bucket to run
one in** — a ticker needs a leader, a lease for that leader, and a story for when it stops ticking.
And a refusal writes nothing (`:179-185`): a rate-limited tenant is by definition sending more than
you want to serve, and if every refusal cost a Raft round, the cheapest rejection in the system would
become the most expensive one under exactly the conditions that produce it.

Known ceiling: one key per tenant is one CAS hot spot per tenant. At 50 gateways with a hot tenant
the answer is a second tier — each gateway leases a batch of tokens and spends it locally — trading
exactness for round trips.

### 4.7 Pull-based shard migration

**Rejected:** the old owner pushes; and a separate protocol to establish "has the source let go".
**Chosen:** the losing group freezes the shard at a log index and the gaining group pulls it.

When `applyConfig` (`shardkv/server.go:264`) sees the group losing a shard it snapshots exactly that
shard's data *and dedup sessions* into `outgoing[shard][configNum]` (`:275-282`), keyed by the config
number that took it away — precisely the number the new owner will ask for, so no negotiation is
needed to agree which version is being transferred. The gaining group records it `needed` (`:295`)
and its poll loop pulls (`:457`).

Pull wins three ways. The gaining group knows when it is ready, so there is no "push arrived at a
replica that has not applied the config yet". Retry is trivial and idempotent: a failed pull is
retried next tick, and `applyMigration` (`:307`) accepts one only if the shard is *still* needed at
*exactly* the claimed config number, rejecting both a re-proposed duplicate and a stale delivery for
a superseded config. And the proof the source let go is structural, not protocol: `HandlePullShard`
(`:483`) answers only if it is leader *and* the frozen bytes are present at that config number, and
they exist only because `applyConfig` put them there at the same log index that ended ownership.

The poll loop is disciplined to one thing at a time (`:421`): while any shard is `needed`, do not
even ask for a newer config — otherwise a shard still in flight from config N could be reassigned
under N+1 before arriving, and that vintage would be delivered nowhere.

### 4.8 ReadIndex reads, not lease reads (and not log reads any more)

**Chosen:** ReadIndex (Raft thesis §6.4, `raft/readindex.go`). **Rejected:** lease reads. **Was:** every
`Get` proposed as a log entry.

A local read breaks linearizability twice over. A follower may be behind: write on the leader, read
on a follower, and miss your own write. And a leader may have been deposed without knowing: a
partitioned old leader keeps answering from a map the cluster has moved past. The original design put
every read in the log, which rules out both trivially, at the highest per-operation price in the
system: a consensus round plus an fsync on every node, per read. Once the gateway's hot path was
measured, that price was the bottleneck: every request read a rate-limit bucket and a cache entry.

ReadIndex keeps linearizability without the log entry. The leader records `commitIndex`, confirms it
is still leader by hearing from a majority in a heartbeat round that began after the read arrived,
waits until its state machine has applied that index, then reads locally. One round trip, no entry,
no fsync. Two details carry the correctness argument:

- **A new leader must have committed in its own term.** Until then it cannot know its commit index
  is final. Real implementations commit a no-op at the start of each term; this one returns
  `ErrReadIndexNotReady` and the server falls back to a log read for those first moments.
- **The confirmation round must start after the read.** Acks from earlier rounds prove nothing about
  now. Each AppendEntries carries the round current when it was built, and a reply in the same term
  records it.

`TestReadIndexDeposedLeaderRefuses` isolates a leader and requires it to refuse. Removing the
confirmation makes it serve the stale read, and the test fails.

*Lease reads* (assume leadership holds for `electionTimeoutMin` after a heartbeat round: zero round
trips) stay rejected. They trade one round trip for a real assumption about bounded clock drift, and
this system has already been bitten once by trusting clocks (the rate limiter, §3).

### 4.9 Seeded chaos, not true virtual-time deterministic simulation

**Rejected as out of scope, not as a bad idea:** FoundationDB/TigerBeetle-style simulation.
**Chosen:** a seeded fault-injection network plus a quorum-bounded nemesis against the real,
concurrent system.

The roadmap asked for "FoundationDB/TigerBeetle style", and this does not deliver it. Determinism
decreases in four tiers, and naming the tier every time is the point (`chaos/api.go:9-48`):

1. **Workload: fully pinned.** Each client owns an RNG seeded from the scenario seed plus its id; its
   decisions are a pure function of the seed and its iteration count.
2. **Network faults: fully pinned.** Every drop, delay and reorder comes from the seeded RNG, proven
   in isolation from Raft.
3. **Nemesis decision *function*: pinned given fixed state.** `chooseEvent` (`chaos/nemesis.go:43`)
   is a pure function of an RNG draw and the cluster's current condition, proven by holding that
   condition still by hand.
4. **Live nemesis *schedule*: best-effort.** Two runs of one seed always agree on the first event and
   diverge later.

Tier 4 is a *consequence of a correctness decision*, not a defect. The quorum bound
(`chaos/nemesis.go:61-62`) must consult the cluster's real condition — a simulated count could drift
and let a genuine below-quorum situation through — and real condition depends on real timing. Pinning
it would make the safety check stop reflecting reality. Bit-for-bit replay needs two structural
changes this codebase does not make: virtualizing every clock read (Raft's timers, the nemesis's
sleeps, the simnet's delay injection) and single-threading the system so "what happens next" is chosen
by an event loop you control. That is a day-one design commitment, which is why FoundationDB and
TigerBeetle build their core logic to run under either real I/O or a simulator from the start.

---

## 5. What a request actually does

### 5.1 One `Generate` call, end to end

1. **Client opens the stream.** `py/gateway_client.py` or `cmd/llmbench` calls
   `gateway.v1.Gateway/Generate` with `(tenant, prompt, max_tokens)`. `gateway/server.go:237` is the
   whole request path; `stream.Context()` is the root of every cancellation that follows.

2. **Rate limit** (`:264-283`). `ratelimit.Take(ctx, tenant, 1)` (`ratelimit/ratelimit.go:150`) reads
   `ratelimit/<tenant>` — a linearizable read, so on Raft a consensus round — refills lazily to now
   (`:138`), and CASes the decremented bucket back (`:191`). A lost CAS means another gateway spent a
   token; re-read, bounded by 32 attempts before `ErrContended`. Refused: `ResourceExhausted` with
   `retry_after_ms` computed exactly from the deficit and rate (`:184`), writing nothing. First,
   because it is the cheapest rejection and must not be dodgeable downstream.

3. **Cache** (`:285-301`). `semcache.Lookup` (`semcache/semcache.go:253`) normalises, hashes to
   `semcache/exact/<sha256>` (`:170-172`), one KV read. A hit streams via `streamCached` (`:340`) with
   `cached=true` and **never reaches a worker**. A miss tries the local near path — embed, brute-force
   scan of at most `MaxLocal` unit vectors (`semcache/index.go:101`), then confirm the winner by a KV
   read, since the entry may have expired since indexing, and check that the stored prompt is within a
   small edit distance of this one (`nearDuplicate`), since nearby in embedding space is not the same
   request. A cache *error* falls through to the model
   (`gateway/server.go:296-300`): caches are an optimisation, not a dependency. Second, because a hit
   costs one KV read and no GPU.

4. **Route** (`:303-331`). `Registry.Live` (`router/router.go:196`) reads `workers/index` through a
   200 ms local cache, filtering lapsed leases. `withLocalLoad` (`gateway/server.go:187`) folds this
   gateway's own outstanding streams into each worker's `Inflight`, because the registry's copy is
   only as fresh as the last lease renewal — about once a second — and cannot steer a decision made
   every few milliseconds (§8.3). Then `router.Prefix` (`router/router.go:237`) takes the first 256
   characters and `Pick` (`:307`) walks workers in rendezvous-score order, returning the first below
   `MaxInflightPerWorker`; with prefix routing off, `PickLeastLoaded` (`:330`) is the deliberate
   baseline. Third, because it is the first stage that costs a worker.

5. **Stream and hedge** (`:334`, `streamFromWorkers` at `:373`). `runCtx, cancelAll :=
   context.WithCancel(ctx)` with `defer cancelAll()` — returning for *any* reason tears down every
   worker stream started. The primary attempt (`runAttempt`, `:568`) calls the worker's
   `infer.v1.Inference/Generate` with its own derived context. If `HedgeAfter` is set and more than
   one worker is live, a timer arms (`:421-425`); on firing a second attempt launches on
   `Pick(live, prefix, 0, primary.ID)` — the *runner-up in the same ranking*, not a random worker
   (`:439-456`). The race is decided on **first token**, not completion: TTFT is dominated by prefill
   and worker queueing, which a different worker may not be suffering from, while decode rate is a
   property of the model and will not improve by moving.

6. **The loser is cancelled** (`:489-495`). Not bookkeeping: without it every race leaves a worker
   generating a full response nobody will read, for the full decode duration. A system hedging 20% of
   requests without cancelling losers has bought its tail latency with ~20% of its fleet. Cancellation
   reaches the worker over the wire, and the worker checks before every token
   (`infer/mock/mock.go:273-277`; the Python worker checks `context.is_active` before every token and
   every 20 ms *inside* prefill).

7. **Worker side.** The prompt is split into 64-char blocks, cumulative block hashes computed
   (`infer/mock/mock.go:169`), the longest resident leading run counted (`:181`), and prefill charged
   only on `len(prompt) - cachedChars` (`:252`). A trailing *partial* block is deliberately not
   cached — real paged caches work in whole blocks, and counting a partial one would let a
   one-character difference at the end of a prompt claim a full block of reuse. The real backend
   does the same thing for real, in 32-*token* blocks: `py/hf_backend.py` looks up the longest
   cached block-aligned token prefix, reuses those `past_key_values`, and prefills only the
   remaining suffix, so `prefix_cache_hit` there means computation genuinely skipped.

8. **Store and finish.** After the last token the response is stored (`semcache/semcache.go:313`)
   with a plain `Put`, not a CAS — if two replicas answer the same prompt at once either answer is
   valid, so last-writer-wins is correct and a CAS loop would add round trips to protect nothing.
   Spans: `gateway.Generate` → {`gateway.ratelimit`, `gateway.cache_lookup`, `gateway.route`,
   `gateway.attempt`} → `infer.Generate`/`worker.Generate`, nested across the gRPC hop by W3C
   trace-context propagation with no manual linking (`obs/tracing.go:13-36`).

### 5.2 One `Submit` → `Complete`, end to end

1. **Submit** (`sched/queue.go:177`). Read `tail` and `head`. If `tail - head + 1 >= MaxQueue`, refuse
   with `ErrQueueFull` → `ResourceExhausted` and a retry hint: shedding while the client can still do
   something about it beats accepting work that will time out in the queue. Otherwise write the record
   at `tail+1` as a CAS expect-absent, **then** bump `tail`. That order is deliberate — a crash between
   them leaves a record `tail` does not cover, which the next Submit detects (its expect-absent CAS
   fails) and repairs; the reverse order would leave a permanent hole indistinguishable from "not
   written yet". With an idempotency key, a leased placeholder is CASed first (`:265`) so a concurrent
   Submit under the same key waits rather than allocating a second job.

2. **Claim** (`:319`). A worker polls. The queue reads `head` and `tail` once, then walks ids: a
   terminal record advances `head` past it in passing (best-effort, CASing the raw bytes read); a
   PENDING record, or a RUNNING one whose lease expired, is runnable. Claiming sets `State=RUNNING`,
   `Worker`, `Attempts++`, `LeaseUntilMs = now + lease`, and **`Gen++`** (`:381`) — the fence. The CAS
   expected value is always the raw bytes `readJob` returned, never a re-marshal, because protobuf
   marshaling is not guaranteed byte-stable across producers and the KV compares bytes.

3. **Run** (`sched/worker/worker.go:249`). The handler runs under a child context while a heartbeat
   goroutine (`:256-302`) renews the lease every `Lease/3` with the `(id, gen)` from Claim. A
   heartbeat returning `FailedPrecondition` means the job is no longer ours, and the handler's context
   is cancelled *immediately* (`:288-293`) with its return value discarded.

4. **Complete** (`sched/queue.go:460`). `checkHolder` (`:408`) runs before the write: RUNNING under our
   gen → accept and write DONE plus the result; RUNNING under a different gen → `ErrFenced`; PENDING
   under any gen → `ErrFenced` (the only way a job we think we hold is PENDING is that Fail or Reap
   requeued it, both of which bump gen); DONE under *our own* gen → success with **no write**, the
   idempotent same-gen retry; DONE under a different gen, or FAILED → refused. The CAS makes the check
   atomic with the write.

5. **The zombie branch** (`sched/worker/worker.go:329-333`) — the single most important branch in the
   worker. A fenced `Complete` means we are a zombie: log at Warn, count it, **and move on**. Do not
   retry (the CAS would refuse again). Do not re-Claim (that would hand the same job to the same
   process under a new gen while the old attempt is still running). Do not crash. This one branch is
   what keeps a stale worker from ever overwriting a live one's result.

6. **Meanwhile, the reaper** (`sched/leader.go:89`). Whichever replica holds the `<prefix>/leader`
   lease scans for expired leases and returns those jobs to PENDING with `Gen++`. Leadership is purely
   efficiency — ten replicas each scanning would multiply KV read load by ten for no correctness gain
   (`sched/leader.go:6-15`) — and if two replicas briefly both believe they lead, the worst case is a
   brief period of double scanning, because every step is a CAS.

---

## 6. Failure modes handled, and how

| Failure | What happens | What survives |
|---|---|---|
| **Leader loss** (crash or partition) | Followers time out on randomized deadlines and one wins. The election restriction (`raft/election.go:92-93`) means the winner already holds every committed entry, so data flows only leader → follower. In-flight requests get `ErrLeaderChanged` or a timeout and retry elsewhere with the same `RequestMeta`. Measured: next write succeeded after a ~2 s election. | Linearizability and every acknowledged write. The retry is recognised as a duplicate by the session table on the new leader (or, if also logged, by `Apply`'s dedup check before it mutates). |
| **Minority partition** | The majority side continues. The minority cannot commit: a partitioned old leader accepts entries it can never commit, and because `Get` also goes through the log (§4.8) it cannot even answer a stale read. On rejoin its uncommitted tail is overwritten. | Full linearizability on the majority side. The minority is unavailable — the CP choice, made explicitly. |
| **Quorum loss** | The cluster stops: no stale data, no two leaders, no writes. This is Raft working, and it is exactly the confusion that produced §8.5, where the nemesis partitioned 3 of 5 nodes and the harness reported the resulting unavailability as a violation. | Safety. Liveness is deliberately sacrificed; no consensus system can do otherwise. |
| **Worker crash (scheduler)** | Its lease lapses; either the reaper notices or the next claimer reclaims the job in passing (`sched/queue.go:319`) without waiting. `Gen++`, back to PENDING, another worker runs it. After `MaxAttempts` it becomes FAILED rather than looping. | Exactly-once *commit*. The job may execute more than once — the documented, unavoidable contract — so handler side effects must be idempotent on `job.Id`. Verified with a real `kill` mid-run: 120 jobs, all DONE, zero FAILED. |
| **Zombie worker** (paused past its lease, then wakes) | Its heartbeat is fenced, cancelling its handler immediately. If it reaches `Complete`, `checkHolder` sees a stale `gen` and refuses; the CAS guarantees the check cannot be raced. It logs, counts, and moves on. | Exactly-once commit. Measured: 1314 fenced writes across 2000 jobs, exactly 2000 accepted completions. Also verified against a real heartbeat-suppressed worker process. |
| **Worker crash (inference)** | It stops renewing its registry lease and drops out of routing — no failure detector, no gossip, because the absence of heartbeats *is* the signal and is the only one that survives `kill -9`. A request racing the lapse onto a dead worker is survivable: the gateway falls back. | The request. A stale registration is harmless because it grants no exclusive permission — nothing for a zombie worker to corrupt, so no fence is needed. |
| **Shard migration mid-write** | A write that had not reached the old owner's log gets `ErrWrongGroup` (re-resolve and retry elsewhere) or `ErrNotReady` (right group, data in flight — retry *here* shortly). A write that committed just before the config change is captured in the frozen snapshot and travels to the new owner intact, sessions included, so its retry is recognised as already-applied. | Linearizability across the key's whole lifetime, verified by Porcupine over 3191 operations across 3 groups during continuous rebalancing: zero violations. |
| **Cache failure** | `Lookup` errors fall through to the model (`gateway/server.go:296-300`); a `Store` failure is likewise non-fatal. | The request. An explicit rule with its own test: a system that fails requests when its optimisation is down has turned an optimisation into a dependency. |
| **Rate-limit contention** | Each gateway loses CASes and re-reads, bounded by 32 attempts, then returns `ErrContended` — deliberately *not* the same signal as "the limit was exceeded". | The hard burst bound, always: two gateways can never both spend the same token, because the bound is enforced by the CAS'd value rather than by time. Skew fuzzes the effective *rate*, never the burst. |
| **A write torn by a crash mid-fsync** | Recovery truncates to the last good record (`kv/wal/wal.go:96`). A bad checksum *at the tail* is a crash artifact and is repaired; one with valid records after it is real data loss and is raised, because a torn write can only be the last thing written, so anything with data after it was acknowledged. | Every acknowledged write. Verified over 200 kill-mid-write iterations: zero lost, log always a contiguous prefix. |

---

## 7. What is deliberately not implemented

Scope limits, stated as limits rather than as future work quietly excused.

- **No batched model serving.** A real model backend exists and works (`--backend hf`, §1), but it
  serves one request at a time per worker on a small CPU model, and every Phase 5/6 benchmark table
  except the "Real model" section of `BENCHMARKS.md` came from the mock. Which backend produced a
  number is the single most important caveat in this document (§1).
- **No continuous batching, no paged attention.** The biggest real-world throughput lever in LLM
  serving, absent. It is orthogonal to routing — batching decides which of the requests *already at a
  worker* run next — but they interact: a worker's effective capacity becomes a function of how
  batchable its queue is, which makes `MaxInflightPerWorker` as a scalar the wrong shape for a real
  scheduler.
- **No cross-shard transactions.** An operation touches exactly one key and therefore one shard, by
  construction. Atomicity across shards needs a real distributed transaction protocol.
- **No GC anywhere.** Frozen `outgoing` shard snapshots are kept forever with no ack protocol to free
  them — a real, known, unbounded leak. Same for completed job records and idempotency bindings, and
  for cache entries (overwritten, never deleted).
- **No token-level rate limiting.** The bucket counts *requests*; real LLM APIs limit on tokens.
  Doing it properly needs reserve-at-admission / settle-at-end-of-stream, a leased reservation record,
  a reaper for gateways that die mid-request, and a decision about actual exceeding estimate.
- **No lease reads, and no start-of-term no-op.** Reads use ReadIndex (§4.8); a leader that has not
  yet committed in its term serves reads through the log instead.
- **No Raft membership changes.** Cluster membership is fixed at start-up: no joint consensus, no
  single-server add/remove, no learners. Replacing a dead node means restarting the cluster with a new
  peer list. Sharding moves *data* between fixed groups; it never changes a group's members.
- **No inference engine.** No continuous batching, no paged KV memory, no GPU scheduling. The real
  backend runs one request at a time per worker, so this is a serving *gateway* in front of workers,
  not an inference server.
- **No virtual-time deterministic simulation.** Four honestly enumerated tiers (§4.9); the live
  nemesis schedule is best-effort and only the first event is guaranteed reproducible.
- **No clock-skew or confirmed disk-pressure chaos.** Both attempted, both reported as not achieved
  rather than faked: containers on one host share a kernel clock, and the disk-pressure experiment
  produced real leader-flapping but no confirmed `ENOSPC`.
- **No Byzantine or corruption faults.** Every fault injected anywhere is crash-stop or omission.
  Nothing tests behaviour against a participant that lies.
- **No auth, no TLS, no authz.** Every connection is insecure; `tenant` is client-supplied, so any
  client can claim any tenant's quota. The limiter is resource protection, not a security boundary.
- **No multi-machine durability.** Three "replicas" fsync to one laptop disk: the replication is
  real, the independence of the failure domains is not.
- **No priorities, fairness, work stealing or job cancellation** in the scheduler; strictly FIFO by
  id with a single global `MaxQueue`.
- **No hedge budget.** `HedgeAfter` is fixed with no cap on the fraction of requests that may hedge,
  so under fleet-wide slowness every request would hedge and double the load on an already-slow fleet.
- **No multi-turn conversation affinity, and no cache key versioning by model.** Routing hashes the
  first 256 prompt characters; the cache key is `sha256(normalised prompt)` with no model name,
  temperature or system-prompt version, so swapping the model serves the old model's answers until TTL.

---

## 8. What testing found: the bug record

Five bugs testing found and reasoning alone did not — the part of the project hardest to fake.

### 8.1 A rendezvous hash that gave one worker in five half the keyspace

`TestRendezvousMinimalDisruption` failed with `moved fraction 0.500, want about 1/5`. The confusing
part: the *other* assertion in the same test passed exactly — every prefix that moved had been on the
removed worker, and none unrelated was reshuffled. The minimal-disruption property, the entire reason
rendezvous hashing exists, was working perfectly. Removing one worker of five moving half the keyspace
can only mean that worker owned half of it to begin with.

The cause was the obvious implementation, `fnv64a(prefix + separator + id)`. FNV-1a's loop is
`h = (h ^ byte) * prime`, so it *ends* with the last byte of the id. Two ids differing only in their
final character — `w0`..`w4`, `pod-1`..`pod-9`, which is how every real worker id looks — produce
final hashes related by a fixed multiplicative step from a common intermediate state, so which id
scores highest depends far more on the *id* than on the *prefix*. The deeper point: rendezvous
hashing's correctness argument is a statement about **n independent random draws**, and a hash weak
with respect to structured input quietly violates that assumption while nothing in the algorithm
notices.

The fix (`router/router.go:283-284`) is three changes, each load-bearing: hash each side *separately*
so the id cannot ride on the prefix's accumulator state; multiply the id's hash by a large odd
constant (the golden ratio, invertible mod 2⁶⁴) to scramble structured ids apart; and run the
combination through `mix64` (`:259`), the splitmix64 finalizer, so one input bit flips about half the
output bits. Distribution afterwards: **528 / 472 / 500 / 500**.

**The lesson.** A loose test bound would have passed this. The bounds are ±20% on purpose; "no worker
gets more than 80%" — which sounds reasonable while you are writing it — would have been satisfied by
a function handing one worker 50%. When testing a *statistical* property, the bound is not a formality
you loosen until the test stops flaking. The bound **is** the test.

### 8.2 Affinity balances prefixes, not load — the A→B regression

> **Re-measured (2026-10-05).** The single-run numbers below came from a confounded setup:
> `-max-inflight 16` against workers that run 4 at once, warm workers reused across phases, one
> trial. With those removed (`scripts/bench_routing.sh`, 5 trials, closed and open loop) the
> throughput cost of affinity is about 9%, not about 35%. Bounded-load routing has the best hit
> rate (0.51 vs 0.29), and hedging remains the clear p99 win. See `BENCHMARKS.md`. The mechanism
> described below is real; its magnitude was overstated.

Not a code bug: a bug in the expectation, found because the benchmark measured outcome and not only
mechanism. Prefix routing tripled the cache hit rate (0.20 → 0.60), cut mean prefill, and collapsed
prefix spread to 1.08 — and made throughput *worse* (20.4 → 13.4 req/s) and p99 TTFT *worse* (967 →
1515 ms). The per-worker column explains it: 12 prefixes onto 4 workers gave two of them 105 and 99
requests against 15 and 21 — ~85% of traffic on half the fleet — where least-loaded had been
61/59/61/59.

Rendezvous hashing guarantees each worker wins ~1/n of the *keyspace* and nothing about the traffic
behind those keys: twelve prefixes over four workers is twelve draws from a four-way multinomial where
an even split is lucky, not typical. Locality and load balance are in direct tension, because a
perfectly sticky router cannot move work off a hot worker. Hedging then recovered the tail (p99 958
ms, below baseline) *and* improved mean prefill to 96 ms, because the hedge target is the runner-up in
the same ranking, so the fleet converged on a replication factor of 2 for hot prefixes. Full numbers:
`BENCHMARKS.md` and `docs/phase5.md` §10.

> **Correction (2026-10-01).** The hedging result above hedged at 250 ms, under the median TTFT, so it hedged ~62% of requests: its p99 gain was mostly load-spreading. Re-measured with tail-only hedging (1 s delay, 23% of requests hedged): p99 4,232 → 2,076 ms, with the median getting *worse*. See [BENCHMARKS.md](BENCHMARKS.md#correction-2026-10-01-the-original-hedging-result-was-mostly-load-spreading).

**The lesson.** Measure mechanism and outcome separately and be ready for them to disagree. Reporting
only the hit rate would have shipped this as a success and a throughput regression.

### 8.3 Load-aware routing reading a signal it could not use

`TestPrefixRoutingGivesAffinityAndCacheHits` fires 16 concurrent requests sharing one system prompt
and compares prefix routing against least-loaded. Least-loaded sent **all 16 to one worker** — and a
load balancer that cannot balance is not much of a baseline.

Routing read load from exactly one place: the registry's `Inflight`, only as fresh as the worker's
last lease renewal, about once a second. Sixteen requests arriving within a few milliseconds all read
the same stale value, usually zeros, and the deterministic tie-break by id sent every one to the same
worker. The tie-break was not the bug; the tie was. The fix (`gateway/server.go:56-66`, `:187`) has
the gateway count its *own* outstanding streams per worker and fold that in before every decision:
**count locally what you can observe exactly, and use the remote signal only for what you cannot** —
here, load from *other* gateways and the worker's internal queueing. The local count is partial by
construction (N gateways each see 1/N), which is exactly how Nginx's least-connections, Envoy's
`LEAST_REQUEST` and gRPC's `least_request` work.

**The lesson, and its second order.** A signal refreshed every second cannot steer a decision made
every few milliseconds — between refreshes it is a constant, and a constant plus a deterministic
tie-break is a stampede. And note what else the fix did: before it, *both* arms piled onto one worker,
so the two strategies produced identical placement and the comparison measured nothing. **A broken
control arm will cheerfully report that your change did nothing, or everything.**

### 8.4 A benchmark that could not fail

The first `scripts/e2e_llm.sh` ran 4 system prompts across 4 workers with the default prefix-cache
size. Within seconds every worker had cached every prompt, the *baseline* already scored a 0.9 hit
rate, prefix routing had nowhere to go, and the script printed two nearly identical numbers and
declared success.

The fix is a design statement, not a tweak: run the workers **cache-capacity-bound** at 32 blocks
(≈3.5 of the benchmark's ~600-char system prompts) with **12** prefixes over **4** workers, so each
worker holds only a few and locality has to do real work. That is also the realistic regime — an
attention KV cache is GPU-memory-bound and evicts constantly, so locality only pays when each worker
sees a *subset* of the prefixes. Same discipline one level down: the comparison must run
*concurrently*, because sequentially every worker is idle on arrival and least-loaded degenerates into
a deterministic tie-break that looks exactly like affinity.

**The lesson.** A benchmark that cannot distinguish the thing it measures is worse than no benchmark,
because it produces a number you will quote. Before trusting any A/B, construct the regime where the
mechanism under test is the binding constraint and verify the control arm can genuinely *fail* there.

### 8.5 A nemesis asking an impossible question — and the honest fix that broke a test

`TestManySeeds` failed on two seeds with `context deadline exceeded`. The first hypothesis — not
enough settle time — was wrong; a fixed sleep did not help. The second was real but incomplete: the
Porcupine check and the scheduler check shared one deadline, and Porcupine's search ate most of it
before the scheduler check got a turn. That fix is still in the code — each phase gets a fresh
deadline created immediately before it runs (`chaos/harness.go:226-237`). The same seeds still failed,
now deterministically at 49 seconds.

Reproducing standalone with `cmd/simrun` and reading the printed event log showed it immediately: the
nemesis had partitioned **three of five nodes** with no heal ever drawn. Two nodes is not a majority
of five. Raft was behaving exactly as specified — a cluster with 2 of 5 votes sits unavailable forever
by design — and the "violation" was the harness asking an impossible question. The fix bounds
`chooseEvent` so Partition, Pause and Crash are eligible only while enough nodes remain to leave a
majority (`chaos/nemesis.go:61-62`), with crashes and partitions sharing one budget, plus a
heal-everything step after the nemesis stops. Seed 2 went from 49.48 s and a false failure to
**5.1 s and PASS**.

**Then the fix broke a different test, correctly.** `TestSeedReproducesNemesisSchedule` asserted that
two live runs of one seed produce the same full event sequence, and it began failing. The cause: the
majority bound makes `chooseEvent` consult *live* state, whose timing is not seeded — and *should not
be*, because pinning it would mean the safety check stops reflecting the cluster's real condition,
which is the whole reason the bound is correct. The right move was to fix the test's claim, not water
down the fix: that test was replaced with two narrower ones true at the layer they claim (`chooseEvent`
is a pure function of fixed state and an RNG; the *first* live event is always reproducible), and the
package documentation was rewritten into the four precise tiers of §4.9.

**The lesson, twice.** A "violation" your own test reports might be your test asking an impossible
question — and the discipline that catches it is not iterating on timeouts but reproducing standalone
and reading what actually happened. Then: when a fix that makes the system *more* correct breaks a
test asserting something about its old behaviour, fix the test's claim. Story 1 is a test that was too
permissive; story 2 is a test that was too strict. Both are the harness being wrong about the system,
in opposite directions.

---

### 8.6 Consensus capped at one write per fsync

Measuring the gateway's request ceiling gave 27 req/s, far below what the KV's own benchmark
suggested. Following it down, the replicated KV plateaued at ~200 ops/s from 8 to 64 concurrent
clients: latency grew with the queue and throughput did not move. The Raft persister rewrote and
fsynced the whole log on every `Start` and every follower append while holding `rf.mu`, so the
cluster could do one write per fsync no matter how much work was waiting. Phase 1's store had group
commit from the start; the Raft path never got it.

The fix is group commit in Raft (`raft/raft.go` `persistLoop`). The safety argument is the part
that needed care:

- **Leader.** It may send entries before they are durable locally (Raft thesis §10.2.1), but its
  own `matchIndex` advances only after the save covering them lands, and only if it is still
  leader of the same term (a leader's log is append-only within its term).
- **Follower.** It acknowledges only after a save covering the acknowledged entries lands. That
  includes entries it merely *matched*, which a concurrent request may have appended and not yet
  saved. Because it releases the lock while waiting, it re-checks afterwards that the term is
  unchanged and the entries are still in its log.
- **Votes and terms** are still saved synchronously before anyone acts on them.

Result: 192 → 1,385 ops/s at 64 clients, with p99 596 → 71 ms (`BENCHMARKS.md`). The first full test
run then hung in Figure 8, and a targeted repeat showed an "apply out of order" failure. That was a
race in the test harness, not in Raft. The harness's apply checker for a crashed instance could be
mid-`select` when the crash closed its stop channel, and record the dead instance's message against
the restarted instance's reset counters. The fix checks for staleness under the harness lock, and
it logs every message it drops, so the explanation was confirmed rather than assumed: the logged
drops matched the original failure exactly.

**The lesson.** A benchmark of the layer above found a bottleneck two layers down. And when a
consensus change makes a test fail, find out which side is wrong before changing either.

## 9. How to run and verify it

Activate the project-local toolchain first — everything lives under `.tools/` and `.venv/`, nothing is
installed system-wide: `. .\env.ps1` (PowerShell) or `source ./env.sh` (Git Bash).

| Target | What it runs | What it proves |
|---|---|---|
| `make test` | `go test -race -count=1 ./...` | The whole repository, 18 test packages, race-detector clean. The one command that has to be green. |
| `make test-crash` | kill -9 mid-write, 200 iterations | The WAL's tail-repair rules are right at every byte offset a kill can land on, and no acknowledged write is lost. |
| `make test-raft` | 27 tests: election, replication, persistence, snapshots | Figure 2 under dropped, delayed and reordered RPCs, crashes and partitions — including Figure 8 for 1000 iterations. |
| `make test-lin` | 8 clients, 12 s, 5 nodes, nemesis, Porcupine | Linearizability of the replicated KV under partitions and crashes. One stale read anywhere makes the history unlinearizable and the checker finds it. |
| `make test-shard` | `shardctrl` + `shardkv` suites | The rebalancer is deterministic, balanced and minimal-movement; data *and* dedup sessions migrate; linearizability holds across live shard moves (3191 ops, 3 groups, zero violations). |
| `make test-sched` | chaos test at 1000+ events, plus the queue over a real Raft KV with a leader outage | Exactly-once commit under thousands of pauses and crashes, and that the CAS-plus-fence discipline composes with consensus rather than merely with a mutex. |
| `make e2e-sched` | real `raftkv` + `sched` + worker processes; one worker killed, one heartbeat-suppressed | The same guarantee against real processes and real gRPC — where Phase 3's two client bugs lived, and which no in-process test could have found. |
| `make test-llm` | gateway, rate limiter, router, semantic cache, mock worker | Exact burst enforcement across independent limiters; rendezvous balance and minimal disruption; affinity under *concurrency*; a hedge whose loser is verifiably cancelled; a client cancel that stops the worker *early*. |
| `make e2e-llm` | real `raftkv` + gateway + 4 Python workers; runs A/B/C and **checks**, not prints | That prefix routing raises the hit rate, that the hit rate is worth something (mean prefill falls), that placement is *stable* and not merely statistically better, that hedging fires, that cache and limiter work over real Raft, and that a real cancelled stream stops a real worker. |
| `make test-chaos` | 12 fixed seeds × KV+scheduler chaos, plus a gateway-mix run | Linearizability and exactly-once hold under *simultaneous* seeded adversarial network and process faults, under `-race`. |
| `make simrun` | one seeded run, verbose, with the event log | A seed reproduces a scenario, and on failure prints the exact command to re-run it — the tool that found §8.5. |
| `make chaos-docker` | real containers: partition, SIGKILL + restart, `tc netem` | The same class of guarantee across real process boundaries, container lifecycles and a real bridge network. 14/14 checks. |
| `make obs-up` | Prometheus + Grafana + Jaeger | That the instrumentation is real rather than decorative: a full nested trace from gateway to Python worker, and non-zero counters. |

Running the system by hand — a three-node cluster, a sharded cluster with a second group joined live,
the scheduler's zombie-worker experiment, the gateway with four Python workers — is covered step by
step in `README.md`.

---

## Where to read more

| Layer | Document |
|---|---|
| WAL, fsync, torn writes, idempotency, CAS, group commit | `docs/phase1.md` |
| Raft, the seven classic bugs, linearizability and Porcupine | `docs/phase2.md` |
| Sharding, the three-kind log, live migration, the rebalancer | `docs/phase3.md` |
| Leases, fencing tokens, exactly-once commit, admission control | `docs/phase4.md` |
| Rate limiting, semantic caching, prefix routing, hedging, cancellation | `docs/phase5.md` |
| Seeded chaos, the determinism tiers, container chaos, observability | `docs/phase6.md` |
| The narrative: what building this actually taught | `docs/phase7.md` |
| Every measured number, with conditions | `BENCHMARKS.md` |
| The original plan and its exit criteria | `ROADMAP.md` |
