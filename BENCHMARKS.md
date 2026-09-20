# Benchmarks

Every measured number from Phases 1–6, with the conditions that produced it.

## Read this before any table

**The inference backend is a mock.** Every Phase 5 and Phase 6 serving number came from
`py/infer_worker.py --backend mock` (the default) and its Go twin `infer/mock`. That is not a
language model: it is a written-down latency model — prefill linear in the number of *uncached*
prompt characters, a prefix cache of 64-character blocks, a fixed per-token decode delay, optional
injected tail stalls. A `--backend hf` path exists and is explicitly untested; torch and
transformers are not installed. No GPU, no weights, no real tokens. These numbers support one narrow
claim: *given a worker whose prefill cost falls when it has already seen your prefix, here is what
routing, hedging, caching and cancellation do to hit rate, throughput and the tail.*

**One laptop.** All of it ran on a single memory-constrained Windows 11 laptop (8 GB RAM). "Three
replicas" means three processes on loopback; "five nodes" means five Raft instances in one process
over a simulated network. Every fsync in a "3-node cluster" hits the same SSD.

**So: ratios, not absolutes.** 339 puts/s says nothing about Raft on real hardware. The *33x gap*
between it and the same store unreplicated, on the same disk the same afternoon, says something that
travels. Read every table for the before/after and the A/B. Counts and pure ratios (moved-key
fraction, hit rate, fenced writes, violations found) are worth more than the timings around them.

**Not production-grade.** Built with production practices — race-detector-clean tests, chaos
engineering, linearizability checking, tracing, metrics — but never run at scale or for more than
minutes, with no auth, no TLS, no security review. See `DESIGN.md` §1.

---

## Phase 1 — single-node durable KV

`cmd/kvctl bench` against `cmd/kvserver`, real fsync per commit.

| Configuration | Clients | Throughput | p50 |
|---|---|---|---|
| fsync per write, no batching | 8 | 1082 puts/s | 7 ms |
| group commit (N writers share one fsync) | 64 | **11312 puts/s** | ~5 ms |

**What this shows.** Durability is a latency floor, not a throughput floor. Group commit made no
single write faster — p50 *stayed* ~5 ms — it let 64 writes share one disk round trip. The knob is
how many writes share an fsync, never whether the ack waits for one.

| Durability test | Result |
|---|---|
| `make test-crash`: kill -9 mid-write, 200 iterations | **zero acknowledged writes lost**; log always a contiguous prefix; log never shrank |

**What this shows, and does not.** The tail-repair rules are right at every byte offset a kill can
land on. It does *not* test fsync: `kill -9` leaves the page cache intact, so this passes even with
fsync disabled. Only power loss would test that, and it was not run (`docs/phase1.md` §5).

---

## Phase 2 — the cost of consensus

Three `cmd/raftkv` processes on loopback, each with its own `FilePersister`, `kvctl bench -c 8`.

| Setup | Throughput | p50 | p99 |
|---|---|---|---|
| Phase 1, single node, group commit, 64 clients | 11312 puts/s | ~5 ms | — |
| Phase 2, Raft across 3 processes, 8 clients | **339 puts/s** | 22 ms | 56 ms |

**What this shows — the honest headline of Phase 2.** Replication cost ~33x, and that is the correct
outcome, not a defect. A write is now: a gRPC hop, a persist-and-fsync on the leader, a fan-out to
two followers each persisting and fsyncing before acking, and the client waiting on the faster of
the two. Reads pay it too (`DESIGN.md` §4.8). Two implementation costs inflate it beyond the
protocol minimum: the persister rewrites the whole log and snapshot per append, and there is no
group commit on the Raft path. What the 33x buys: an acknowledged write survives any one node's
disk, and every future leader is guaranteed to hold it.

| Failure test | Result |
|---|---|
| `kill -9` the leader mid-bench | next write succeeded 66 ms after a ~2 s election |
| Restart the killed node | rejoined and later served as leader from persisted state |
| `make test-raft`, 27 tests (election / replication / persistence / snapshots) | all pass under `-race` |
| `make test-lin`: 8 clients, 12 s, 5 nodes, nemesis partitions + crashes, Porcupine | **zero violations** |

**What this shows.** ~2 s failover is the election timeout, a tuning choice. The Porcupine result is
load-bearing: one stale read anywhere makes the whole history unlinearizable and the checker finds it.

---

## Phase 3 — sharding and rebalancing

| Rendezvous hashing property | Measurement |
|---|---|
| Balance: 2000 prefixes over 4 workers | **528 / 472 / 500 / 500** |
| Minimal disruption: remove 1 of 5 workers, 3000 prefixes | **19.4% moved, 100% of them from the removed worker** |
| Same removal under `hash(key) mod n` | ~75% of the keyspace reshuffles |

**What this shows.** Two different claims. *All moved keys came from the removed worker* is
correctness; *19.4% ≈ 1/5* is balance. The first implementation passed the correctness assertion
perfectly and still gave one worker in five 50% of the keyspace (`DESIGN.md` §8.1) — a loose bound
would have shipped it.

| Linearizability under migration | Result |
|---|---|
| 3 groups, 9 replicas, 6 clients, 12 s, shards reassigned ~every 0.5 s on an unreliable network | **3191 operations, zero violations** |

**What this shows.** Keys stay linearizable *while their shard is being handed between Raft groups* —
no window with two owners, none with a silently unreachable key — because the migration is an entry
in the same totally ordered log as the client writes.

Real multi-process cluster: 3 `shardctrl` + two three-replica `shardkv` groups, all separate OS
processes, driven by `kvctl -ctrl ... bench`.

| Stage | Puts | Retries |
|---|---|---|
| Before either client fix | 500 | 508 |
| After reusing one leader-cache per group id | 500 | 16 |
| After also giving each bench worker its own client identity | 1000 | 32, then 32 again |

**What this shows.** 508 retries for 500 puts is ~1:1 — too clean to be noise, which is what prompted
the investigation (`docs/phase3.md` §7). Also verified by hand, not by any automated check: a second
group joined a **live** cluster, the controller rebalanced ~half the shards onto it, and all 20
previously written keys kept resolving — no restart, no client repointing, zero intervention.

---

## Phase 4 — scheduler: exactly-once commit under chaos

In-memory linearizable KV, injected clock (1 ms real ≈ 5–25 ms fake), 8 workers, under `-race`.

| Metric | Count |
|---|---|
| Jobs | 2000 |
| Workers paused past their lease, then committing anyway | 1334 |
| Worker crashes (claim and never return) | 1124 |
| Zombie writes refused by the fence | 1314 |
| **Accepted completions** | **exactly 2000** |
| Jobs committed twice | **0** |
| Wall time | 5.1 s |

**What this shows.** 2458 chaos events against the roadmap's 1000. Checked three ways — every job
DONE, no job with more than one accepted `Complete`, and the *total* accepted equal to the job count
(which closes the gap the first two leave). The test also asserts `fenced > 0`: a run where no zombie
appeared did not exercise the mechanism, and passing on it would be vacuous.

| Setup | Result |
|---|---|
| Same queue over 3 real `raftkv` replicas, leader disconnected 400 ms in for 1.5 s | 30 jobs, exactly-once, 2.6 s |
| `make e2e-sched`: 3 `raftkv` + 1 `sched` + 3 workers as OS processes; 120 jobs; one worker `kill`ed mid-run; one run `-suppress-heartbeat -slow-after 3` | **all 120 DONE, 0 FAILED**; suppressed worker fenced exactly once; 13 of its results accepted while its lease was live |
| Submit rate in that e2e | 120 submits in 17 s |

**What this shows.** The CAS-plus-fencing discipline composes with Raft, not merely with a mutex. The
120-in-17s figure is not throughput — the script forks a fresh `schedctl.exe` per submit — it is a
reminder that a CAS-per-step design over consensus pays consensus latency per step. The "fenced once,
13 accepted" split is the design working: being slow is not a crime, only being slow *and stale*.

---

## Phase 5 — LLM serving layer

**Setup.** 3 `raftkv` + 1 `gateway` + 4 `py/infer_worker.py`, all separate OS processes over real
gRPC. 240 requests, 16 concurrent, 12 distinct ~600-char system prompts. Workers capped at **32
prefix-cache blocks** (≈3.5 of these prompts) so eviction pressure is real and locality binds. Mock
backend.

| Run | Routing | Hedging | Hit rate | Mean prefill | TTFT p50 | TTFT p99 | req/s | Prefix spread | Per-worker |
|---|---|---|---|---|---|---|---|---|---|
| A | least-loaded | off | 0.20 | 167 ms | 247 ms | 967 ms | **20.4** | 3.92 | 61/59/61/59 |
| B | prefix | off | **0.60** | 140 ms | 651 ms | 1515 ms | 13.4 | **1.08** | 15/21/105/99 |
| C | prefix | on (250 ms) | **0.675** | **96 ms** | 357 ms | **958 ms** | 17.8 | 1.83 | 42/47/79/72 |

*Prefix spread = mean distinct workers a given prefix was sent to; 1.0 is perfect stickiness.*

**What this shows — the most interesting result in the project, and it is a regression.**

*A → B, the mechanism worked perfectly.* Affinity tripled the hit rate (0.20 → 0.60), cut mean
prefill 167 → 140 ms, and collapsed spread 3.92 → 1.08. Every design claim about rendezvous hashing
is confirmed.

*A → B, the outcome got worse.* Throughput fell 20.4 → 13.4 req/s and p99 TTFT rose 967 → 1515 ms.
Not noise: hashing **12 prefixes onto 4 workers** gave two of them 105 and 99 requests against 15
and 21 — **~85% of traffic on half the fleet** — where least-loaded had been 61/59/61/59.

> **Affinity balances prefixes, not load.**

Rendezvous hashing guarantees each worker wins ~1/n of the *keyspace* and nothing about the traffic
behind those keys. Twelve prefixes over four workers is twelve draws from a four-way multinomial with
unequal per-prefix request counts on top; an even split is lucky, not typical. Locality and load
balance are in direct tension — a perfectly sticky router is one that cannot move work off a hot
worker.

*B → C, hedging recovered the tail and more.* p99 1515 → 958 ms (below run A's 967), throughput 13.4
→ 17.8 req/s, p50 651 → 357 ms. It works because a request queued behind 100 others is slow for a
*request-independent* reason. The surprise: mean prefill also improved, 140 → 96 ms, and the hit rate
rose to 0.675 — because the hedge target is the **runner-up in the same rendezvous ranking**, so the
fleet converged on a replication factor of 2 for hot prefixes. The cost is spread rising 1.08 → 1.83.

**Reported prominently because had only the hit rate been reported, this would have shipped as a
success and been a throughput regression in production.**

| Also measured, same run | Result |
|---|---|
| Semantic cache on repeated identical prompts | **49/50 (0.98)** |
| Requests shed at `-rate 2 -burst 3` (30 logical requests, 6 clients, with retries) | **93** |
| Client cancels after reading 3 tokens | worker verifiably **stopped at 3 tokens** |
| `BenchmarkLookupNear`: embed + scan 5000 vectors (dim 512) + KV confirm | **~1.44 ms** |

**What these show.** The shed count exceeding the request count is by design — each refusal is
retried after backoff. The 1.44 ms scan is the argument for *not* building an ANN index: a generation
is 100x that, so the naive scan is noise on the path it protects.

---

## Phase 6 — chaos, simulation, observability

| In-process harness run | Result |
|---|---|
| `TestManySeeds`: 12 fixed seeds × KV+scheduler chaos (5 nodes, 6 clients, nemesis ~every 25–30 ops, bounded to a minority) under `-race` | **zero violations** |
| `TestGatewayMixSurvivesChaos`: seed 999, 4 s, gateway + 3 mock workers | zero panics |
| `go test -race ./chaos/...` total | ~77 s |
| Hand run `simrun -seed 42 -duration 5s -nemesis-interval 20` | **603 KV ops, 21 nemesis events** across all 5 nodes in **5 wall seconds**, PASS |

| Seed 2, the nemesis-bound fix | Wall time | Result |
|---|---|---|
| Before the majority bound | 49.48 s | FAIL — `scheduler: could not read stats: context deadline exceeded` |
| After | **5.1 s** | PASS |

**What this shows.** The "violation" was the harness partitioning 3 of 5 nodes with no heal drawn,
and Raft correctly refusing to proceed without quorum. A nemesis that pushes a cluster below its
stated failure threshold tests only whether the system declines to work, which it always will
(`DESIGN.md` §8.5).

| Docker chaos (`scripts/chaos_docker.sh`, real containers) | Result |
|---|---|
| Full script | **14/14 checks passed**, ~248 s engine time (~4.5 min wall) |
| Partition: dynamic leader detection, `docker network disconnect`, heal, rejoin | remaining majority kept serving read-your-writes |
| Crash: `docker compose kill kv2` (SIGKILL) + restart on the same volume | pre-crash data readable from the cluster and from `kv2` directly |
| `tc netem` 300 ms delay + 5% loss via a netns-sharing sidecar | round trip **1.6 s → 7.9 s**, confirmed numerically |

**Two scenarios not achieved, reported rather than faked.** *Clock skew*: containers on one Docker
host share a kernel clock and Docker gives them no private time namespace, so skewing one means
skewing the host including the test script; the right home is an injected fake clock in the Go
harness, which does not exist yet. *Disk pressure*: a 3-node throwaway cluster on 8 MiB volumes
showed real leader-flapping under a write hammer, but no `ENOSPC` was confirmed, so the script
asserts only that the experiment ran end to end.

| k6 load test: real streaming `Generate`, 3 tenants, 0 → 20 → 0 VUs over ~56 s, **two** workers | Result |
|---|---|
| Checks (stream completed, first token arrived, stream reached done) | **552 at 100%** |
| `grpc_req_duration` p95 threshold (<5000 ms) | **FAILED at ~10.6 s** |

**What this shows, and why the failure is the good outcome.** Twenty VUs were pointed at two workers
on purpose. A serving layer with no admission queue should queue and grow a tail *visibly* under
demand beyond capacity, and 552/552 checks confirm nothing was dropped, corrupted or truncated — just
slow. A threshold that passes no matter how hard you push tells you nothing about where capacity ends.

---

## Final verification of the whole repository

| Check | Result |
|---|---|
| `go test -race -count=1 ./...` | **all 18 test packages `ok`, exit code 0** |
| Longest: `dsys/raft` | 257.9 s |
| Second: `dsys/chaos` | 78.1 s |
| `go build ./...` / `go vet ./...` / `gofmt` | clean |
| Commits | 10, one per milestone |

Every number here was produced by a target in the `Makefile` or a script in `scripts/`; `DESIGN.md`
§9 lists which target proves which claim.
