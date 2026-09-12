# Distributed LLM Inference Platform — Learning Roadmap

**Goal:** Learn distributed systems by building one real system, from the consensus
layer up, that ends as a CV headline for senior distributed-systems / AI-infra roles.

**The one-line CV pitch we are building toward:**

> Designed and built a distributed LLM inference platform in Go and Python:
> Raft-replicated control plane with sharding and snapshots, lease-based
> exactly-once job scheduler, distributed rate limiting and semantic cache,
> KV-cache-aware routing to GPU workers; verified with a linearizability
> checker and fault-injection harness, instrumented with OpenTelemetry.

Every phase ships something runnable and maps to questions you will get asked
in interviews at OpenAI / Anthropic / Meta / Jane Street.

---

## Why this project

- It is *actually* distributed: consensus, replication, partitioning, failure
  handling, consistency models. Not "microservices with Postgres".
- It builds on what you already know (inference, serving, REST) so the domain
  is familiar and the *distributed* parts are the new learning.
- Interviewers can probe any layer and you built all of them.

## Language decision

- **Go** for the control plane, gateway, scheduler, and test harness.
  Goroutines + channels map cleanly to the concepts; it is what MIT 6.824
  labs, etcd, CockroachDB, and most infra teams use. Simple enough to learn
  alongside the material.
- **Python** for the inference workers (vLLM / HF Transformers / a mock model),
  where you are already fluent.
- Rust would impress more at Jane Street but would halve your learning
  velocity on the distributed-systems content. Rewrite one component in Rust
  at the end if you want that signal.

## Architecture (final state)

```
clients ──> gateway (N replicas, stateless)
              │  rate limit / admission / semantic cache lookup
              ▼
          control plane  = Raft-replicated, sharded KV store   (kv/)
              │  holds: quotas, cache index, job queue, worker registry, leases
              ▼
          scheduler (leader-elected)                            (scheduler/)
              │  lease-based dispatch, exactly-once, priority, backpressure
              ▼
          workers (Python, pull model, heartbeat)               (worker/)
              │  KV-cache-aware placement, prefix reuse
              ▼
          chaos + verification harness                          (chaos/)
              partitions, crashes, clock skew, linearizability checking
```

---

## Phases

Each phase: **Learn → Build → Break → Write up**. Exit criteria are the bar.

### Phase 0 — Setup and foundations (1 week)
- Install Go 1.23+, protoc, make, golangci-lint.
- Read: DDIA (Kleppmann) ch. 5 (Replication), 6 (Partitioning), 7 (Transactions), 8 (Trouble with Distributed Systems), 9 (Consistency & Consensus).
- Watch: MIT 6.5840 (formerly 6.824) lectures 1–3.
- Read: Go concurrency basics (Tour of Go, "Go by Example" channels/mutexes/select/context).
- **Exit:** `make test` runs a hello-world gRPC server + client in Go.

### Phase 1 — Single-node durable KV store (1–2 weeks)
Concepts: write-ahead log, fsync, crash recovery, idempotency, gRPC.
- Put/Get/Delete/CAS over gRPC.
- Append-only WAL with checksums, replay on startup.
- Client request IDs for idempotent retries (dedup table).
- **Break it:** kill -9 mid-write in a loop; must recover with no torn writes.
- **Exit:** crash-recovery test passes 1000 iterations.
- Interview map: "How do you make a write durable?", "What is exactly-once really?"

### Phase 2 — Raft consensus (3–4 weeks, the core)
Concepts: leader election, log replication, term/commit index, persistence, log compaction, membership.
- Implement Raft from the paper (not a library). Follow the 6.5840 Lab 3 structure: 3A election → 3B replication → 3C persistence → 3D snapshots.
- Replicate the Phase 1 KV on top: linearizable reads (ReadIndex or lease reads).
- **Break it:** partitions, dropped/delayed/duplicated RPCs, leader crashes during commit.
- **Exit:** passes a 6.5840-style stress test 500 runs in a row with random faults; linearizability checker (Porcupine) finds zero violations.
- Interview map: "Explain Raft", "Why can't a leader commit entries from previous terms by counting replicas?", "How do stale reads happen?"

### Phase 3 — Sharding and rebalancing (2 weeks)
Concepts: consistent hashing vs range sharding, shard controller, config changes, hot shards.
- Shard controller (itself a Raft group) assigns key ranges to replica groups.
- Shard migration without downtime; clients follow reconfiguration.
- **Break it:** reconfigure under load with partitions.
- **Exit:** 6.5840 Lab 4-style tests pass; throughput scales near-linearly to 3 groups.
- Interview map: "How would you scale a KV store to 1000 nodes?", "What happens to in-flight requests during a shard move?"

### Phase 4 — Scheduler: leases, queues, exactly-once (2 weeks)
Concepts: fencing tokens, leases vs locks, at-least-once + idempotency = effectively-once, work stealing, backpressure, priority inversion.
- Job queue stored in the KV; workers claim with leases; scheduler reaps expired leases.
- Fencing token on every worker write so a zombie worker cannot corrupt results.
- Admission control: bounded queue, shed load early with 429 + Retry-After.
- **Break it:** pause a worker past its lease, resume it, confirm it is fenced.
- **Exit:** no job runs to completion twice under 1000 random pauses/crashes.
- Interview map: "Design a distributed task queue", "Why is a lock not enough?"

### Phase 5 — LLM-specific layer (2–3 weeks) — this is what makes it *yours*
Concepts: distributed rate limiting, semantic caching, locality-aware routing, tail latency.
- Distributed token-bucket rate limiter per tenant in the KV (CAS-based).
- Semantic cache: embed prompt, ANN lookup, cache hit skips inference. Index metadata in KV, vectors in a local store.
- KV-cache-aware routing: hash on prompt prefix so shared prefixes land on the same worker (what vLLM/SGLang prefix caching exploits).
- Streaming responses end-to-end with cancellation propagation (context).
- Hedged requests for p99.
- **Exit:** benchmark showing p99 latency with vs without prefix routing and hedging.
- Interview map: "Design ChatGPT's serving layer", "How do you rate-limit across 50 gateways?"

### Phase 6 — Verification, chaos, observability (2 weeks)
- Deterministic simulation harness: run the whole cluster in one process with a simulated network and clock, seed-reproducible failures (FoundationDB / TigerBeetle style).
- Chaos in Docker Compose: `tc netem` partitions, clock skew, disk-full.
- OpenTelemetry traces across gateway → scheduler → worker; Prometheus + Grafana dashboard; k6 load tests.
- **Exit:** one command reproduces any failure from a seed; a dashboard screenshot for the README.

### Phase 7 — Write-up (1 week)
- Design doc with trade-offs (why Raft not Paxos, why leases, consistency guarantees per endpoint).
- Benchmarks table. Architecture diagram.
- Blog post: "What I learned building an LLM serving platform from Raft up".
- Optional: rewrite the rate limiter or WAL in Rust.

**Total: roughly 14–16 weeks at 10–15 hrs/week.**

---

## Repo layout (target)

```
distributed_systems/
├── ROADMAP.md
├── docs/            design docs, per-phase notes, interview Q&A
├── proto/           gRPC definitions
├── kv/              WAL, storage engine, Raft, sharded KV server
├── gateway/         API gateway, rate limiter, semantic cache
├── scheduler/       job queue, leases, dispatch
├── worker/          Python inference worker
├── chaos/           simulation harness, fault injection, linearizability checks
├── deploy/          docker-compose, Grafana dashboards
└── Makefile
```

## Rules for how we work

1. You write the code; I explain, review, and write tests that break it. If I write a component for you, you re-implement it before we move on.
2. Every phase ends with a `docs/phaseN.md` in your own words. This becomes your interview prep.
3. No consensus libraries (etcd/raft, hashicorp/raft). Libraries are fine for gRPC, metrics, tracing, embeddings.
4. Commit per milestone with a message that says what guarantee was added.
