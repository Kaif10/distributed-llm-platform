# Phase 4 — Scheduler: leases, queues, exactly-once

Code: `sched/` (`api.go`, `queue.go`, `leader.go`), `sched/worker`, `sched/grpcserver`,
`sched/kvadapter`, `kv/client` (`client.go`, `cluster.go`, `sharded.go`), `cmd/sched`, `cmd/worker`,
`cmd/schedctl`, `py/worker.py`, `proto/sched/v1/sched.proto`, `scripts/e2e_sched.sh`.
Run: `make test-sched` (chaos test under `-race`, `Makefile:45-47`), `make e2e-sched` (real
processes, `Makefile:51-52`), `go test -run OverRaftKV ./sched/`.

Build note: the queue core (`sched/queue.go`, `sched/leader.go`) and the worker contract
(`sched/worker/worker.go`) were written by hand and are the two places where a wrong invariant
would silently produce a job that ran to completion twice with both results recorded. Everything
else — gRPC translation, the KV adapter, the client-library extraction from `kvctl`, the Python
port, the e2e script — is plumbing around those two files and was reviewed against them.

## 1. What Phase 4 builds and why

The roadmap's Phase 4 (`ROADMAP.md:96-103`) asks for a job queue *stored in the KV*, workers that
*claim with leases*, a scheduler that *reaps expired leases*, a *fencing token on every worker
write*, and *admission control* — with the exit criterion "no job runs to completion twice under
1000 random pauses/crashes."

The design decision that shapes everything else is stated in the first sentence of the package
doc (`sched/api.go:1-6`): **the scheduler is a client of the KV, holding no job state of its own.**
Every job record, both counters, every lease, and the reaper's own leader lease live in the KV
store built in Phases 1-3, and every mutation is a compare-and-swap against the record's previous
value. The `KV` interface the scheduler consumes is three methods — `Get`, `Put`, `CAS`
(`sched/api.go:66-70`) — and the only property it demands of them is linearizability
(`api.go:56-57`).

Two things fall out of that decision for free:

- **N schedulers are the same queue.** `Queue` has no mutable fields (`sched/queue.go:44-48`), so
  several `Queue`s in several processes over the same KV and prefix *are* one queue
  (`queue.go:50-51`). `cmd/sched`'s doc says the same thing operationally: producers and workers
  may talk to whichever replica they like (`cmd/sched/main.go:4-7`).
- **A scheduler crash at any instant loses nothing and duplicates nothing** (`api.go:5-6`), because
  there was never any state in the process to lose. The interesting crash cases move to *sequences
  of KV writes* interrupted halfway (section 4, Submit's write order).

```
   producers                  scheduler replicas (stateless)               workers
 (schedctl, gateway)         ┌──────────────────────────────┐        (sched/worker, py/worker.py)
        │                    │ sched A     sched B    sched C│               │
        │ Submit ──────────► │  Queue       Queue      Queue │ ◄──── Claim / Heartbeat /
        │ ◄── id | 429+retry │  reaper*     reaper     reaper│       Complete(id, gen) / Fail
        │                    └──────┬───────────┬────────┬───┘               │
        │                           │  Get / Put / CAS   │  (kv/client, kvadapter session pool)
        │                           ▼           ▼        ▼
        │                    ┌──────────────────────────────┐
        │                    │  KV store (raftkv, or shardkv via shardctrl)   Phases 1-3  │
        │                    │  <prefix>/tail   <prefix>/head                              │
        │                    │  <prefix>/job/<id>   Job{state, gen, worker, lease_until}   │
        │                    │  <prefix>/idem/<key> <prefix>/leader  ("holder|untilMs")    │
        │                    └──────────────────────────────┘
   * only the replica holding <prefix>/leader scans for expired leases (leader.go)
```

The alternative the roadmap's architecture diagram (`ROADMAP.md:45-49`) hints at and this phase
did *not* build is a scheduler that is itself a replicated state machine with its own Raft log.
Section 5 says why the client-of-KV design was chosen and what it costs.

## 2. Leases vs locks

The package doc's argument (`sched/api.go:8-28`) in three steps:

**A lock is not enough.** A worker that takes a plain lock on a job and then dies holds that lock
forever, because *nobody can tell a dead lock-holder from a slow one* (`api.go:12-14`). Every
distributed lock service you have used (ZooKeeper ephemeral nodes, etcd leases, Redis SETNX with
EX) has a timeout bolted on precisely because of this — and the moment it has a timeout, it is
not a lock, it is a lease.

**A lease alone is not safe.** A lease is permission that expires. The reaper reclaims a job whose
lease lapsed and hands it to someone else — that is liveness restored. But the original holder
*does not know it was paused* (`api.go:16-19`). It was stopped by the OS, or sat in a 30-second GC
pause, or was on the wrong side of a partition; the lease expired without its knowledge; it wakes
up, finishes the job, and writes its result on top of whatever the new holder did. That is the
zombie. Making the lease longer does not fix it, only makes it rarer; no finite lease is long
enough against an unbounded pause.

**The fencing token is what makes it safe.** Every time a job changes hands, its generation `gen`
increments, and a worker may only write while presenting the `gen` it was handed at Claim
(`api.go:21-23`). Kleppmann's fencing-token argument (DDIA ch. 8, "Fencing tokens"), in this
codebase's terms: the lease service (here, `Claim`) hands out a monotonically increasing token with
each grant, and the *storage being protected* — not the lease holder — refuses any write whose
token is older than the highest it has seen. The check has to live at the storage because the
zombie, by definition, cannot be trusted to check itself. Here the storage and the lease record are
the same KV row, so the check is one read-then-CAS:

- The token is minted at `queue.go:342` — `next.Gen++ // the fence: every previous holder is now
  stale` — and again wherever a job changes hands without a new Claim: `Fail` (`queue.go:458`),
  `Reap` (`queue.go:540`), and the attempts-exhausted branch inside `Claim` (`queue.go:334`).
- The token is checked by `checkHolder` (`queue.go:369-383`), called at the top of every
  worker-side mutation: `Heartbeat` (`:393`), `Complete` (`:422`), `Fail` (`:451`). A RUNNING job
  whose `Gen != gen` is `ErrFenced`; so is a DONE job under a different gen.
- The check is *enforced*, not merely performed, by the KV's CAS: `casJob` compares against the
  exact raw bytes that were read (`queue.go:124-135`), so even if two schedulers race between
  `checkHolder` and the write, only the one whose read reflects the latest committed record wins.
  A `checkHolder` that passed against a stale read produces a CAS that fails and a loop that
  re-reads (`queue.go:407-409`), at which point `checkHolder` says `ErrFenced`.

Summed up in the package doc's own words (`api.go:26-28`): clock skew, GC pauses and network delay
can all make a lease expire "wrongly"; none of them can make two holders both commit, because only
one holds the current gen. **Leases are for liveness. Fences are for safety.** Never let a design
depend on a lease not expiring early — `leader.go:20-22` calls that "a design that is wrong on every
real machine."

## 3. Effectively-once, stated honestly

`sched/api.go:30-39` is the most important paragraph in the phase and it is a *disclaimer*:

> A job may EXECUTE more than once ... What is exactly-once is the COMMIT: Complete succeeds for at
> most one gen per job, so the recorded result is written once.

The scenario is concrete: a worker runs the job to the end, is declared dead a moment before it
calls `Complete`, and the next holder runs it again. Both executions happened. Only one `Complete`
is accepted. If the handler's side effect was "write `result` into the job record," exactly-once
holds end to end. If it was "charge the customer's card," it does not, and no queue can make it —
the handler must be idempotent, keyed by `job.Id` (`api.go:35-37`; `worker.go:21-28`,
`worker.go:60-61`).

This is *exactly* Phase 1's `RequestMeta` lesson (`docs/phase1.md` §2.6) one level up. There, a
client retrying a Put across a lost reply needed a `(client_id, request_id)` pair so the store
could recognise the duplicate and answer from the cache instead of applying it twice. Here, a job
re-executing across a lost lease needs its effects keyed by `job.Id` so the *downstream* system can
recognise the duplicate. The queue does its part — it is the "store" that deduplicates the commit
on `(id, gen)` — and then hands the same obligation to whoever is one layer above it. The package
doc says it flatly: "There is no distributed queue that gives exactly-once execution; anyone who
says otherwise is describing at-least-once execution plus idempotent effects" (`api.go:37-39`).

## 4. Walk the code in request order

### 4.1 Key layout and the scan

Five kinds of key under `Options.Prefix` (`sched/queue.go:9-15`): `/tail` (ids ever assigned;
ids are `1..tail`), `/head` (lowest id that may still be non-terminal), `/job/<id>` with the id
zero-padded to 20 digits so keys sort (`queue.go:65`), `/idem/<key>`, and `/leader`. A `Job`
record (`proto/sched/v1/sched.proto:45-62`) carries `state`, `gen`, `worker`, `lease_until_ms`,
`attempts`, `result`, `last_error`, `submitted_ms`.

There is no secondary index. `Claim` and `Reap` walk ids from `head` toward `tail`, one
linearizable read per step (`queue.go:17-28`). The doc comment describes the steady-state shape a
healthy queue converges to: a prefix of DONE jobs (skipped by `head`), then RUNNING ones (skipped
by the scan), then PENDING ones (claimed). `ScanLimit` (`api.go:107-111`, default 256) bounds the
worst case.

### 4.2 Submit: write order, crash repair, and the idempotency placeholder

`Submit` (`queue.go:155-228`) reads `tail` and `head`, refuses with `ErrQueueFull` if
`tail-head+1 >= MaxQueue` (`:178-180`), then does two writes **in this order**: the record, as a
CAS expect-absent at `tail+1` (`:189`), and *then* the `tail` bump (`:203`). The doc comment
(`:148-154`) states why the order matters and what the reverse would cost:

- Crash between the two writes: a record exists at `tail+1` that `tail` does not cover. The next
  Submit's expect-absent CAS at that id *fails* (`:193`), which is the signal — someone is there —
  and it repairs `tail` past the orphan (`:197`) and retries at the next id. `Claim`'s scan tolerates
  the transient gap the other way round too: a `readJob` that says `ErrNotFound` is simply skipped
  (`:307-309`). `TestSubmitRepairsAfterCrash` (`queue_test.go:327`) plants exactly this orphan and
  proves the next Submit gets id 3, `tail` ends at 3, and all three are claimable in order.
- The reverse order (bump `tail`, then write) would leave a *permanent hole* on a crash — an id that
  `tail` covers but nothing was ever written to, indistinguishable from "not written yet," which
  every scan forever after would have to step over.

Losing the `tail` CAS is not an error either: it means someone else's repair path already moved it
at least as far, so the loser re-reads and pushes only if `tail` is still behind (`:203-220`).

**The idempotency key uses the lease trick a second time** (`queue.go:230-233`). `claimIdem`
(`:238-283`) does not bind `key -> id` in one step, because the Submit that reserved the key might
crash before it has an id to bind. Instead it first CASes a placeholder `pending|<untilMs>` with a
30-second TTL (`:265-266`). A concurrent Submit under the same key sees a live placeholder and
waits (`:251-261`); a Submit that finds an *expired* placeholder steals it (`:262`) — the reserving
Submit is presumed dead, the same reasoning as reaping a job lease. Only after the job is allocated
does `release(id)` CAS the placeholder to `id|<n>` (`:273-281`). The failure mode is documented
right there (`:274-277`): if a Submit is slower than the TTL, its placeholder is stolen, the thief
also allocates, and the key ends up with two jobs — "preferable to a stuck key." See section 10.

### 4.3 Claim: scan, lazy head, reclaim-in-passing, and the raw-bytes rule

`Claim` (`queue.go:292-362`) reads `head` and `tail` once, then per id:

- **Terminal (`:316-326`)**: try to advance `head` past it, but *only if this id is exactly the
  `head` we read*, CASing against the raw bytes we read. Anyone else who already moved `head` makes
  our CAS lose, which is fine — lazy, best-effort, and safe. `TestSubmitClaimComplete`
  (`queue_test.go:130-138`) checks that claiming job 2 advances `head` past DONE job 1 in passing.
- **Runnable (`:328-329`)**: PENDING, *or* RUNNING with `LeaseUntilMs < now`. The second case is
  reclaim-in-passing: a claimer does not wait for the reaper to notice an expired lease, it takes
  the job itself. `TestClaimReclaimsExpiredLease` (`queue_test.go:239`) proves a live lease cannot
  be stolen and an expired one is, with `Gen+1` and `Attempts` 2. If the expired job has already
  used `MaxAttempts`, it is FAILED instead (`:330-339`) — with `Gen++`, so the old holder is fenced
  from that too.
- The claim itself (`:340-351`): `State = RUNNING`, `Gen++`, `Worker`, `Attempts++`,
  `LeaseUntilMs = now + lease`, CAS. A lost CAS (`:353-355`) means another claimer got this id; move
  on to the next one rather than retry this one.
- **RUNNING under a live lease (`:357-358`)**: skip.

The CAS expected value is always the raw bytes `readJob` returned, never a re-marshal of the
decoded record (`queue.go:124-127`). Protobuf marshaling is not guaranteed byte-stable across
producers (field order, unknown fields, different library versions in the Go scheduler and a
future Python one), and the KV compares bytes. Compare against exactly what is stored or the CAS
will spuriously fail forever against a record some other binary wrote.

### 4.4 Heartbeat, Complete, Fail: the `checkHolder` table

`checkHolder(job, gen)` (`queue.go:369-383`) is the fence, evaluated on every worker-side write:

| job.State | job.Gen vs gen | `checkHolder` | Heartbeat | Complete | Fail |
|---|---|---|---|---|---|
| RUNNING | equal | nil | extend lease (`:399-400`) | `DONE`, store result (`:428-431`) | `Gen++`, PENDING or FAILED (`:457-467`) |
| RUNNING | different | `ErrFenced` | `ErrFenced` | `ErrFenced` | `ErrFenced` |
| PENDING | any | `ErrFenced` (`:379`) | `ErrFenced` | `ErrFenced` | `ErrFenced` |
| DONE | equal | nil (`:374-376`) | `ErrTerminal` (`:396-398`) | **nil, no write** (`:425-427`) | `ErrTerminal` (`:454-456`) |
| DONE | different | `ErrFenced` (`:377`) | `ErrFenced` | `ErrFenced` | `ErrFenced` |
| FAILED | any | `ErrTerminal` (`:370-372`) | `ErrTerminal` | `ErrTerminal` | `ErrTerminal` |

The DONE/equal row is the idempotent same-gen retry: a worker whose first `Complete` committed but
whose reply was lost retries with the same `(id, gen)`, finds the job DONE *under its own gen*, and
gets success without a second write (`queue.go:413-415`). This is the Phase 1 dedup contract
re-derived without a dedup table: the record itself carries enough (`state`, `gen`) to recognise
the replay. `TestSubmitClaimComplete` (`queue_test.go:121-124`) pins it.

PENDING with any gen is `ErrFenced` because the only way a job the caller thinks it holds is
PENDING is that `Fail` or `Reap` requeued it (both bump `gen`), so the caller's gen is stale by
construction. `TestFailRequeuesThenFails` (`queue_test.go:305-307`) proves that a `Complete` with
the pre-Fail gen is fenced.

`Fail` (`queue.go:445-476`) always advances `gen` — the caller is fenced from any further write
whether the job is requeued (`attempts < MaxAttempts`) or terminal FAILED. The returned `requeued`
bool is surfaced as `FailResponse.requeued` (`sched.proto:105-106`).

### 4.5 Reap and RunReaper

`Reap` (`queue.go:507-557`) is the same scan as `Claim` with a different action at RUNNING-expired:
`Gen++`, clear `Worker`, back to PENDING (or FAILED after `MaxAttempts`), CAS (`:539-554`). Its
doc (`:502-506`) states the concurrency story: safe from any number of processes, because every
step is a CAS and a lost CAS just means another reaper or claimer did that step first.

So why elect a leader for it? `leader.go:6-15` answers: **purely efficiency.** Ten replicas each
scanning would multiply KV read load by ten for no correctness gain. `tryLead` (`leader.go:53-72`)
is a lease on `<prefix>/leader` holding `holder|untilMs`, taken over when expired, renewed when
ours; `RunReaper` (`leader.go:89-108`) tries every `interval`, with `ttl = 3*interval` (`:93`) so a
leader survives two missed ticks. If two replicas both believe they lead — skew, a GC pause across
the boundary — "the worst case is a brief period of double scanning" (`:14-15`).

The doc comment then generalises the rule the whole phase is built on (`leader.go:17-22`): use a
lease to decide who *should* do work; use a fence — or, here, the fact that every write is a CAS —
to make sure a stale lease-holder *cannot do harm*. The job leases and the reaper lease are the
same mechanism with the safety supplied two different ways.

## 5. Time is an input

`Options.Clock` (`sched/api.go:99-106`) is the only source of "now" in the package; tests inject
a fake (`queue_test.go:69-77`) and drive lease expiry without sleeping. Two consequences:

**Lease expiry is judged by the scheduler's clock, not the KV's.** A `LeaseUntilMs` was written by
whichever replica handled the Claim or Heartbeat; whether it is expired is decided by whichever
replica handles the next Claim or Reap (`queue.go:329`, `:536`). Two replicas whose clocks disagree
by *d* will disagree about expiry by *d*. The `Clock` doc states the classification exactly: that is
"a liveness concern (a lease reaped early or late), never a safety one (fencing)" (`api.go:103-105`).
A reap that fires early because a replica's clock is fast costs a re-execution — which the
effectively-once contract already permits — and a `gen` bump, which fences the still-running
holder. A reap that fires late costs waiting. Neither lets two holders both commit.

**The alternative design and why it was not chosen.** A scheduler built as its own replicated state
machine (etcd's lease implementation is the reference) cannot have replicas consult their local
clocks inside `apply`, for the reason Phase 1 §2.5 and Phase 2 established: `apply` must be a
deterministic function of the log, and "now" is not in the log. Such a design has to make the
leader *propose* time — log an explicit "lease X expired" or "the clock is now T" entry — so every
replica expires the same lease at the same log index. That is more machinery, a leader-only clock
authority, and a second Raft group to operate. This phase instead keeps consensus where Phases 2-3
already put it (the KV), keeps the scheduler stateless, and accepts the skew-as-liveness trade. It
also matches the roadmap's control-plane picture (`ROADMAP.md:45-47`): the KV "holds: quotas, cache
index, job queue, worker registry, leases" — the queue is one more tenant of the store, which is
what lets Phase 5 build the rate limiter and semantic-cache index the same way.

## 6. Admission control and backpressure

The queue is bounded (`api.go:41-46`). `Submit` refuses once `tail-head+1 >= MaxQueue`
(`queue.go:178-180`; default 1024, `api.go:118-120`), `grpcserver` maps `ErrQueueFull` to
`ResourceExhausted` with `retry_after_ms=200` in the status message
(`sched/grpcserver/server.go:50-51`, `RetryAfter` at `:34`), and `schedctl` parses that hint and
sleeps it out (`cmd/schedctl/main.go:43`, `:146-161`). The reason to shed at the door is in the
package doc (`api.go:44-46`): refusing while the client can still do something about it beats
accepting work that will time out in the queue. `TestQueueFull` (`queue_test.go:144`) proves the
bound and that finishing a job — once `head` advances past it — frees a slot.

Note the depth is `tail - head`, and `head` advances *lazily*. A queue whose terminal prefix has not
yet been walked past reports itself fuller than it is until the next Claim or Reap notices. That is
a conservative error (refuse slightly early), consistent with the intent.

The idle side: `ErrNoJob` is *not* an error at the gRPC layer — `Claim` returns `Found: false`
(`grpcserver/server.go:75-81`), and the worker polls again with exponential backoff from
`PollInterval` (200 ms) doubling to 2 s, reset on a successful claim (`worker.go:75-78`,
`:189-228`, `maxPollInterval` at `:114`). There is no long-poll or push; a wake-up channel is a
natural extension once there is a leader to own it.

## 7. The worker contract

`sched/worker`'s package doc (`worker.go:1-34`) and `runJob`'s invariant list (`worker.go:231-248`)
are the contract. Numbered as the code numbers them:

1. The handler runs under a child context; a heartbeat goroutine renews the lease every `Lease/3`
   with the `(id, gen)` from Claim (`worker.go:261-283`).
2. **Cancel on fence.** If a Heartbeat returns `FailedPrecondition`, the job is no longer ours; the
   handler's context is cancelled *immediately* with `errFenced` (`:288-293`) and its eventual return
   value is discarded (`:308-312`). Further work would produce a result the scheduler will refuse.
3. **The single most important line.** On handler success, `Complete(id, gen, result)`. If that is
   `FailedPrecondition`, we are a zombie: log at Warn, count `fenced`, **move on** (`:329-333`).
   The doc spells out the three things not to do (`:243-246`): do not retry Complete (the CAS would
   refuse it again), do not re-Claim (that would hand the same job to the same process under a new
   gen while the old attempt is still running — `:17-19`), do not crash. "This one branch is what
   keeps a stale worker from ever overwriting a live one's result" (`:245-246`).
4. On handler error, `Fail(id, gen, err)`, identical fenced handling (`:340-352`).
5. Transient errors (`Unavailable`, `DeadlineExceeded`; `isTransient`, `:402-408`) are retried with
   backoff against the *same* `(id, gen)` (`rpc`, `:375-392`); the request is built once and closed
   over, so a retry is the same logical write, never a new claim. A stuck heartbeat that outlives the
   lease simply becomes a fenced heartbeat on its next attempt (`:31-33`).

Two more details worth knowing: a handler panic is recovered and reported via `Fail` so one bad job
cannot take the worker down (`callHandler`, `:361-369`); and reporting runs under a context
detached from the run context's cancellation, bounded by `reportTimeout` (10 s), so a worker that is
shutting down still records the job it just finished (`:314-318`).

`FailedPrecondition` is deliberately the code for *both* `ErrFenced` and `ErrTerminal`
(`grpcserver/server.go:14-16`, `:52-53`): from the worker's side they mean the same thing, "the
job's state no longer permits what you asked; do not retry as-is."

`py/worker.py` is the same shape in Python (`py/worker.py:1-21`): a heartbeat thread every
`lease/3` (`:108-120`), a `fenced` `threading.Event` handed to the handler as the cooperative cancel
since Python threads cannot be killed (`:105`, `:159-168`), the zombie branch that logs and never
retries (`:146-154`), transient codes retried with the same request (`:65-76`). This is the shape
Phase 5's inference workers will take: a vLLM process that claims a generation request under a
lease, streams tokens while heartbeating, and treats a fenced heartbeat as "stop generating, the
gateway has re-dispatched this prompt."

## 8. Cost model, stated plainly

Every KV operation the queue makes is a linearizable operation against Raft. In the Phase 2 design
reads go through the log too (`docs/phase2.md` §5), so a `Get` is a Raft round, not a local read.

- **Submit** without an idempotency key: `Get tail`, `Get head`, `CAS job`, `CAS tail` — four
  rounds. With a key, add `Get idem`, `CAS placeholder`, `CAS bind`. Call it 3-4 Raft rounds plus
  the gRPC hop from the producer, and add another round per lost CAS under contention.
- **Claim** is `O(distance from head to the first runnable job)` reads, each a Raft round, plus one
  CAS to take the job and possibly one to advance `head` (`queue.go:17-28`). In steady state with
  lazy head advance and a mostly-drained queue, distance is small; with a long RUNNING prefix
  (many workers, long jobs) every Claim re-reads all of them. `ScanLimit` caps the damage at 256
  reads per Claim, at which point Claim returns `ErrNoJob` even if a runnable job exists beyond it.
- **Heartbeat / Complete / Fail**: one read, one CAS.
- **Reap**: the same scan as Claim, on one replica, every `reap-interval`.

The e2e run measured **120 submits in 17 s** (section 9). Some of that is the script forking a fresh
`schedctl.exe` per submit (`scripts/e2e_sched.sh:66-69`) — process start plus a new gRPC connection
each time — and the rest is one gRPC round to the scheduler plus the Raft rounds above against
three real replicas fsyncing on one disk. It is not a throughput number; it is a reminder that a
CAS-per-step design over consensus pays consensus latency per step.

**What an index would buy and cost.** A `<prefix>/pending` list (or a per-state counter) would make
Claim O(1) reads. It costs a second key that must be kept consistent with the record on every
transition — Submit, Claim, Fail, Reap — with no multi-key transaction to do it atomically. Each of
those becomes a two-step protocol with its own "crashed between the two writes" repair path, the
same shape as Submit's record-then-tail ordering but at four sites instead of one. That is
Exercise (a). The current design chose one crash-repair story over five.

**The session pool** (`sched/kvadapter/kvadapter.go:6-23`). The scheduler serves concurrent gRPC
handlers, and `kv/client`'s rule is one outstanding mutation per client identity
(`kv/client/client.go:18-33`) — the exact invariant whose violation was Phase 3's bug 2. So each
concurrent CAS needs its own `Session()` (`client.go:164-166`). But a fresh identity per operation
would cost the *store* one permanent dedup-table row per mutation, replicated through Raft and
carried in every snapshot, forever (`kvadapter.go:12-16`). A bounded pool of 32 sessions
(`DefaultSessions`, `:36`), checked out per operation through a buffered channel (`:51-58`,
`:75-84`), costs the store exactly 32 rows for the scheduler's lifetime. Gets carry no meta and
bypass the pool (`:22-23`, `:86-88`). Compare `raftkv_test.go:34-42`, which mints a fresh identity
per op because a test's dedup table does not have to live past the test.

`kv/client` itself is the retry/leader-hint machinery extracted from `kvctl` into a library:
`doOn` (`kv/client/cluster.go:208-245`) is the one retry loop, following `leader=<addr>` hints
(`:30`, `:165-179`); sharded mode caches one `cluster` per group (`sharded.go:58-67` — the Phase 3
bug 1 fix, now with its reasoning in the doc comment) and re-resolves on wrong-group
(`sharded.go:177-200`). `cmd/sched` accepts either `-kv` (flat raftkv) or `-ctrl` (sharded) and
the queue does not know which (`cmd/sched/main.go:11-19`, `:64-74`).

## 9. How we know it works

**Unit tests over an in-memory linearizable KV** (`sched/queue_test.go`; `memKV` is one mutex,
`:26-66`, which is exactly the guarantee Raft provides and exactly what the CAS discipline needs):

| Test | What it proves |
|---|---|
| `TestSubmitClaimComplete` :96 | ids are sequential; Claim yields gen 1, attempts 1; a same-gen Complete retry is a no-op success; the next Claim advances `head` past the DONE job; an empty queue is `ErrNoJob` |
| `TestQueueFull` :144 | `MaxQueue` is enforced on `tail-head+1`; draining one job and advancing `head` frees a slot |
| `TestIdempotentSubmit` :164 | a repeat Submit under the same key returns the original id and does not allocate (`tail` stays 2) |
| `TestFencingAfterReap` :187 | the whole point: reap bumps gen; a fresh Claim takes gen+2; the zombie's Heartbeat, Complete and Fail under the old gen are all `ErrFenced`; the recorded result is the legitimate holder's; a zombie Complete *after* the real one is still refused |
| `TestClaimReclaimsExpiredLease` :239 | a live lease cannot be stolen; an expired one is reclaimed in passing with gen+1, attempts 2; the old holder is fenced |
| `TestHeartbeatExtendsLease` :256 | a heartbeat pushes `LeaseUntilMs` out by a full lease from *now*; Reap does not touch a job whose lease was renewed |
| `TestMaxAttemptsViaExpiry` :274 | a job that expires `MaxAttempts` times is FAILED, not requeued, and is no longer claimable |
| `TestFailRequeuesThenFails` :297 | Fail requeues with gen+1 (the old gen is fenced), the next Claim gets gen+2 and attempts 2, the second Fail is terminal; Heartbeat on FAILED is `ErrTerminal` |
| `TestSubmitRepairsAfterCrash` :327 | an orphan record at `tail+1` (a Submit crashed between its two writes) is repaired by the next Submit; nothing is lost, all three jobs are claimable in order |
| `TestLeaderLease` :355 | acquire, refuse a competitor while live, renew, take over after expiry |

**The chaos test, `TestExactlyOnceUnderChaos`** (`queue_test.go:382-560`) is the roadmap's exit
criterion. The design choices, each deliberate:

- *A fake clock advancing in its own goroutine* (`:414-425`): every millisecond of real time,
  5-25 ms of fake time. That is what turns a worker's real `time.Sleep` into an *expired lease* —
  the pause happens in real time, expiry is judged in fake time, and fake time keeps moving while
  the worker is asleep, exactly as a wall clock keeps moving while a process is SIGSTOPped. Without
  this the test would need real 100 ms leases and real multi-second sleeps to produce a thousand
  events.
- *Eight workers, three behaviours* (`:471-487`): 25% crash (claim and never return, `:472-475`),
  30% pause past the lease and then try to commit anyway (`:476-480`), the rest heartbeat once and
  complete. A reaper runs every 2 ms (`:428-441`). `MaxAttempts` is effectively unbounded so a
  crashed-many-times job never goes FAILED and the assertion stays "every job DONE."
- *Why `fenced > 0` is asserted* (`:533-535`): a run in which no worker was ever fenced did not
  exercise the zombie path at all, and a passing result would be vacuous. The test refuses to pass
  on a scenario that did not happen.
- *Why "accepted Completes == jobs" is the property* (`:537-559`): it is checked three ways — every
  job is DONE, no job has more than one accepted Complete (`completions` map, `:546-549`), and the
  total accepted equals `nJobs`. The last one closes the gap the first two leave: a job could be
  DONE with exactly one *counted* Complete while a second Complete was accepted but its counter
  increment raced; the total catches it.

Measured (in-memory KV, fake clock, 8 workers, `-race`): **2000 jobs, 1334 pauses past the lease,
1124 crashes, 1314 zombie writes fenced, exactly 2000 accepted Completes, every job DONE once,
5.1 s.** That is 2458 chaos events against the roadmap's asked-for 1000.

**Over a real Raft KV** (`sched/raftkv_test.go`, `TestOverRaftKVWithLeaderLoss` :130): the same
queue over three `raftkv` replicas on `raft/simnet`, called in-process, with the current leader
disconnected 400 ms in for 1.5 s and then reconnected (`:199-211`). Every counter and record round
trips through consensus; the leader outage turns into `Unavailable` that `raftKV.do` retries against
the next replica (`:44-65`). Measured: **30 jobs, exactly-once, 2.6 s.** This is what proves the CAS
discipline composes with Phase 2 rather than merely with a mutex.

**Real processes** (`scripts/e2e_sched.sh`, `make e2e-sched`), step by step:

1. `make build`; start three `raftkv` replicas on 7301-7303 and probe them with `kvctl put`
   (`:41-47`).
2. Start one `sched` with `-lease 1500ms -reap-interval 300ms -max-attempts 1000` (`:50`).
3. Start three workers (`:55-61`): `healthy-1` and `healthy-2` at concurrency 2, and `zombie` at
   concurrency 1 with `-slow-after 3 -suppress-heartbeat` — after three normal jobs it sleeps 3x
   the lease on the fourth and never renews any lease (`cmd/worker/main.go:60-61`, `:84-87`;
   `worker.Options.SuppressHeartbeat`, `worker.go:82-86`).
4. Submit N jobs with random 20-100 ms sleeps, one `schedctl submit` process each (`:64-70`).
5. One second later, `kill` `healthy-1` mid-run (`:72-75`) — a crash with jobs in flight.
6. Poll `stats` until `head > tail`, or every job from head to tail reads DONE (`:77-93`).
7. Verify every job DONE and none FAILED (`:95-104`); verify the zombie's log contains "fenced"
   (`:106-111`); count results attributed to `zombie` — legitimate only if written under a live
   lease, which the store guarantees (`:113-122`).

Measured (mentor's run, 2026-09-12): **3 raftkv + 1 sched + 3 workers as separate OS processes; 120
jobs submitted in 17 s; one healthy worker killed mid-run; the slow worker fenced exactly once; all
120 DONE, 0 FAILED; 13 results legitimately written by the slow worker while its lease was still
live.** The "exactly once" for the zombie is the expected count: `-slow-after 3` makes it sleep past
its lease on exactly one job; the other 13 it finished within 1.5 s and committed under a current
gen, which is correct — being slow is not a crime, only being slow *and stale* is.

## 10. What is NOT implemented, on purpose

- **No priority levels, and so no priority-inversion handling.** The roadmap lists priority
  inversion as a concept (`ROADMAP.md:97`); this queue is strictly FIFO by id. What inversion would
  look like here, once priorities exist: an urgent job blocked behind cheap ones in two distinct
  ways — the *scan* (a high-priority job at id 900 behind 800 RUNNING low-priority ones is 800 reads
  away, or beyond `ScanLimit` entirely), and *admission* (a low-priority tenant fills `MaxQueue`
  and the urgent tenant gets `ResourceExhausted` at the door). Exercise (b).
- **No work stealing across queues.** Two `Prefix`es are two fully independent queues
  (`api.go:89-91`); a worker idle on one does not look at the other.
- **No job TTL or cancellation.** A submitted job stays PENDING until claimed, however long that is,
  and a producer has no way to withdraw it. Exercise (c) shows that the machinery is already there.
- **No result GC.** DONE records and their `Result` bytes live in the KV forever, and so do `idem`
  bindings. `head` moves past them but nothing deletes them. Same shape as Phase 3's unfreed
  `outgoing` snapshots; same reason it is left as a separable problem.
- **No per-tenant fairness.** One producer can fill the queue; `MaxQueue` is global.
- **The idempotency-key TTL edge case** (`queue.go:274-277`): a Submit slower than the 30 s
  placeholder TTL has its reservation stolen and two jobs are created for one key. Documented in
  the code as the accepted trade against a permanently stuck key. Any lease-based reservation has
  this hole; the only fixes are a fence on the bind (the reserver CASes against *its own* placeholder
  bytes, which it already does — so the *slow* Submit's bind fails, but its job was already
  allocated) or no TTL at all.
- **`RetryAfter` is a constant** (`grpcserver/server.go:32-34`), not derived from drain rate.

## 11. Exercises

**(a) A pending index to make Claim O(1).** Add `<prefix>/pending`, a list (or a head/tail pair of
its own) of PENDING ids, and have Claim pop from it instead of scanning. Then enumerate every
transition that must now touch two keys — Submit (record + pending push), Claim (record + pending
pop), Fail-requeue (record + push), Reap-requeue (record + push) — and for each write down the
crash-between-the-two-writes repair path, exactly as `Submit`'s doc comment does for record-then-tail
(`queue.go:148-154`). Decide which write goes first at each site and why. Measure Claim's read count
before and after with the chaos test's `gets` counter (`memKV.gets`, `queue_test.go:29`).

**(b) Priority levels with anti-starvation.** Give `Submit` a priority and keep one `head`/`tail`
pair per level. Claim scans high first. Now show the starvation: a steady trickle of high-priority
work means low never runs. Add aging (a low-priority job older than T is served as high) or a
weighted pick (serve low one time in N), and decide which of the two admission problems from
section 10 you solve by per-priority `MaxQueue` and which you cannot.

**(c) Job cancellation via a gen bump.** Cancel is a state transition to a new terminal state
`CANCELLED` with `Gen++`. Convince yourself in writing that nothing else is needed: the running
worker's next Heartbeat is fenced (so rule 2 cancels its handler), its Complete is fenced (rule 3
discards the result), and Claim's `terminal()` (`queue.go:137-139`) skips it. Then find the one
thing that *is* needed: what `checkHolder` returns for CANCELLED, and why the DONE/equal row's
"idempotent success" must not apply to it.

**(d) Reap-on-claim only.** Delete `RunReaper` and the leader lease; rely solely on Claim's
reclaim-in-passing (`queue.go:328-329`). Run the chaos test and the e2e. It passes (why?). Then
construct the case it makes worse — an expired job at the *front* of the queue, no worker currently
claiming because they are all busy on long jobs — and measure how long that job waits versus the
`reap-interval` design. That delay is the price of dropping the leader; decide if it is worth it.

**(e) The roadmap's experiment, by hand.** Run the three commands in `cmd/worker/main.go:14-25`
(`healthy` at `-lease 2s`; `zombie` at `-lease 2s -slow-after 1 -suppress-heartbeat`; two submits
and `schedctl watch 2`). Write down the exact `watch` lines you see — the `(state, gen, worker,
attempts)` transitions with timestamps — and the exact zombie log line. Then re-run *without*
`-suppress-heartbeat` and write down why the slow job now completes on `zombie` (`main.go:32-34`).

**(f) A deterministic-simulation version of the chaos test.** Replace `memKV` with a fake whose
`CAS` randomly returns `swapped=false` (and occasionally an error) from a seeded RNG, and whose
`Get` occasionally returns a value one write stale — the latter is *not* linearizable and the test
should fail; the former is a normal outcome (`api.go:63-65`) and it must pass. Make the clock
goroutine and the worker scheduling seed-driven too, so a failing seed reproduces. This is the
Phase 6 harness in miniature.

## 12. Interview questions with model answers

1. **Design a distributed task queue.** State the guarantee first: at-least-once execution,
   exactly-once commit, handlers idempotent on job id. Store every job as a record in a linearizable
   KV; producers append under a monotonically assigned id; workers pull the oldest runnable record
   under a time-bounded lease and heartbeat it; every worker write carries a fencing token that
   increments each time the job changes hands and is checked by the store, not the worker; a reaper
   returns expired leases to the pool. Bound the queue and refuse at admission with a retry hint.
   Make the queue front end stateless so it scales horizontally and crashes without consequence.
   Then name what you left out — priorities, fairness, cancellation, result GC — and how each would
   bolt on (section 10).

2. **Why is a lock not enough?** Because a lock's holder can die holding it and nobody can tell a
   dead holder from a slow one (`api.go:12-14`). You add a timeout to get liveness back, and now you
   have a lease; a lease can expire while the holder is merely paused, and the holder does not
   know, so you need something *else* for safety. That something is the fencing token.

3. **What is a fencing token and where is it checked?** A monotonically increasing number handed
   out with each grant of the lease. The check lives at the resource being protected, not at the
   holder, because the holder is exactly the party that cannot be trusted to know it is stale.
   Here `gen` is minted in `Claim`/`Fail`/`Reap` (`queue.go:342`, `:458`, `:540`) and checked in
   `checkHolder` (`:369-383`) before every worker write, with the KV's CAS making the check atomic
   with the write.

4. **Exactly-once vs effectively-once.** Exactly-once *execution* is impossible across a network
   with failures: a worker can finish the work and die before saying so, and the system cannot
   distinguish that from a worker that never started. What is achievable is exactly-once *commit* —
   at most one acceptance of a result per job — plus idempotent side effects keyed on the job id,
   which together are indistinguishable from exactly-once to an observer. Same lesson as Phase 1's
   `RequestMeta`, one layer up (section 3).

5. **A worker is alive but slow. What happens?** If it heartbeats, nothing: heartbeats extend the
   lease every `Lease/3` (`worker.go:263`) and slow is allowed. The e2e's zombie legitimately
   committed 13 results that way. If it stops heartbeating (paused, partitioned), its lease lapses,
   a claimer or the reaper bumps `gen` and hands the job on, and its eventual Complete is refused —
   at which point the *only* correct behaviour is to discard the result and move on
   (`worker.go:243-246`). The work was wasted; nothing was corrupted.

6. **What if the scheduler's clock is wrong?** Expiry is judged by the scheduler's clock
   (`api.go:102-105`), so a fast clock reaps early and a slow clock reaps late; replicas that
   disagree, disagree by their skew. Both are liveness effects — extra re-execution or extra
   waiting. Neither is a safety effect, because safety comes from `gen`, and `gen` does not involve
   time. The design was chosen so that this sentence is true; a design in which correctness depends
   on clocks agreeing "is wrong on every real machine" (`leader.go:21-22`).

7. **How would you add priorities without starvation?** Separate head/tail pairs per level, scan
   high first, and either age low-priority jobs into higher levels after a threshold or pick levels
   by weighted lottery so low always has a nonzero share. Separately bound admission per level, or
   a flood of low-priority submits will consume `MaxQueue` and the high-priority tenant gets refused
   at the door — the admission form of priority inversion (Exercise b).

8. **Compare with SQS, Celery, Kafka consumer groups.** SQS is this design's closest relative:
   visibility timeout is the lease, `ChangeMessageVisibility` is the heartbeat, `ReceiveCount` is
   `attempts`, the dead-letter queue is FAILED — but SQS has no fencing token, so a message whose
   visibility timeout lapsed can be processed twice with *both* side effects landing; you are
   expected to dedup downstream. Celery with a broker gives at-least-once with `acks_late` and, like
   SQS, no fence. A Kafka consumer group gives ordered, partition-exclusive consumption with the
   consumer's committed offset as a coarse progress marker; a rebalance is the analogue of a reap,
   and the generation id Kafka attaches to group membership *is* a fencing token — but at partition
   granularity, not per message, and only for offset commits, not for the consumer's side effects.
   This queue fences per job, at the record.

9. **How would you test it?** Three layers, each catching what the previous cannot. An in-memory
   linearizable KV with an injected clock so thousands of pauses-past-the-lease run in seconds and
   the assertion is "accepted Completes == jobs, fenced > 0" (section 9's chaos test). The same
   queue over a real Raft cluster with a leader disconnected mid-run, to prove the CAS discipline
   survives consensus-layer failures and not just a mutex. And real OS processes with a killed
   worker and a deliberately non-heartbeating one, because the Phase 3 bugs taught that some things
   only show up under real gRPC and real latency.

10. **Walk me through `head`/`tail` and their failure modes.** `tail` is ids ever assigned, `head`
    the lowest id that may be live (`queue.go:11-12`). Submit writes the record first and bumps
    `tail` second so a crash between them leaves a detectable orphan, repaired by the next Submit's
    failed expect-absent CAS (`:148-154`, `:193-198`). `head` advances lazily, by whoever notices a
    terminal record at exactly `head`, CASing the raw bytes it read (`:316-326`); a lost CAS means
    someone else advanced it. Failure modes: a long RUNNING prefix makes every Claim re-read it
    (cost, not correctness; `ScanLimit` caps it and can hide runnable jobs beyond the cap); lazy
    `head` makes the queue look fuller than it is to admission (refuses early — conservative); and a
    record at `tail+1` that was written but never covered is claimable *before* `tail` covers it,
    which is fine because the record, not the counter, is the source of truth.

## 13. Your notes

Answer these in your own words, here, before starting Phase 5.

1. Explain, to someone who has only read Phase 2, why the scheduler is a client of the KV rather
   than its own Raft group. Be concrete about what a replicated-state-machine scheduler would have
   to do about `time.Now()` inside `apply`, and what this design gives up in exchange for not doing
   that (section 5).
2. The distributed token-bucket rate limiter in Phase 5 is "per tenant in the KV, CAS-based"
   (`ROADMAP.md:107`). Sketch its record and its CAS loop in the style of `Heartbeat`
   (`queue.go:386-411`). Then identify which of this phase's two properties — the lease or the fence
   — a rate limiter needs, and why it is only one of them.
3. The semantic cache index (`ROADMAP.md:108`) will be metadata in the KV pointing at vectors in a
   local store on some worker. That is a lease-shaped problem: the index entry is a claim that a
   particular worker holds a particular vector. Write down what "the worker died" and "the worker was
   paused and came back" each look like for a cache entry, and which one needs a `gen`.
4. KV-cache-aware routing (`ROADMAP.md:109`) wants a prompt-prefix hash to land on the same worker
   repeatedly. That is affinity, which pulls against this phase's "any worker takes the oldest
   runnable job." Design the Claim variant that prefers a worker's own prefix hashes and falls back
   to the global scan — and state exactly where in `Claim` (`queue.go:304-360`) the preference goes
   without weakening the fence.
5. In one paragraph: the chaos test asserts `fenced > 0`. Phase 5 will have a streaming handler
   whose "result" is tokens already sent to a client before Complete is called. What does "discard
   the result" (worker rule 3) mean when the result has already left the building, and what does
   that imply the gateway must do with a fenced stream?

## Reading

- DDIA ch. 8, "The Truth Is Defined by the Majority" through "Fencing tokens": the lock-holder
  GC-pause example is this phase's zombie, and the fencing-token diagram is `checkHolder`.
- Kleppmann, "How to do distributed locking" (2016), and the Redlock debate it responds to — read
  both sides, then re-read `leader.go:17-22`.
- The etcd lease and `concurrency` package docs, for the replicated-state-machine alternative in
  section 5 and how a leader proposes time.
- Amazon SQS visibility-timeout documentation and the Kafka consumer-group protocol's generation id,
  for interview question 8.
- 6.5840 Lab 1 (MapReduce) — the coordinator's task timeout and reassignment is this phase's reaper
  with no fence, and worth re-reading now to see what it would let a zombie do.
