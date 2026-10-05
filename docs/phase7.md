# Phase 7 — What I learned building an LLM serving platform from Raft up

Over about four months I built a distributed LLM inference platform in Go and Python, starting from
an empty directory and an append-only file and ending with a Raft-replicated sharded control plane,
a lease-and-fencing job scheduler, a serving gateway with distributed rate limiting and prefix-aware
routing, and a chaos harness that checks the whole thing with a linearizability checker under
injected partitions and crashes. No consensus libraries — Raft is implemented from the paper.

This is the narrative account. The design is in `DESIGN.md`, the numbers are in `BENCHMARKS.md`, and
each layer's full reasoning is in `docs/phase1.md`–`docs/phase6.md`. What follows is what the process
actually taught me, including the parts where I was wrong.

---

## Two things to say before anything else

**The inference backend is a mock.** The Python worker defaults to `--backend mock`, which generates
pseudo-words from a fixed word list against a latency model I wrote down deliberately: prefill cost
linear in the number of *uncached* prompt characters, a prefix cache of 64-character blocks, a fixed
per-token decode delay, and optional injected tail stalls. There is a `--backend hf` path in the
file; it is untested, and torch and transformers are not installed here. Every serving number in this
project came from the mock. There is no GPU anywhere in this repository and never was.

I want to be precise about what that does and does not invalidate, because the temptation to fudge it
is exactly why it is worth stating up front. Genuinely not measured: anything about model quality,
real GPU memory behaviour, real batching dynamics, or absolute serving throughput. Genuinely
measured: what a distributed serving layer does to hit rate, throughput and tail latency *given* a
backend whose prefill cost falls when it has already seen your prefix — which is the one property of
an inference server that routing exists to exploit, and which the mock reproduces faithfully and
identically in both its Go and Python implementations. The right description of this project is **LLM
serving infrastructure**. It has never served an LLM.

**And it is not production-grade.** It is built with production *practices* — every test runs under
the race detector, correctness is checked with Porcupine rather than asserted, failures are injected
deliberately, there is distributed tracing and there are Prometheus metrics, and chaos runs against
real containers and not just in-process fakes. It has none of the things that actually make software
production software: no authentication, no TLS, no security review, no deployment story, no on-call
story, no capacity model, no data retention. Every "three-node cluster" here is three processes
fsyncing to one laptop's SSD, so the replication is real but the independence of the failure domains
is not. It has run for minutes at a time, never days. All benchmarks ran on one memory-constrained
Windows laptop with 8 GB of RAM, over loopback or in-process simulation, which means the ratios in
`BENCHMARKS.md` mean something and the absolute numbers do not.

Both caveats cost me nothing to state and would have cost a great deal of credibility to omit. That
turns out to be a theme.

---

## Phase 1: the whole system is already here, at n=1

I started with a single-node key-value store: a map behind gRPC where every mutation goes through one
pattern. Serialize the command as a log entry. Append it to a file and wait until it is durable. Apply
it to the map. Reply. Recovery is "replay the log through step 3". That is the entire store.

What I did not expect is how much of the rest of the project was decided here. In Phase 2 the only
thing that changes is step 2 — "fsync locally" becomes "a majority of replicas have durably logged
it" — and the apply function is byte-identical. So every discipline the single-node store needed, it
needed for reasons that get *worse* with five nodes and a partition. If `apply` is not a pure function
of (state, entry), a replay disagrees with live state at n=1 and replicas diverge at n=5. If the
dedup table is not rebuilt from the log, a restart double-applies a retry at n=1 and a new leader
double-applies it at n=5.

**fsync is the only line in the project that makes anything durable, and everything else is
negotiating with it.** A `write()` copies bytes into the page cache and returns; the disk has not
been touched. The fsync is the round trip that ends the negotiation, costing roughly 0.5–2 ms on a
consumer SSD — three or four orders of magnitude more than the memcpy. Because the store held one
mutex across append-fsync-apply, throughput was pinned at about one write per fsync: **1082 puts/s**.
Group commit — let N concurrent writers land their records, do one fsync, wake all of them — took it
to **11312 puts/s at 64 clients**. The more instructive number is the one that *didn't* move: p50
stayed at about 5 ms. Group commit does not make any single write faster; it lets 64 writes share one
disk round trip. The knob is *how many writes share an fsync*, never *whether the ack waits for one*,
and once you see it that way the difference between batching (safe) and async commit (loses
acknowledged writes) stops being a matter of taste.

**Torn writes taught me an asymmetry I would not have guessed.** A crash can leave the log ending in
a partial header, a header with a partial payload, or a full-length record of garbage — so recovery
truncates to the last good record. But a bad checksum *in the middle* is not a crash artifact and must
not be skipped, because the append offset only advances after a successful append: if a complete
record exists after the bad one, the bad one was *acknowledged*. Silently skipping it loses a
committed write and replays everything after it against the wrong state. A torn tail is routine
maintenance; a torn middle is an emergency.

**Exactly-once delivery is impossible, and saying so precisely is the whole skill.** A client that
times out cannot distinguish "never arrived" from "applied, reply lost", so it must retry. What you
can build is at-least-once delivery plus server-side deduplication. The identity has to be assigned by
the client — `(client_id, request_id)`, reused on retry — because hashing the request body cannot
distinguish "same intent, retried" from "same bytes, new intent" (two clients incrementing a counter
produce identical bytes and both are meant). And the dedup table has to be populated *inside* `apply`,
so replay rebuilds it. That one placement decision is what makes deduplication survive a restart at
n=1 and survive a failover at n=5.

The crash test — kill mid-write, 200 times, check every acknowledged write is still there — passed
with zero losses. It also passes with fsync *disabled*, because `kill -9` doesn't drop the page cache.
I wrote that limitation into the phase doc rather than quietly enjoying a green test. Knowing what
your test does not prove is worth about as much as the test.

---

## Phase 2: paying 33x for something, and knowing exactly what

Raft took the longest of any phase and was the most worthwhile. I implemented it from the paper —
elections with randomized timeouts, replication with conflict-hint backtracking, persistence before
every reply, snapshots with log compaction — against a simulated network that drops, delays and
reorders RPCs, and a harness that continuously checks that no two nodes ever apply different commands
at the same index.

The headline result is a **33x throughput regression**, and it is the honest centrepiece of the phase.
Single node with group commit: 11312 puts/s. Three processes with Raft: **339 puts/s, p50 22 ms, p99
56 ms**. Each write is now a gRPC hop to the leader, a persist-and-fsync on the leader, a fan-out to
two followers that each persist and fsync before acking, and the client waiting on the faster of the
two. Reads pay it too, because I route `Get` through the log as well. What you buy for 33x: an
acknowledged write survives any single node's disk, and — via the election restriction — every future
leader is *guaranteed* to already contain it, which is why data only ever flows leader-to-follower and
a new leader never has to fetch anything.

Some of that 33x is the protocol and some is my implementation: the persister rewrites the entire log
and snapshot on every append, and there is no group commit on the Raft path. Being able to separate
"what consensus costs" from "what my persister costs" is, I think, the actual skill this number
teaches.

**Majorities are an intersection argument, and everything follows from it.** Any two majorities of
2f+1 share at least one node. So two candidates in one term cannot both win — the shared node voted
once. And a candidate that wins must have talked to at least one node holding every committed entry,
because "committed" means "on a majority". The election restriction turns "talked to" into "has a log
at least as up to date as", and log matching turns *that* into "contains it". Three sentences, and
they are the entire reason Raft is safe. Once I could reconstruct that chain from scratch, the
implementation stopped being a list of rules and became a thing with a shape.

**The bug everyone writes is committing an old-term entry by counting replicas.** An entry from term 2
reaches a majority under a term-4 leader, is applied, and then a term-5 leader that never saw it
overwrites that index. State machine safety, gone. The fix is one early return: when scanning for a
new commit index, stop at the first entry not of the current term. Old entries commit *indirectly*,
when a current-term entry after them commits. I wrote that line, understood why, then watched
`TestFigure8` fail 1000-iteration runs when I got an adjacent detail wrong. The other six classic bugs
— resetting the election timer on a *rejected* vote, truncating a follower's log on length rather than
first conflict, acting on stale replies from a previous term, holding the mutex across an RPC — are
catalogued with line numbers in `docs/phase2.md` §4. I wrote at least four of them.

The verification is what makes me believe any of it: eight clients hammering five unreliable nodes for
12 seconds while a nemesis partitions, heals, crashes and restarts them, every operation's call and
return time recorded, handed to Porcupine to search for a valid sequential witness. Zero violations.
The methodological detail I would have gotten wrong on my own: an operation that timed out must be
recorded as *pending* with its return time pushed past the end of the run, not as "failed". The client
does not know whether its write happened. Claiming it didn't would be wrong if it did; dropping it
would let the checker accept a history where a later read returns a value nobody wrote. Pending is the
only honest encoding, and it is what Jepsen does.

---

## Phase 3: more leaders, and a hash that lied

Replication makes existing throughput durable; it does not add more of it. Every write still funnels
through one leader's consensus round. The only way to get more is more leaders, each owning a disjoint
slice of the keyspace — which means a second thing to agree on: who owns which shard, right now.

So: a shard controller, itself a small Raft group, holding a history of configurations; and replica
groups each running their own Raft. The design decision I am most pleased with is that a group's log
carries **three kinds of entry** — client operations, configuration changes, and shard migrations —
all in one total order. "Did this write happen before or after the shard moved" needs one unambiguous
answer on every replica, and the only mechanism this system has for giving every replica the same
answer to an ordering question is putting both events in one log. A side channel for config changes
would let two replicas of the same group observe one client write on different sides of the ownership
change, which is a linearizability violation with exactly the shape of a stale read.

Migration is pull-based. When a group loses a shard it freezes a snapshot of that shard's data *and
its dedup sessions* into a map keyed by the config number that took it away — precisely the number the
new owner will ask for, so the two groups need no negotiation to agree which version is transferred.
The gaining group marks the shard "needed", refuses client traffic for it with a distinct
`ErrNotReady` (meaning "right group, retry here shortly", as opposed to `ErrWrongGroup`'s "re-resolve,
go elsewhere"), pulls it, and installs it. The proof that the source has let go is structural rather
than protocol: the frozen bytes exist at that config number only because the config change put them
there, at the same log index that made the group stop owning the shard.

That the dedup sessions travel *with the shard* rather than staying with the group is the subtle part,
and it is Phase 1's lesson arriving with a vengeance: a CAS applied once before a move must be
answered from the cached result after the move, not re-evaluated against post-move state.

Porcupine verified the whole thing under continuous rebalancing: **3191 operations across 3 groups
while a nemesis reassigned shards roughly every half second, zero violations.** And by hand, which I
found more convincing than the test: I started a second replica group on a *live* cluster, ran
`shardctl join`, and watched the controller rebalance about half the shards onto it while writes were
in flight. Every key kept resolving. No restart, no repointing a client, zero manual intervention.

### The hash that had perfect minimal disruption and terrible balance

This phase produced the bug I like most. I needed placement that every gateway computes identically
with no coordination, and that moves as few keys as possible when a worker joins or leaves. Rendezvous
hashing does both: score every worker by `hash(key, workerID)` and take the maximum; remove a worker
and only the keys it was *winning* move, each to its own runner-up.

The test failed with `moved fraction 0.500, want about 1/5`. What made it confusing is that the
*other* assertion in the same test passed exactly: every key that moved had been on the removed
worker, and no unrelated key was reshuffled. The minimal-disruption property — the entire reason
rendezvous hashing exists — was working perfectly. Removing one worker of five moved half the
keyspace, which can only mean that worker owned half the keyspace to begin with.

The cause was the obvious implementation: `fnv64a(key + separator + id)`. FNV-1a's loop is
`h = (h ^ byte) * prime`, so the hash *ends* with the last byte of the id. Two ids differing only in
their final character — `w0`..`w4`, `pod-1`..`pod-9`, which is how every real worker id in the world
looks — produce final hashes related by a fixed multiplicative step from a common intermediate state.
Which id scores highest then depends far more on the *id* than on the *key*.

The deeper point took a while to articulate and is why I still think about this bug: rendezvous
hashing's correctness argument is a statement about **n independent random draws**. Each worker wins
about 1/n, and removing one moves only its keys, *because the scores are independent*. A hash that is
weak with respect to structured input quietly violates that assumption, and nothing in the algorithm
notices. The fix is three changes, each load-bearing — hash each side separately so the id cannot ride
on the key's accumulator state, multiply the id's hash by a large odd constant to scramble structured
ids apart, and run the combination through a splitmix64 finalizer so one input bit flips half the
output bits. Distribution afterwards: **528 / 472 / 500 / 500**.

And the lesson I actually took: **a loose test bound would have passed this.** I had written the bound
as ±20% of uniform. A bound of "no worker gets more than 80%" — which sounds perfectly reasonable when
you are writing it — would have been satisfied by a function handing one worker 50%. When you are
testing a *statistical* property, the bound is not a formality you loosen until the test stops
flaking. The bound **is** the test.

Two more bugs came out of running real multi-process clusters rather than in-process tests: a
per-group leader cache that was being rebuilt on every call (a benchmark reporting 500 puts and 508
retries — a ratio too clean to be network noise, which is what made me look), and, once that was
fixed, concurrent benchmark workers sharing one client identity in violation of the store's own "one
outstanding request per client" contract. The second was invisible until the first was fixed, because
the retry storm had been acting as an accidental mutex. **Making the system fast surfaced a
correctness bug that slowness had been hiding**, which is now something I expect rather than find
surprising.

---

## Phase 4: leases are for liveness, fences are for safety

The scheduler is the phase whose ideas felt most portable. A job queue, stored entirely in the KV,
with workers claiming jobs under time-bounded leases.

The argument runs in three steps and I think it is the best three-step argument in distributed
systems. **A lock is not enough**: a worker that takes a lock and dies holds it forever, because
nobody can distinguish a dead holder from a slow one. Every lock service you have used bolts a timeout
on for exactly this reason — and the moment it has a timeout, it is not a lock, it is a lease. **A
lease alone is not safe**: the holder does not know it was paused. It was SIGSTOPped, or sat in a GC
pause, or was on the wrong side of a partition; the lease expired without its knowledge; it wakes up,
finishes the job, and writes its result over whatever the new holder did. Making the lease longer only
makes this rarer — no finite lease survives an unbounded pause. **The fencing token is what makes it
safe**: every time the job changes hands its generation number increments, and a worker may only write
while presenting the generation it was handed at claim time.

The part that took longest to internalize is *where the check lives*. It has to be at the resource
being protected, not at the holder, because the zombie is by definition the party that cannot be
trusted to know it is stale. Here the resource and the lease record are the same KV row, so the check
is a read-then-CAS — and the CAS is what makes the check *enforced* rather than merely performed. Even
if two schedulers race between checking and writing, only the one whose read reflects the latest
committed record wins; the loser re-reads and then the check correctly fails.

The second shaping decision: **the scheduler holds no state of its own.** Every job record, both
counters, every lease and the reaper's own leadership lease live in the KV, and every mutation is a
CAS. Two things fall out for free. N schedulers over the same KV *are* one queue. And a scheduler
crash at any instant loses nothing and duplicates nothing, because there was never state in the
process to lose — the interesting crash cases move to *sequences of KV writes interrupted halfway*,
which is a much smaller and more enumerable surface than "arbitrary in-memory state".

The decisive argument against the alternative — the scheduler as its own replicated state machine — is
about time. Such a design cannot read a clock inside `apply`, because `apply` must be a deterministic
function of the log and "now" is not in the log. It would have to make the leader *propose* time,
logging explicit "lease X expired" entries so every replica expires the same lease at the same index.
That is a leader-only clock authority and a second Raft group to operate. Instead I let lease expiry
be judged by whichever scheduler replica happens to look, and accepted that two replicas whose clocks
differ by *d* disagree about expiry by *d*. That is purely a liveness effect — an extra re-execution,
or extra waiting — and never a safety one, because safety comes from the generation number and the
generation number does not involve time. Designing so that this sentence is *true* is the point; a
design where correctness depends on clocks agreeing is wrong on every real machine.

And the disclaimer I think is the most important paragraph in the phase: **a job may execute more than
once.** A worker can run the job to the end, be declared dead a moment before it calls Complete, and
have the next holder run it again. Both executions happened. What is exactly-once is the *commit* — at
most one accepted result per job. If the side effect was "write the result into the job record",
exactly-once holds end to end. If it was "charge the customer's card", it does not, and no queue can
make it. The handler must be idempotent on the job id. There is no distributed queue that gives
exactly-once execution; anyone who says otherwise is describing at-least-once execution plus idempotent
effects.

The chaos test is the exit criterion: 2000 jobs, 8 workers, a fake clock advancing 5–25 ms per real
millisecond so that a worker's real sleep becomes an *expired lease*. Measured: **1334 pauses past the
lease, 1124 crashes, 1314 zombie writes fenced, and exactly 2000 accepted completions.** No job ever
committed twice. The design choice I would highlight is that the test asserts `fenced > 0` and fails
if it is zero — a run in which no zombie ever appeared did not exercise the mechanism under test, and
passing on it would be vacuous. Then the same queue over three real Raft replicas with the leader
disconnected mid-run, and then real OS processes: 120 jobs, one worker killed mid-run, one deliberately
heartbeat-suppressed. All 120 DONE, zero FAILED, the suppressed worker fenced exactly once — and 13 of
its results *accepted*, because it finished those within its lease. Being slow is not a crime. Being
slow *and stale* is.

---

## Phase 5: the result I did not want, and the one I am most glad I have

The serving layer is four stages in a fixed order, and the order is the design. **Rate limit first**,
because it is the cheapest rejection and must not be dodgeable by anything downstream. **Cache
second**, because a hit costs one KV read and no GPU. **Route third**, because it is the first stage
that costs a worker. **Hedge last**, because it is the only stage that *spends* rather than saves —
hedging a request you were going to refuse, or one the cache would have served, is pure waste, and
putting it last makes that impossible.

The rate limiter is one shared token bucket per tenant in the KV, with every gateway doing
read-refill-decrement-CAS. The design I rejected — give each gateway `Rate/N` — breaks in two silent
ways. It breaks when N changes: autoscale 4 → 6 and every tenant's effective limit shifts. And it
breaks under uneven client affinity: a tenant whose traffic lands on one replica exhausts "its" 1/N
while the other shares idle, and is refused at a fraction of its paid rate while the operator sees a
system nowhere near capacity. One shared bucket has neither problem and introduces exactly one race,
which the CAS settles. Refill is computed lazily by the reader as a pure function of (last stored
state, now) rather than by a background ticker, because **no process owns the bucket to run one in** —
a ticker would need a leader, a lease for that leader, and a story for when it stops ticking. And a
refusal writes nothing: a rate-limited tenant is by definition sending more than you want to serve,
and if every refusal cost a consensus round, the cheapest rejection in the system would become the
most expensive one under exactly the conditions that produce it.

I also noticed something I had not expected to be able to say cleanly. The rate limiter needs a fence
(the CAS) but not a lease, because a token is *spent*, not *held* — there is nothing to reclaim. The
worker registry needs a lease but not a fence, because a registration grants no exclusive permission
and a stale one can corrupt nothing. The job queue needed both. That is a property of each *problem*,
not of each implementation, and being able to derive which mechanism a new problem needs before
building it is the most transferable thing in this project.

The semantic cache splits along the same axis: the exact-match index lives in the shared KV, so a hit
on any replica is a hit on all, costing one hash and one read with no embedding at all; the
near-duplicate vectors are local per replica, because vectors are large and a similarity scan would
cost a linearizable read per candidate. The consequence, plainly: **exact hits are fleet knowledge;
near hits are per-replica knowledge** that accumulates as a replica serves traffic. I measured the
brute-force scan — 5000 vectors of dimension 512, about **1.44 ms** — and that measurement is the
entire argument for not building an ANN index, since a generation is a hundred times that. Measuring
the naive thing against the cost it avoids is cheaper than assuming you need the clever thing.

And I wrote down the caveat that is not a systems problem: a semantic cache can serve **a different
prompt's answer**. That is the definition of the feature, not a bug. Every threshold is a point on a
precision/recall curve, and with a *better* embedding model the failure mode gets more dangerous, not
less, because a good model will happily rate "refund policy for enterprise customers" and "refund
policy for trial customers" as very similar. The systems layer offers mechanisms — threshold, TTL,
per-tenant namespacing, confirm-by-exact. Which risk is acceptable is a product decision. Presenting a
similarity threshold as a correctness guarantee is the wrong answer.

### The benchmark that could not fail

My first end-to-end benchmark ran 4 system prompts across 4 workers with the default prefix-cache
size. Within seconds every worker had cached every prompt, the *baseline* already scored a 0.9 hit
rate, prefix routing had nowhere to go, and the script printed two nearly identical numbers and
declared success.

The fix is a design statement rather than a tweak: run the workers **cache-capacity-bound**, at 32
cache blocks (about 3.5 of the benchmark's ~600-character system prompts), with **12** prefixes over
**4** workers, so each worker holds only a few prefixes and locality has to do real work. That is also
the realistic regime — an attention KV cache is GPU-memory-bound and evicts constantly, so locality
only pays when each worker sees a *subset* of the prefixes. The same discipline one level down: the
comparison must run *concurrently*, because sequentially every worker is idle on arrival and
least-loaded degenerates into a deterministic tie-break that looks exactly like affinity.

**A benchmark that cannot distinguish the thing it measures is worse than no benchmark**, because it
produces a number you will quote. Before trusting any A/B, construct the regime where the mechanism
under test is the binding constraint and verify the control arm can genuinely fail there.

Relatedly: when I first ran that comparison properly, least-loaded routing sent **all 16 concurrent
requests to one worker**. A load balancer that cannot balance is not much of a baseline. The cause was
that routing read load from the worker registry's inflight counter, which is only as fresh as the
worker's last lease renewal — about once a second. Sixteen requests arriving within a few milliseconds
all read the same stale value, usually zeros, and the deterministic tie-break sent every one of them
to the same worker. The tie-break was not the bug; the tie was. The fix is for the gateway to count
its *own* outstanding streams locally and fold that in: **count locally what you can observe exactly,
and use the remote signal only for what you cannot.** A signal refreshed every second cannot steer a
decision made every few milliseconds — between refreshes it is a constant, and a constant plus a
deterministic tie-break is a stampede. And note what else that fix did: before it, *both* arms piled
onto one worker, so the two routing strategies produced identical placement and the comparison
measured nothing. A broken control arm will cheerfully report that your change did nothing, or
everything.

### Affinity balances prefixes, not load

With the benchmark finally able to fail, here is what it said. Four workers, 12 distinct ~600-char
system prompts, 240 requests, 16 concurrent, workers capped at 32 cache blocks.

> **Re-measured (2026-10-05).** This table came from one trial with confounds (an admission cap of 16 against workers that run 4 at once, workers reused across phases, one slow worker). Re-measured without them over 5 trials, closed and open loop, affinity costs about 9% of throughput rather than about 35%, bounded-load routing gives the best hit rate, and hedging remains the clear p99 win. See [BENCHMARKS.md](../BENCHMARKS.md#routing-re-measured-with-repeated-trials-2026-10-05).

> **Correction (2026-10-01).** The hedging run below hedged at 250 ms, under the median TTFT, so it hedged ~62% of requests: its p99 gain was mostly load-spreading. Re-measured with tail-only hedging (1 s delay, 23% of requests hedged): p99 4,232 → 2,076 ms, with the median getting *worse*. See [BENCHMARKS.md](../BENCHMARKS.md#correction-2026-10-01-the-original-hedging-result-was-mostly-load-spreading).

| run | routing | hedging | hit rate | mean prefill | TTFT p50 | TTFT p99 | req/s | per-worker |
|---|---|---|---|---|---|---|---|---|
| A | least-loaded | off | 0.20 | 167 ms | 247 ms | 967 ms | 20.4 | 61/59/61/59 |
| B | prefix | off | **0.60** | 140 ms | 651 ms | 1515 ms | 13.4 | 15/21/105/99 |
| C | prefix | on (250 ms) | **0.675** | **96 ms** | 357 ms | **958 ms** | 17.8 | 42/47/79/72 |

**A → B: the mechanism worked perfectly and the outcome got worse.** Prefix affinity tripled the cache
hit rate, cut mean prefill, and collapsed prefix spread from 3.92 distinct workers per prefix to 1.08
— essentially perfect stickiness. Every claim I had written about rendezvous hashing was confirmed. And
throughput fell from 20.4 to 13.4 req/s while p99 time-to-first-token rose from 967 to 1515 ms.

Not noise, not a bug. The per-worker column explains it entirely: hashing **12 prefixes onto 4
workers** gave two of them 105 and 99 requests against 15 and 21 for the other two — **about 85% of
traffic on half the fleet** — where the least-loaded baseline had been 61/59/61/59, almost perfectly
even.

> **Affinity balances prefixes, not load.**

Rendezvous hashing guarantees each worker wins about 1/n of the *keyspace*. It guarantees nothing
about the traffic volume behind those keys. Twelve prefixes over four workers is twelve draws from a
four-way multinomial with unequal per-prefix request counts on top; an even split is lucky, not
typical. The law of large numbers has not had a chance to operate. And cache locality and load balance
are in *direct* tension, because a perfectly sticky router is by construction one that cannot move work
off a hot worker.

**B → C: hedging recovered the tail, and more.** p99 fell to 958 ms — below run A's 967 — throughput
recovered to 17.8 req/s, p50 nearly halved. It works here for a precise reason: a request queued
behind a hundred others is slow for a *request-independent* reason, so a second attempt on a nearly
idle worker genuinely is faster. The surprise is that mean prefill also *improved*, 140 → 96 ms, and
the hit rate rose to 0.675. Naively a hedge should land somewhere cold. What actually happens is that
the hedge target is the **runner-up in the same rendezvous ranking**, not a random worker, so over 240
requests the runner-up for each hot prefix warms up too. Hedging accidentally built a replication
factor of 2 for hot prefixes.

I have thought about this result more than anything else in the project, and I am glad it came out this
way. Had I reported only the hit rate — the number the feature was built to move — I would have shipped
a throughput regression and called it a win. **Measure mechanism and outcome separately, and be ready
for them to disagree.**

Two more things this phase insisted on. Hedging is only safe because generation has no side effects —
hedging is retrying early, so everything that makes a retry safe applies unchanged, and the moment a
request writes something you need a dedup key or a fence. And **the loser must be cancelled**, which is
not bookkeeping: without it, every race leaves a worker generating a full response nobody will read,
for the full decode duration. A system that hedges 20% of requests and does not cancel losers has
bought its tail latency with 20% of its fleet. So cancellation is one unbroken context chain from the
client's stream, through the gateway's `defer cancel()`, into the outbound RPC, to a worker that checks
before every token and *during* prefill. The test asserts the worker stopped *early* — under 100 of 200
tokens — because a worker that "noticed" the cancel after finishing would pass a naive test.

---

## Phase 6: my test was wrong, twice, in opposite directions

The last build phase was verification: a seeded chaos harness running a real five-node Raft cluster, a
real scheduler and optionally a real gateway in one process, driven by a nemesis of partitions, pauses
and crashes, checked with Porcupine and an exactly-once check. Plus chaos against real Docker
containers, plus OpenTelemetry traces, Prometheus metrics and a k6 load test.

The roadmap asked for "FoundationDB/TigerBeetle style" deterministic simulation, and the most valuable
thing I did in this phase was refuse to quietly redefine "deterministic" downward to match what I had
built. Determinism here decreases in four tiers, and naming the tier every time is the point. The
**workload** is fully pinned — each client owns its own RNG seeded from the scenario seed plus its id,
so its sequence of decisions is a pure function of the seed and its iteration count. **Network faults**
are fully pinned: every drop, delay and reorder comes from the seeded RNG, proven by a test that draws
500 decisions from two identically-seeded networks and requires them identical. The nemesis's
**decision function** is pinned given fixed state, proven by holding the state still by hand. The **live
nemesis schedule** is best-effort: two runs of one seed always agree on the first event and diverge
later.

True deterministic simulation needs two structural changes I did not make: virtualizing every clock
read, and single-threading the system so "what happens next" is chosen by an event loop you control
rather than the Go scheduler. That is why FoundationDB and TigerBeetle build their core logic to run
under either real I/O or a simulator from day one — it is a design commitment, not an addition. Being
able to state exactly where my harness sits on that spectrum is worth more to me than being able to
claim the stronger thing.

### The violation that was my test asking an impossible question

Two seeds failed with `context deadline exceeded`. My first hypothesis — not enough settle time — was
wrong, and a fixed sleep did not help. My second was real but incomplete: I had given the Porcupine
check and the scheduler check one *shared* deadline, and Porcupine's search was eating most of it
before the scheduler check got a turn. That fix is still in the code — each phase gets a fresh deadline
created immediately before it runs. And the same seeds still failed, now deterministically, at 49
seconds.

So I stopped iterating on the test and reproduced it standalone with the simulation runner, and read
the printed event log line by line. It was sitting right there: the nemesis had partitioned **three of
five nodes**, with no heal ever drawn. Two nodes is not a majority of five. Raft was behaving *exactly
as specified* — a cluster with 2 of 5 votes sits unavailable forever, by design — and the "violation"
my test reported was my harness asking an impossible question.

The fix bounds the nemesis so that partitions, pauses and crashes are only eligible while enough nodes
remain up to leave a majority, with crashes and partitions sharing one budget, plus a heal-everything
step after the run regardless (standard Jepsen practice: heal before you check). Seed 2 went from 49.48
seconds and a false failure to **5.1 seconds and a pass**.

The lesson generalises past chaos testing: **a "violation" your own test reports might be your test
asking an impossible question.** The discipline that catches it is not "trust the test" and not "keep
adjusting timeouts" — it is reproduce it standalone, with the smallest tool that shows you the raw
sequence of events, and read what actually happened before forming a new hypothesis.

### And then the fix broke a test, correctly

With the majority bound in, a test asserting that two live runs of the same seed produce the same full
nemesis event sequence began failing — wildly, with different event counts per run.

The instinct is to assume the new fix introduced a regression. The actual cause is that the bound makes
the nemesis consult *live* cluster state, and live state's timing is not seeded — and **should not be**,
because pinning it would mean the safety check stops reflecting the cluster's real condition, which is
precisely the property that made the bound correct. The test's claim had only ever passed by accident,
before the majority bound existed to make the nemesis's choices depend on real timing at all.

So I deleted the claim and replaced it with two narrower ones that are actually true at the layer they
claim: the decision function *is* a pure function of fixed state and an RNG, and the *first* live
nemesis event *is* always reproducible. Then I rewrote the package documentation into the four precise
tiers above, so nobody (including me) re-adds the stronger, false claim later.

**When a fix that makes a system more correct breaks a test asserting something about the system's old,
less-correct behaviour, fix the test's claim — do not water down the fix.** Story one is a test that
was too permissive. Story two is a test that was too strict. Both are the harness being wrong about the
system, in opposite directions, and learning to tell which shape I am looking at is probably the most
practically useful thing this phase taught me.

### Real containers found things no in-process harness could

Docker chaos produced **14/14 checks passing** — dynamic leader detection by grepping container logs,
`docker network disconnect` and confirming the remaining majority still serves read-your-writes,
SIGKILL and restart against the same volume with data surviving, and `tc netem` injection measured at
**1.6 s → 7.9 s** round trip under 300 ms delay plus 5% loss. The injection runs from a throwaway
sidecar sharing the target's network namespace, so the capability never has to be granted to a service
container.

The two bugs this layer found were not distributed-systems bugs at all, which is exactly the point. One
was a Dockerfile named `Dockerfile.go`, which the Go toolchain dutifully tried to compile, breaking
`go build ./...` repo-wide on the `#` of its first line — a file extension is a contract with every tool
that scans by extension, not just the one you had in mind. The other was a multi-phase pip install
silently downgrading protobuf below the version the generated gRPC stubs were compiled against,
crash-looping every worker container at import time — any ordered dependency install lets a later phase
downgrade something a different part of the system depends on, and the defense is to re-assert the
load-bearing version explicitly afterward.

I also reported two things I *could not* do. Clock skew: all containers on one Docker host share a
kernel clock, and Docker does not put container PIDs in a private time namespace, so skewing one
container means skewing the host including the test script. The right home for that test is an injected
fake clock inside the Go harness, which I have not built. And a clean disk-pressure repro: a 3-node
throwaway cluster on tiny volumes did show real leader-flapping instability under a write hammer, but I
never confirmed a specific `ENOSPC` failure mode, so the script asserts only that the experiment ran end
to end and produced an inspectable result. Writing "not achieved" in a document is much cheaper than
writing something that is not true.

The k6 load test is a small case of the same discipline: 20 virtual users against **two** workers
produced 552 checks at 100% pass and a p95 latency threshold that **failed** at about 10.6 seconds.
That is the good outcome. A serving layer with no admission queue, pushed past capacity, should queue
and grow a tail *visibly and measurably*, and 552/552 checks confirm nothing was dropped or truncated —
it was just slow, which is the right failure mode. A threshold that passes no matter how hard you push
tells you nothing about where capacity ends.

---

## What I would do differently

**Build the observability first, not last.** Tracing and metrics arrived in Phase 6, and every phase
before it would have been faster to debug with them. The Phase 5 routing bug — a stale load signal
producing a stampede — would have been obvious in a per-worker request-distribution panel within
seconds.

**Write the "what this cannot distinguish" section of a benchmark before running it.** Both benchmark
failures — the one that could not fail, and the broken control arm — would have been caught by asking
"construct the case where this reports success incorrectly" *before* collecting numbers. It is the same
discipline as writing down what a test does not prove, applied earlier.

**Take the clock seriously from day one.** Phase 4 introduced an injectable clock and it made thousands
of lease expiries testable in seconds. Raft still reads the wall clock directly, which is the single
biggest obstacle to the deterministic simulation Phase 6 could not reach. Threading a clock interface
through from the start would have cost almost nothing then and bought a much stronger verification
story.

**Pick the harder persister early.** The Raft persister rewrites the entire log and snapshot on every
append, inflating the 33x consensus cost with an O(log size) write amplification that has nothing to do
with consensus. A WAL-backed persister — reusing the Phase 1 WAL, which was sitting right there — would
have made the phase's headline number a cleaner statement about the protocol.

**Be more careful about which correctness claims a component can make alone.** Both Phase 5 war stories
are cases where a component worked exactly as designed and the system was still wrong: the hash had
perfect minimal disruption and terrible balance; least-loaded routing read exactly the field it was
built to read. Component tests cannot catch that class of bug. Only an end-to-end measurement with an
honest control arm can.

## What I would build next

**ReadIndex reads**, first, because it is the highest-leverage unmade change. Every `Get` currently
costs a full consensus round and an fsync per node. ReadIndex removes both — record the commit index,
confirm leadership with one heartbeat round, wait for the apply to catch up, answer locally — and stays
linearizable. Then measure a read-heavy mix against the current numbers.

**Bounded-load consistent hashing**, because it is the principled answer to the result I am proudest of
finding. It caps every worker at (1+ε) times average load with deterministic overflow, which is affinity
*with a proof* about balance — the one thing my current spill threshold lacks. The interesting
experiment is the hit-rate-versus-p99 curve as ε sweeps from pure least-loaded to pure affinity, and
finding where run B's 0.60 hit rate is reachable at run A's 967 ms p99.

**Token-level rate limiting with reserve/settle**, because counting requests is wrong when a 10-token
and a 10,000-token request are both "one". Reserve an estimate at admission, settle the difference at
end of stream — and then handle the three things that make it a distributed problem rather than
arithmetic: a gateway dying between the two (the reservation needs a lease and a reaper, which is Phase
4's pattern applied to tokens), a retried settle (idempotent on request id, so the reservation record
is the dedup point), and actual exceeding estimate.

**A real embedder behind the existing interface**, which is a one-line swap by construction, followed by
an honest precision/recall sweep on a labelled set including near-misses that must *not* hit. I expect
recall to improve a lot and precision to become harder to control, and I would like to measure that
rather than assert it.

**And virtual time**, eventually — the thing that would turn the chaos harness from seeded fuzzing into
real deterministic simulation. It is the largest of these by a wide margin, because it means
restructuring how Raft and the simulated network dispatch concurrent work so a single event loop can
step them. But it is also the one that would change what the whole project can claim.

---

## The one-paragraph version

I built a distributed LLM serving platform from a write-ahead log up: Raft from the paper, a
linearizable replicated KV, a sharded control plane with live migration verified by Porcupine across
3191 operations, a scheduler that accepted exactly 2000 completions for 2000 jobs while fencing 1314
zombie writes, and a serving gateway whose prefix-aware routing tripled the cache hit rate and made
throughput *worse* — because affinity balances prefixes, not load — until hedging to the runner-up in
the same ranking recovered the tail below baseline. The inference backend is a mock with a documented
latency model, it all ran on one 8 GB laptop, and it is not production software. What I have is a system
I can explain at every layer, a set of measurements I trust because I know what each one cannot
distinguish, and a handful of bugs that only showed up because the bounds were tight, the control arm
worked, and I reproduced the failure standalone instead of adjusting the timeout.
