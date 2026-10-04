# Phase 5 — The LLM serving layer: rate limiting, caching, locality, tails

Code: `kvapi/kvapi.go`, `ratelimit/`, `router/`, `semcache/` (`semcache.go`, `embed.go`, `index.go`),
`gateway/` (`api.go`, `server.go`), `infer/mock/`, `cmd/gateway`, `cmd/llmbench`,
`py/infer_worker.py`, `py/gateway_client.py`, `proto/infer/v1`, `proto/gateway/v1`,
`scripts/e2e_llm.sh`.
Run: `make test-llm` (`Makefile:55-56`), `make e2e-llm` (real processes, `Makefile:61-62`),
`go test -bench LookupNear ./semcache/`.

Build note. Three files decide whether this is correct and were written by hand: `ratelimit.Take`'s
CAS loop (`ratelimit/ratelimit.go:150-200`), `router.score` (`router/router.go:283-285` — four lines,
and wrong the first time, §9.1), and `gateway.streamFromWorkers` (`gateway/server.go:332-503`), which
owns every goroutine and every context in the request path. The protos, generated stubs, Python
worker, benchmark tool and e2e script are plumbing around those three and were reviewed against them.

## 1. What Phase 5 builds and why

The roadmap's Phase 5 (`ROADMAP.md:105-113`) asks for a CAS-based distributed token bucket per
tenant, a semantic cache with metadata in the KV and vectors local, prefix-hash routing so shared
prefixes land on one worker, streaming with cancellation propagation, and hedged requests — exit
criterion "a benchmark showing p99 latency with vs without prefix routing and hedging" (`:112`).

Phases 1-4 built a store and a scheduler; this phase builds the thing in front of them. Every piece
of shared state it needs — rate-limit buckets, the worker registry, the cache's exact-match table —
lives in the Phase 1-3 KV, so **every gateway replica is stateless** (`gateway/api.go:8`,
`cmd/gateway/main.go:4-8`). Same decision Phase 4 made for the scheduler (`sched/api.go:1-6`), buying
the same two things: N gateways behind one address *are* one gateway, and a gateway crash loses
nothing. The interface that makes it possible is deliberately tiny — `kvapi.KV` is `Get`, `Put`,
`CAS` (`kvapi/kvapi.go:17-21`) and demands only linearizability, and `sched.KV` is now an alias of it
(`sched/api.go:68-70`), so the scheduler, limiter, registry and cache program against one interface
and each runs unchanged over the in-memory test fake, the single-node store, the Raft-replicated
store, or the sharded cluster (`kvapi/kvapi.go:1-6`).

```
 client (py/gateway_client.py, cmd/llmbench) ─Generate(prompt)─▶ stream of Token
   ┌─────────────────────────────────────────────────────────┐
   │ gateway replica (any of N, stateless)  gateway/server.go │
   │  1. rate limit  Limiter.Take(tenant,1)        :247-259   │
   │  2. cache       Cache.Lookup(prompt)          :261-271   │
   │  3. route       Registry.Live + router.Pick   :273-292   │
   │  4. stream+hedge streamFromWorkers            :294-295   │
   └────┬───────────────────────────────┬────────────────────┘
        │ Get / Put / CAS               │ Generate(stream), cancellable
        ▼                               ▼
 ┌──────────────────────┐   ┌──────────────────────────────────┐
 │ KV store  Phases 1-3 │   │ inference workers                │
 │  ratelimit/<tenant>  │   │  py/infer_worker.py (4x in e2e)  │
 │  workers/index       │◄──┤  infer/mock         (Go, tests)  │
 │  semcache/exact/<sha>│   │  prefix (attention KV) cache,    │
 └──────────────────────┘   │  prefill, decode, per-token      │
   registry renewed by each │  cancellation checks             │
   worker's lease loop      └──────────────────────────────────┘
   (py/infer_worker.py:288-325)
```

**What this is.** A serving *layer*: admission control, caching, placement, tail management and
cancellation, measured end to end against real OS processes over real gRPC. The worker is GPU-free
and its latency model is written down explicitly (`infer/mock/mock.go:8-19`): prompt split into
64-char blocks, a cache of cumulative block-prefix hashes, `prefillMs = BasePrefillMs +
PrefillPerChar * (len(prompt) - cachedChars)`, then one token every `TokenMs`. `py/infer_worker.py`
implements the same model to the character (`:21-32`, `:67-69`) so a benchmark can mix Go and Python
workers behind one gateway.

**What this is not.** A model server. No real model on the measured path (the `hf` backend at
`py/infer_worker.py:196-242` is explicitly untested), no continuous batching, no paged attention —
§12. The claim is narrower and defensible: *given a worker whose prefill cost falls when it has
already seen your prefix*, here is what routing, hedging and cancellation do to hit rate, throughput
and the tail, with numbers (§10).

## 2. Why the stages are in that order

Argued in `gateway/api.go:16-40`, implemented top to bottom in `Generate`
(`gateway/server.go:230-296`). Each stage is cheaper than the one after it, and each sits where it
cannot be dodged.

**1. Rate limit** (`:247-259`). First, because it is the cheapest rejection and the one that **must
not be dodgeable by anything downstream** (`api.go:19-22`): a tenant over quota should not get a cache
lookup, should not touch the registry, and should certainly not reach a worker. Refusal is
`ResourceExhausted` carrying `retry_after_ms=` (`:256-257`), which `llmbench` turns into a doubling
backoff (`cmd/llmbench/main.go:226-239`) — the shed-early-with-a-hint contract of Phase 4's
`ErrQueueFull` (`docs/phase4.md` §6).

**2. Cache** (`:261-271`). Second, because a hit costs one KV read and **no GPU** (`api.go:24-26`). A
hit streams back as a normal token stream (`streamCached`, `:301-309`) with `cached=true`, so a client
cannot tell the difference except by the flag and the speed. One rule is enforced: **a cache failure
must never fail the request** (`:266-270`) — on a Lookup error we fall through to the model, because
caches are an optimisation, not a dependency (`TestCacheFailureDoesNotFailRequest`,
`gateway/gateway_test.go:296`).

**3. Route** (`:273-292`). Third, because it is the first stage that costs a worker. It reads the live
set, folds in this gateway's own outstanding streams (`withLocalLoad`, `:178-192` — war story 2), and
picks `router.Pick` on the prompt prefix or `PickLeastLoaded` (`:285-289`).

**4. Hedge** (`:294-295`, `streamFromWorkers` `:332-503`). Last, because it is the only stage that
*spends* rather than saves: it buys tail latency with duplicate GPU work. Hedging a request you were
going to refuse, or one the cache would have served, is pure waste — putting it last makes that
impossible.

## 3. Distributed rate limiting

A token bucket admits bursts up to `Burst` and sustains `Rate` tokens per second after that
(`ratelimit/ratelimit.go:5-11`) — the standard shape for an API quota because it forgives a client
that was idle and then sends twenty requests at once, without letting a sustained flood through. The
stored record is 16 bytes: tokens as float64 bits, then `lastRefill` as unix ms (`:113-125`).

**One shared bucket, not N divided buckets.** The naive design gives each gateway its own bucket and
divides by the gateway count; `ratelimit.go:15-19` says what breaks. *It breaks when the gateway count
changes*: autoscale 4 → 6 and every tenant's effective limit silently changes; roll a deploy and old
and new replicas both hold a share for a few seconds, so the fleet admits more than the limit. *It
breaks under uneven client affinity*: a tenant whose traffic lands on one replica (sticky LB, one
long-lived connection, one region) exhausts "its" 1/N while the other N-1 shares idle — refused at
1/N of its paid rate while the operator sees a system nowhere near its limit. One shared bucket has
neither problem and introduces exactly one: two gateways may read at the same instant and both try to
take the last token.

**CAS is the enforcement point.** `Take` (`:150-200`) reads, refills, decrements, and CASes against
the raw bytes it read (`:191`). The comment states the invariant (`:188-190`): if another gateway
consumed tokens between our Get and now, `expected` no longer matches, we lose the CAS and re-read — at
which point there is no token left. Two gateways can never both take the same token. The loop is
bounded by `MaxAttempts` (32) and gives up with `ErrContended` (`:71-74`), which means "many gateways
are hammering one tenant's bucket," not "the limit was exceeded." Same structure as every CAS loop in
Phase 4 (`docs/phase4.md` §4.3): compare against exactly the bytes read, treat a lost CAS as a normal
outcome, re-read and re-decide.

**Lazy refill is a pure function.** `refill` (`:137-145`) computes the bucket at `nowMs` from its last
stored state and nothing else: `tokens = min(Burst, tokens + elapsed*Rate)`. No background ticker,
because **no process owns the bucket to run one in** (`:24-27`) — a ticker would need a leader, a lease
for that leader, and a story for when it stops ticking. Lazy refill needs none of that and is exactly
correct: the bucket's value at time *t* is a pure function of its last stored state and *t*.
`TestBurstThenRefill` (`ratelimit_test.go:60`) checks the whole shape: 5 admitted from a burst of 5, a
refusal whose hint is exactly 100 ms (one token at 10/s), exactly 3 more after 300 ms, and a cap at
`Burst` rather than `Rate*3600` after an hour.

**Refuse without writing** (`:179-185`). When `tokens < n`, `Take` returns having written nothing:
nothing changed that others need to see, and skipping the write keeps a *refused* tenant from
generating KV traffic. The second reason matters more than it looks — a rate-limited tenant is by
definition sending more than you want to serve, and if every refusal cost a Raft round, the cheapest
rejection in the system would become the most expensive one under exactly the conditions that produce
it. The hint is exact given the deficit and the rate (`:183-184`), and a request larger than `Burst`
is refused up front with a finite hint rather than looping (`:152-155`).

**Clocks.** "now" is the gateway's clock (`:29-36`). Skewed clocks disagree about how much has
refilled, so the effective *rate* is fuzzy by the skew. What skew can never do is let a tenant take
more than `Burst` tokens in one instant, because that bound is enforced by the CAS'd value, not by
time: whatever the clock says, `b.tokens` is a number that one writer at a time decrements. This is
Phase 4's rule one layer up (`sched/api.go:102-105`): **clocks affect liveness and precision; the CAS
guards the hard limit.** Note the symmetry with the lease/fence split — a rate limiter needs only *one*
of Phase 4's mechanisms. It needs the fence (the CAS on the stored value) because two writers must not
spend the same token. It does not need a lease, because nothing here is *held*: no holder can go
silent, no work is in flight to reclaim, no stale participant can write over a live one. **Leases
exist to reclaim things; a token is spent, not held.**

**The proof.** `TestConcurrentGatewaysShareOneBudget` (`ratelimit_test.go:129`, rationale `:125-128`):
sixteen goroutines, **each constructing its own `Limiter`** (`:142`) as separate processes would, 50
attempts each, one shared `memKV`, clock frozen. The assertion is exact — `admitted == burst`. Not
`burst+1`. Not `burst` per gateway. Then the clock advances 200 ms at 100 tokens/s and the same 800
attempts admit **exactly 20** (`:160-177`). Exactness is the point: a bound like "`<= burst + 5`" would
pass with a race in it, and a shared token bucket either is one bucket or it is not.
`TestTwoGatewaysShareRegistryAndBudget` (`gateway_test.go:457`) carries it up to the gateway: two
`Server`s over one KV, six alternating requests, burst 3, `admitted == 3`.

## 4. Service discovery by lease

Workers register with a lease and renew before it expires (`router/router.go:3-13`). `Register`
(`:119-157`) upserts into one gob-encoded index under `workers/index`, prunes lapsed entries in
passing (`:139-141`), and CASes. `Live` (`:196-219`) reads through a short local cache (`CacheTTL`,
default 200 ms, `:64-68`) and filters by lease (`:221-229`). `py/infer_worker.py`'s `register_loop`
renews every `lease_ms/3` (`:288-325`), falling through to the next gateway address on failure — any
replica will do, they all write the same registry (`:291`).

**Why no failure detector is needed** (`router.go:7-9`): a worker that crashes stops renewing and drops
out of routing when its lease lapses. No gossip, no health-check sweeper, no phi-accrual estimator. The
Python worker says it from the other side (`py/infer_worker.py:7-13`): "the absence of heartbeats IS
the signal, and it is the only one that survives a `kill -9`." `Deregister` (`router.go:160-186`)
exists for graceful shutdown but is an optimisation — removal is instant instead of one lease later —
and nothing depends on it arriving.

This is Phase 4's lease reused for a different purpose, and the difference matters. There, a lease
granted *exclusive* permission to run a job, and safety needed a fence because a paused holder could
wake and write. Here a registration is not exclusive and grants no permission: it is a *liveness
advertisement*. The worst case for a stale one is a request routed to a dead worker, which the gateway
handles by falling back (`server.go:411-436`; `TestFailoverToAnotherWorker` `gateway_test.go:522` kills
a worker's server while leaving its registration in place and requires all 6 requests to survive). No
fence, because there is nothing to corrupt. `TestRegistryLeaseExpiry` (`router_test.go:53`) drives the
lifecycle on a fake clock — one worker renews, one does not, the lapsed one disappears, a *second*
`Registry` over the same KV sees the identical set — and `TestConcurrentRegistrationsAllLand` (`:94`)
requires all 20 concurrent registrations to land ("a CAS race lost a registration", `:112`).

**The single-key hot spot, and when it stops being fine.** The whole registry is one key, so every
renewal is a read-modify-CAS of the entire worker list and concurrent renewals lose CASes and retry
(`router.go:10-13`). At tens of workers renewing every second that is nothing. It stops being fine on
two axes. *Write contention*: renewals are `O(W)` CASes per lease period against one key and the
probability of a lost CAS grows with W, so past a few hundred workers the retry loop does real work and
`ErrContended` becomes reachable. *Value size*: the gob-encoded list is `O(W)` bytes read by every
gateway every `CacheTTL` and rewritten by every renewal, through Raft. The fix is per-worker keys plus
a listing primitive — which the KV does not have, and which is the real content of the exercise, since
"list keys under a prefix" in a sharded store means fanning out across groups.

## 5. Prefix-aware routing

**What a paged attention KV cache actually caches.** An inference server holds the attention key/value
tensors for tokens it has already processed. In a paged implementation (vLLM, SGLang) those live in
fixed-size **blocks**, each identified by the hash of the token sequence *up to and including* it — so
two prompts sharing a leading run of blocks share the blocks themselves, and prefill for the shared
part is skipped. `infer/mock` models exactly that (`mock.go:8-19`): `blockHashes` computes the
cumulative hash of blocks `0..i` for each **full** 64-char block (`:169-178`), `lookupPrefix` counts
the longest leading run already resident (`:180-194`), and prefill is charged only on
`len(prompt) - cachedChars` (`:252`). A trailing partial block is deliberately not cached (`:162-168`)
— real paged caches work in whole blocks, and counting a partial one would let a one-character
difference at the end of a prompt claim a full block of reuse; `py/infer_worker.py:101-135` makes the
identical choice, which is why the two workers report the same `cached_prefix_chars`. The consequence
that drives this section: **decode is unaffected; prefill is roughly linear in the number of NEW
tokens.** A 600-character system prompt shared across a class of requests costs one full prefill per
worker that sees it and nearly nothing after — *if* those requests reach the same worker.

**Placement must be stable with no coordination** (`router.go:15-31`). Two properties are needed. (1)
Every gateway replica computes the same answer for the same prefix and live set, with no coordination
and no shared placement table — anything else means two gateways sending one prefix to two workers and
halving the hit rate. (2) When a worker joins or leaves, as few prefixes as possible move, because a
moved prefix is a cold prefill on its new worker. `hash(prefix) mod n` has (1) and catastrophically
fails (2): going from 5 workers to 4, a key keeps its owner only when `h%5 == h%4` — roughly a quarter
of keys, so **~75% of the keyspace reshuffles** because one worker died, every one of them now cold.

**Rendezvous (highest-random-weight) hashing** has both: score every worker by `hash(prefix, workerID)`
and take the maximum (`Ranked`, `:289-300`). Remove a worker and only the prefixes it was *winning*
move, each to its own runner-up, because the remaining workers' scores did not change; add one and it
steals only where it now scores highest, about `1/n`. `TestRendezvousMinimalDisruption`
(`router_test.go:149`) checks both halves separately, and the separation matters: `moved ==
movedFromRemoved` (`:164-166`) is *correctness* (no unrelated prefix reshuffled) and
`0.15 <= frac <= 0.25` (`:167-170`) is *balance*. Measured: **19.4% of 3000 prefixes moved when
removing 1 of 5, all from the removed worker** — against ~75% for hash-mod-n.

**Load-aware spill: affinity is a preference, capacity is a rule** (`:33-35`). Pure affinity lets one
hot prefix pin one worker into the ground, so `Pick` (`:302-326`) walks workers **in score order** and
takes the first below `maxInflight`; if all are at capacity it returns the best-scoring one anyway,
because queueing at the affine worker beats losing the cache. `exclude` skips an id, which is how a
hedge target is chosen different from the primary (`:305-306`); `TestPickIsLoadAwareAndCanExclude`
(`router_test.go:174`) pins all four behaviours in sequence. `PickLeastLoaded` (`:328-345`) is the
deliberate baseline: ignore the prefix, take the lowest `Inflight`, ties by id so it stays
deterministic. **The contrast between the two functions is the experiment** — `PrefixRouting` chooses
between them at one call site (`server.go:285-289`) and the e2e toggles it between runs A and B
(`e2e_llm.sh:91`, `:109`).

## 6. Hedging

**Why it is safe here, and what makes it safe in general.** Hedging launches a second attempt for a
request already in flight, which is only ever safe when the operation is idempotent. The gateway's doc
says it directly (`api.go:35-40`): **generation has no side effects.** Two workers generating the same
prompt produce two streams; one is forwarded, the other dropped, and nothing changed either way. The
lineage: Phase 1's retried `Put` needed `RequestMeta` so the store could recognise a duplicate; Phase
4's retried `Complete` needed a **fence** so a zombie's write could be refused at the storage; Phase 5
needs neither, because there is no state to double-apply — the "fence" is degenerate, the loser is
cancelled and its tokens never forwarded. The moment generation *does* have a side effect (a tool call,
a billing event, a conversation-log write), hedging stops being free and needs one of the two. That is
the general form worth saying out loud: **hedging is retrying early, so everything that makes a retry
safe applies unchanged.**

**The race.** `streamFromWorkers` (`server.go:332-503`). The primary starts immediately (`:371`); if
`HedgeAfter` is set and more than one worker is live, a timer arms (`:374-379`). The loop at `:387-440`
waits for the first attempt to produce a **first token** — not to finish. On the timer firing
(`:393-409`) a second attempt launches on a different worker and `hedgesLaunched` increments; the first
`attemptFirst` carrying a token wins (`:411-439`). Triggering on time-to-first-token is right for this
workload: TTFT is dominated by prefill and worker-side queueing, exactly the two things a different
worker might not be suffering from, while decode rate once started is a property of the model and
hardware and will not improve by moving.

**The loser MUST be cancelled** (`:442-447`, "their tokens are waste"). This is not bookkeeping — it is
the difference between hedging costing a little and hedging doubling your fleet's load. Without it
every race leaves a worker generating a full response nobody will read, for the full decode duration,
on a GPU: a system that hedges 20% of requests and does not cancel losers has bought its tail latency
with ~20% of its capacity. `TestHedgingRescuesAStalledWorker` (`gateway_test.go:379`) is explicit —
`w0` stalls 400 ms every request, `w1` is fast, hedge after 60 ms; it asserts the hedge launched, `w1`
won, the request finished under 300 ms, then polls `w0`'s `Cancelled` counter for two seconds
(`:410-419`), failing with "the losing attempt was never cancelled: it is still burning compute."

**The cost, and when not to hedge.** Dean and Barroso's *The Tail at Scale* (CACM 2013) is the framing:
where a request fans out, the p99 of the whole is driven by the p99 of the parts, and the cheapest fix
is not making every part fast but issuing a second request after a brief delay. Their number: hedging
after the **95th percentile** took a BigTable-backed service's p99 from **1800 ms to 74 ms while adding
~2% more requests**, because a delay at p95 duplicates only ~5% of traffic. That is the whole cost
model — hedging at delay *d* costs roughly `P(latency > d)` extra work. So: do not hedge when the
system is saturated, because the extra work lands on the same overloaded fleet and is a positive
feedback loop (production gates hedging on a budget; this does not, §12); do not hedge below your p50,
or you duplicate most traffic to shave nothing; do not hedge non-idempotent work. **Do** hedge when the
tail is request-independent — a GC pause, a queueing spike, a noisy neighbour — because a different
worker genuinely is not suffering from it. The e2e manufactures exactly that: one of four Python
workers runs with `--stall-prob 0.15 --stall-ms 500` (`e2e_llm.sh:94`) and the gateway hedges at 250 ms.

## 7. Cancellation end to end

Not an optimisation — a correctness property of the design (`api.go:42-48`). Tokens produced after a
cancel are pure waste, and **with hedging in play the loser of every race would otherwise keep
generating.** The mechanism is one unbroken context chain:

1. **Client → gateway.** `stream.Context()` (`server.go:231`) is cancelled by gRPC when the client
   cancels or its deadline passes.
2. **Gateway internal.** `runCtx, cancelAll := context.WithCancel(ctx)` with `defer cancelAll()`
   (`:344-347`); every attempt's context derives from `runCtx` (`:360`). The comment states the
   guarantee: returning from this function **for any reason** — client cancel, error, clean completion
   — tears down every worker stream we started. There is no path out that leaks a stream.
3. **Per-attempt.** Each attempt also holds its own `cancel` (`:360-361`), which kills losers
   individually (`:442-447`) without ending the request.
4. **Gateway → worker.** `runAttempt` passes its context into `cli.Generate(ctx, ...)` (`:516`), so
   cancelling it cancels the gRPC stream on the wire.
5. **Worker.** The Go mock checks `ctx.Err()` before every token (`mock.go:272-277`, "tokens after a
   cancel are waste") and sleeps cancellably during prefill (`sleepCtx`, `:126-139`) — the common case
   for a hedge loser, which is usually still prefilling. The Python worker does the same with
   `context.is_active` (`py/infer_worker.py:258-260`), checked before every token (`:166-167`) and
   every 20 ms *inside* prefill (`_sleep_unless_cancelled`, `:182-191`).

Two details easy to get wrong and handled: `runAttempt`'s pump selects on `ctx.Done()` when writing to
the attempt channel (`server.go:549-553`), so a cancelled attempt with a full buffer exits instead of
blocking forever; and a `Recv` error that is `context.Canceled` or gRPC `Canceled` is reported as a
clean end, not a failure (`:538-541`), so cancelling a loser does not inflate `workerErrors`.

`TestClientCancelPropagatesToWorker` (`gateway_test.go:423`) asks for 200 tokens at 30 ms, reads 3,
cancels, then requires the worker's `Cancelled` counter to rise **and** `TokensEmitted` to stay under
100 (`:441-453`) — a worker that "observed" the cancel after finishing would pass a naive test. In the
e2e the same check runs against the real Python worker over real gRPC (`e2e_llm.sh:162-179`). Measured:
**the worker stopped after 3 tokens.** `py/gateway_client.py:8-13` records the lesson from the client
side: *stop reading and walk away is not enough* — an unconsumed stream holds the worker until its send
buffer fills and then blocks it there. You must call `cancel()`.

## 8. The semantic cache

**The split, and what it means for a fleet** (`semcache/semcache.go:7-31`):

```
exact path   semcache/exact/<sha256(normalised prompt)> -> entry   in the KV, SHARED
near path    vector -> that same exact key                          in memory, PER REPLICA
```

The exact path is what makes it **one cache rather than N**: the moment any replica `Store`s an answer,
every replica's next Lookup of that prompt is a hit, because they all read the same linearizable KV
(`:13-18`). A hit costs one hash and one Get — no embedding at all. Prompts are normalised first
(lowercase, whitespace collapsed, `embed.go:108-115`), so "Hello world" and "hello   world" are one
prompt; the key is the sha256 of that, keeping keys fixed-size and prompt text out of the key space
where it would end up in logs (`semcache.go:167-173`).

The near path is local, and the doc says why and what it costs (`:20-31`): vectors are large relative
to the record, similarity search is not a key lookup, and pushing a brute-force scan through a
Raft-replicated store would cost a linearizable read *per candidate*. The price, plainly: a replica can
only find near-duplicates of prompts **it has itself seen** — because it Stored them, or because it
served an exact hit and learned the vector then (`:266-272`). For a multi-gateway deployment: **exact
hits are fleet knowledge; near hits are per-replica knowledge.** A fresh replica has a fully working
shared cache and an empty near path that fills in as it serves traffic.
`TestTwoGatewaysShareExactNotNear` (`semcache_test.go:181`) pins both halves — B sees A's Store
immediately; a third replica C with an empty index misses the *variant* and has `LocalVectors == 0`;
after C serves one exact hit it has learned and its near path works.

**`Store` uses `Put`, not CAS** (`:33-43`), and that is considered rather than lazy. Every other
KV-backed component here uses read-then-CAS because two racing writers can corrupt a counter or
double-admit a job. A cache entry has no such invariant: if two replicas answer the same prompt at once
and both Store, either answer is valid. Last-writer-wins is the correct semantics; a CAS loop would add
round trips to protect nothing.

**The embedder is honest about what it is.** `NGramEmbedder` (`embed.go:20-42`) is **feature hashing,
not an embedding model**: normalise, hash character 2- and 3-grams plus word unigrams into a 512-bucket
vector with a hash-derived sign (Weinberger et al. 2009, so collisions cancel in expectation instead of
inflating similarity, `:38-40`), L2-normalise. The doc says what it cannot do (`:27-31`): it knows
nothing about meaning — "cheap flights to Paris" and "inexpensive airfare to France" score near zero,
while "do X" and "do not X" score near one. What it *has* is what a cache's tests need without
downloading a model: identical prompts give identical vectors across processes, it costs microseconds,
spelling variants score ~0.95 ("Summarize" vs "Summarise") and unrelated prompts ~0.30. Threshold 0.92
sits between those, and `TestNearHit` (`semcache_test.go:129`) **logs both similarities** so the choice
is auditable rather than asserted.

> **Update (2026-10-01): a threshold alone was not enough.** Those test prompts are short. With a
> long shared system prompt, two *different* questions score ~0.93, because n-gram cosine is dominated
> by the shared text, so the second question was served the first one's answer. Recording the
> real-model demo exposed it. A near hit is now a two-stage decision: the embedding proposes, and
> `nearDuplicate` disposes, accepting only if the stored prompt is within a bounded edit distance
> (5% of length, minimum 3). `TestNearHitRejectsDifferentQuestionSharingSystemPrompt` fails without
> the check. The general lesson: a similarity threshold tuned on short inputs says nothing about long
> inputs that share most of their text.

**Brute force, and its measured cost.** `BruteIndex.Nearest` (`index.go:101-120`) is a linear scan of
at most `MaxLocal` unit vectors, one dot product each, under an `RWMutex` so concurrent lookups scan in
parallel (`:38-41`). `BenchmarkLookupNear` (`semcache_test.go:385`) measures a full near-path Lookup —
embed, scan **5000 vectors of dimension 512**, confirm in the KV — at **~1.44 ms** on one laptop core;
`MaxLocal=10000` is ~3 ms (`semcache.go:45-54`). An LLM generation is hundreds to thousands of
milliseconds, so the scan is **noise on the path it protects**. That is the whole argument for not
building an ANN index, and a good instance of a general habit: measure the naive thing against the cost
it avoids before assuming you need the clever thing. `Index` is an interface (`index.go:5-27`) precisely
so HNSW or IVF can replace the default without touching `Cache`. The index maps vectors to **KV keys,
never text** (`:5-7`), so a near hit is always confirmed by a KV read (`semcache.go:280-299`) — not
optional, since the entry may have expired since indexing; if it is gone the dangling entry is dropped
and it is a miss (`TestNearHitDanglingKeyIsMiss` `:161`).

**The correctness caveat that is not a systems problem.** A semantic cache can serve **a different
prompt's answer**. That is not a bug — it is the definition of the feature. Every threshold is a point
on a precision/recall curve: lower it and you catch more paraphrases and occasionally answer the wrong
question; raise it and you cache almost nothing. With a real embedding model the failure mode gets
*more* dangerous, not less, because a good model will happily rate "refund policy for enterprise
customers" and "refund policy for trial customers" as very similar. The systems layer offers mechanisms
— a threshold, a TTL, per-tenant scoping of every cache key (the gateway prefixes keys with a hash
of the tenant), and an exact-match-only mode, which is now the default (near-duplicate hits need
`-cache-near`) — but **which risk is acceptable is a product decision**. (An earlier version of
this paragraph listed tenant namespacing and an exact-only mode before either existed. An outside
review caught it; both were then built.) Say that out loud in a design interview; the wrong
answer is presenting a similarity threshold as a correctness guarantee. Two smaller honest notes:
expiry is judged by whichever replica looks, so skew shifts expiry by the skew — a hit-rate concern,
never a correctness one (`:101-105`); and `Store` runs *after* the last token (`server.go:480-482`), so
its embedding cost lands on the tail of the response, not the head, and with a real embedder the right
shape is a synchronous Put plus background embedding — a change the split keeps local to `Store`.

## 9. Three things that went wrong

### 9.1 War story 1 — the skewed rendezvous hash

**Symptom.** `TestRendezvousMinimalDisruption` failed with `moved fraction 0.500, want about 1/5`.

**What was confusing.** The *other* assertion in the same test passed. `moved == movedFromRemoved` held
exactly: every prefix that moved had been on the removed worker, and no unrelated prefix was
reshuffled. The minimal-disruption property — the thing rendezvous hashing exists to provide — was
working perfectly. The problem was that removing one worker of five moved **half the keyspace**, which
can only mean that worker owned half the keyspace to begin with.

**Cause.** The obvious implementation, `fnv64a(prefix + separator + id)`. FNV-1a's loop is
`h = (h ^ byte) * prime`, so it **ends** with `h = (h ^ lastByte) * prime`. Two ids differing only in
their final byte — `w0`..`w4`, `pod-1`..`pod-9`, which is exactly how real worker ids look — produce
final hashes related by a fixed multiplicative step from a common intermediate state. "Which id scores
highest" then depends far more on the *id* than on the *prefix*, and one worker wins most of the
keyspace regardless of what is hashed against it. Measured: **one worker in five owned 50% of all
prefixes.** The deeper point is that **rendezvous hashing assumes the scores are independent random
variables** — its entire correctness argument ("each worker wins about 1/n, and removing one moves only
its keys to their runners-up") is a statement about n independent draws. A hash that is weak with
respect to structured input quietly violates that, and nothing in the algorithm notices.

**Fix** (`router/router.go:283-285`, reasoning preserved at `:270-282`):

```go
func score(prefix, id string) uint64 {
	return mix64(fnv64(prefix) ^ (fnv64(id) * 0x9E3779B97F4A7C15))
}
```

Three changes, each load-bearing: hash each side **separately** so the id cannot ride on the prefix's
accumulator state; multiply the id's hash by a large **odd** constant (golden ratio, invertible mod
2^64) to scramble structured ids apart; and run the combination through `mix64` (`:258-266`), the
murmur3/splitmix64 finalizer, which avalanches so flipping one input bit flips about half the output
bits. **After**: `TestRendezvousIsStableAndBalanced` (`router_test.go:124`), 2000 prefixes over 4
workers, **528 / 472 / 500 / 500**; `TestRendezvousMinimalDisruption`, **19.4% moved when removing 1 of
5, all from the removed worker**.

**Lesson.** Two, and the second is the more useful. First, *a hash that "looks random" is not the same
as a hash that is independent across the inputs you actually feed it* — the skew appeared only for ids
sharing a prefix and differing in the last character, which is every worker-id scheme in production and
no worker-id scheme in a toy example. Second: **a loose test bound would have passed.** The bounds are
`±20%` on purpose, with the reason written next to them (`router_test.go:136-137`): "A weak hash passes
a loose bound and still skews badly in production." A bound of "no worker gets more than 80%" would
have been satisfied by a function giving one worker 50%. When you are testing a *statistical* property,
the bound is not a formality you loosen until the test stops flaking — it **is** the test.

### 9.2 War story 2 — load-aware routing that was blind

**Symptom.** `TestPrefixRoutingGivesAffinityAndCacheHits` (`gateway_test.go:320`) fires 16 concurrent
requests sharing one long system prompt and compares prefix routing against least-loaded. Least-loaded
sent **all 16 to one worker**, failing "least-loaded routing did not spread under concurrency"
(`:369-371`). A load balancer that cannot balance is not much of a baseline.

**Cause.** Routing read load from one place: the registry's `Inflight` field (`router.go:56`), which is
only as fresh as the worker's last lease renewal — about once per second (`py/infer_worker.py:288-292`).
Sixteen requests arriving within a few milliseconds all read **the same stale value**, usually zeros,
and `PickLeastLoaded`'s deterministic tie-break by id (`router.go:337-339`) sent every one to the same
worker. The tie-break was not the bug; the tie was.

**Fix** (`gateway/server.go:53-63`, `:169-192`). The gateway counts its **own** outstanding streams per
worker in `localInflight`, incremented when an attempt starts and decremented when its goroutine exits
(`:362-366`); `withLocalLoad` folds that into `Inflight` before every routing decision (`:178-192`,
called at `:282`). The comment states the principle (`:55-61`): the registry's field "is far too stale
to steer routing... What a gateway knows exactly, and instantly, is its own outstanding requests" — and
generalises: "the same reason real load balancers count outstanding requests locally rather than
trusting a periodic health report."

**Lesson.** *A periodic health report and a per-request routing decision operate on different time
scales, and mixing them silently produces herds.* A signal refreshed every second cannot steer a
decision made every few milliseconds; between refreshes it is a constant, and a constant plus a
deterministic tie-break is a stampede. The rule: **count locally what you can observe exactly, and use
the remote signal only for what you cannot** — here, load from *other* gateways and the worker's own
internal queueing, which genuinely do need the registry. The local count is partial by construction (N
gateways each see 1/N), and that is fine: it is exactly how Nginx's least-connections, Envoy's
`LEAST_REQUEST` and gRPC's `least_request` work. **Note what else the fix did**: before it, both arms of
the benchmark piled onto one worker, so prefix routing and least-loaded produced the *same* placement
and the comparison measured nothing. Fixing the baseline is what made the contrast visible at all — a
broken control arm will cheerfully report that your change did nothing, or everything.

### 9.3 The benchmark that proved nothing

The first `scripts/e2e_llm.sh` ran 4 system prompts across 4 workers with the default prefix cache (512
blocks, `mock.go:106-108`). Within seconds **every worker had cached every prefix**, the baseline
already scored a 0.9 hit rate, prefix routing had nowhere to go, and the script printed two nearly
identical numbers and declared success. The fix is at the top of the script (`:17-22`) and is a design
statement, not a tweak: run the workers **cache-capacity-bound** at `--kv-cache-blocks 32` (~3.5 of the
benchmark's ~600-char system prompts) with **12** prefixes over **4** workers, so each worker holds only
a few prefixes and locality has to do something. The comment names the reason: "a real attention KV
cache is GPU-memory-bound and evicts constantly, so locality only pays when each worker sees a SUBSET
of the prefixes."

**Lesson: a benchmark that cannot distinguish the thing it measures is worse than no benchmark,**
because it produces a number you will quote. Before trusting any A/B, check that the control arm can
*fail*: construct the regime where the mechanism under test is the binding constraint and verify the
baseline is genuinely bad there. The same discipline appears one level down in
`TestPrefixRoutingGivesAffinityAndCacheHits`'s comment (`gateway_test.go:311-319`): the comparison
**must run concurrently**, because sequentially every worker is idle on arrival, least-loaded keeps
picking the same worker by tie-break, and it looks identical to affinity. It is exactly under concurrent
load — which is also when it matters in production — that the two differ.

## 10. The result, and the trade-off it exposed

Measured 2026-09-13. Three `raftkv` replicas, one `gateway`, four `py/infer_worker.py` processes, all
**separate OS processes** over real gRPC; workers capped at 32 prefix-cache blocks (~3.5 system
prompts); 240 requests, 16 concurrent, 12 distinct ~600-char system prompts.

> **Correction (2026-10-01).** The hedging run below hedged at 250 ms, under the median TTFT, so it hedged ~62% of requests: its p99 gain was mostly load-spreading. Re-measured with tail-only hedging (1 s delay, 23% of requests hedged): p99 4,232 → 2,076 ms, with the median getting *worse*. See [BENCHMARKS.md](../BENCHMARKS.md#correction-2026-10-01-the-original-hedging-result-was-mostly-load-spreading).

| run | routing | hedging | prefix-cache hit rate | mean prefill | TTFT p50 | TTFT p99 | req/s | prefix spread | per-worker |
|---|---|---|---|---|---|---|---|---|---|
| A | least-loaded | off | 0.20 | 167 ms | 247 ms | 967 ms | 20.4 | 3.92 | 61/59/61/59 |
| B | prefix | off | **0.60** | **140 ms** | 651 ms | 1515 ms | 13.4 | **1.08** | 15/21/105/99 |
| C | prefix | on (250 ms) | **0.675** | **96 ms** | 357 ms | **958 ms** | 17.8 | 1.83 | 42/47/79/72 |

**A → B: affinity works exactly as designed.** Hit rate triples (0.20 → 0.60), mean prefill falls
167 → 140 ms, prefix spread collapses 3.92 → 1.08 distinct workers per prefix — essentially perfect
stickiness. Every claim in §5 is confirmed.

**A → B: and throughput and tail latency got WORSE.** 20.4 → 13.4 req/s; p99 TTFT 967 → 1515 ms; p50
247 → 651 ms. Not a bug, not noise — the per-worker column explains it completely. Hashing **12
prefixes onto 4 workers** gave `py-2` 105 requests and `py-3` 99 against 15 and 21 for `py-0` and
`py-1`: **two workers took ~85% of the traffic** while two sat nearly idle. Run A's placement
(61/59/61/59) is almost perfectly even. The one-line lesson, and the most valuable sentence in this
phase:

> **Affinity balances prefixes, not load.**

Rendezvous hashing guarantees each *worker* wins about 1/n of the *keyspace*. It guarantees nothing
about the traffic behind those keys. With 12 prefixes and 4 workers you are drawing 12 samples from a
4-way multinomial — a 3/3/3/3 split is not typical, it is lucky — with unequal per-prefix request counts
on top. The law of large numbers has not had a chance to operate, and **cache locality and load balance
are in direct tension**: a perfectly sticky router is one that cannot move work off a hot worker.

**B → C: hedging recovers the tail, and more.** p99 TTFT 1515 → 958 ms (below run A's 967), 13.4 → 17.8
req/s, p50 651 → 357 ms. It works here for a precise reason: a request queued behind 100 others on
`py-2` is slow for a *request-independent* reason, so a second attempt on an idle `py-0` genuinely is
faster — §6's "do hedge when the tail is request-independent." The surprise is that **mean prefill also
improved, 140 → 96 ms**, and the hit rate *rose* to 0.675. Naively a hedge should land somewhere cold;
what actually happens is that the hedge target is `router.Pick(live, prefix, 0, primary.ID)`
(`server.go:400-402`) — the **runner-up in the same rendezvous ranking**, not a random worker — so over
240 requests the runner-up for each hot prefix warms up too and the fleet converges on ~2 workers
holding each hot prefix. Hedging accidentally built a replication factor of 2 for hot prefixes. The cost
is the spread rising 1.08 → 1.83: the same fact from the other side.

**The knobs.** `-max-inflight` (`cmd/gateway/main.go:59` → `MaxInflightPerWorker`, used at
`server.go:286`; the e2e ran at 16, `e2e_llm.sh:74`) makes `Pick` spill off a hot worker sooner, trading
hit rate back for balance continuously — cheapest thing to tune first. **More prefixes than workers**:
the skew is a small-sample effect, and at 1000 prefixes over 4 workers the multinomial concentrates;
real chat traffic has many system prompts and this benchmark's 12 is a deliberately hard case.
**Consistent hashing with virtual nodes** (Phase 3 exercise (b) applied here) fixes *keyspace* balance
but does nothing about one prefix being 10x more popular. **Bounded-load consistent hashing**
(Mirror/Google 2016) caps every worker at `(1+ε)` times average load with deterministic overflow — the
only one of the four that attacks the actual problem, affinity *with a proof* about balance, and
Exercise (b).

**Also measured in the same run:** semantic cache **49/50 (0.98)** on repeated identical prompts
(`e2e_llm.sh:122-127`, check `:156`); rate limiting **shed 93 requests** at `-rate 2 -burst 3` from 30
logical requests issued with retries by 6 clients (`:129-134`, check `:159` — each shed request is a
`ResourceExhausted` the client backs off and retries, `cmd/llmbench/main.go:216-240`, so the count
exceeds the request count by design); and **a cancelled client stream stopped the worker after 3
tokens** (`:162-179`).

## 11. How we know it works

**Unit tests** (`make test-llm`, `Makefile:55-56`, everything under `-race`):

| Test | What it proves |
|---|---|
| `ratelimit.TestBurstThenRefill` :60 | burst is exactly `Burst`; the hint is exact (100 ms for 1 token at 10/s); refill is exactly `elapsed*Rate`; an idle hour caps at `Burst`, not `Rate*3600` |
| `ratelimit.TestTenantsIsolatedAndOverridable` :95, `TestTakeLargerThanBurstIsRefusedWithHint` :117 | tenants are isolated and overridable; an unsatisfiable request is refused immediately with a finite hint instead of looping |
| `ratelimit.TestConcurrentGatewaysShareOneBudget` :129 | **the rate-limiting claim**: 16 independent `Limiter`s over one KV admit EXACTLY `Burst` with the clock frozen, then EXACTLY the refilled amount. Catches any per-replica budget and any CAS bug admitting `Burst+1` |
| `router.TestRegistryLeaseExpiry` :53, `TestConcurrentRegistrationsAllLand` :94 | a worker that stops renewing lapses; a second `Registry` sees the identical set; 20 concurrent registrations all land (catches a lost CAS) |
| `router.TestRendezvousIsStableAndBalanced` :124 | placement is deterministic and within ±20% of uniform. **Caught war story 1**; a loose bound would not have |
| `router.TestRendezvousMinimalDisruption` :149 | removing a worker moves ONLY its prefixes (correctness) and about 1/n of them (balance). Catches hash-mod-n and any skewed score |
| `router.TestPickIsLoadAwareAndCanExclude` :174 | idle → top-ranked; saturated → runner-up; `exclude` → hedge target; all saturated → still the affine worker, never nothing |
| `semcache.TestNearHit` :129, `TestNearHitDanglingKeyIsMiss` :161 | a variant hits (~0.95) and an unrelated prompt misses (~0.30), both similarities **logged** so 0.92 is auditable; an indexed key whose KV entry is gone is a miss and is dropped |
| `semcache.TestTwoGatewaysShareExactNotNear` :181 | exact is shared instantly; near is per-replica; a replica **learns** a vector from serving an exact hit |
| `semcache.TestTTLExpiry` :229, `TestMaxLocalEviction` :269, `BenchmarkLookupNear` :385 | expiry on both paths; the index stays bounded; brute force measured at **~1.44 ms per 5000 vectors, dim 512** — the number that justifies not building an ANN index |
| `gateway.TestRateLimitRejectsWithRetryHint` :231 | refusal maps to `ResourceExhausted` with `retry_after_ms=`; another tenant is unaffected |
| `gateway.TestCacheHitSkipsWorkerEntirely` :258, `TestCacheFailureDoesNotFailRequest` :296 | a hit does not reach the worker at all (asserted on the worker's own counter), `no_cache` bypasses, and a broken cache degrades to a normal generation |
| `gateway.TestPrefixRoutingGivesAffinityAndCacheHits` :320 | **the routing claim**, under concurrency (:311-319): 16 concurrent shared-prefix requests land on ONE worker with **16/16** prefix-cache hits, versus **4-5/16** spread over four with least-loaded |
| `gateway.TestHedgingRescuesAStalledWorker` :379 | a hedge launches, the fast worker wins, the stall is bypassed — **and the loser's `Cancelled` rises**, the expensive half |
| `gateway.TestClientCancelPropagatesToWorker` :423 | a cancel reaches the worker AND it stopped early (<100 of 200 tokens), not merely noticed afterwards |
| `gateway.TestTwoGatewaysShareRegistryAndBudget` :457 | two `Server`s over one KV route over one live set and admit exactly 3 against a shared burst of 3 |
| `gateway.TestFailoverToAnotherWorker` :522, `TestConcurrentLoad` :587 | a registered-but-dead worker does not sink requests (6/6 survive); 40 concurrent requests with stalls and hedging under `-race` neither deadlock nor leak |

**The e2e** (`scripts/e2e_llm.sh`, `make e2e-llm`): 3 `raftkv` + 1 `gateway` + 4 Python workers as
separate processes, the gateway restarted between runs with different flags (`start_stack` `:71-77`).
It **checks** rather than prints (`:136-160`), and each check earns its place: `B_HIT > A_HIT`
(`:140-142`) catches a routing function that does not produce affinity and a worker whose prefix cache
does not work; `A_PREFILL > B_PREFILL` (`:143-145`) catches a hit rate that rose without the hits being
worth anything (hitting on a trailing partial block); `B_SPREAD < 2.0` (`:147-149`) catches placement
that is *statistically* better but not *stable*, which a hit-rate check alone would miss;
`C_HEDGE > 0` is hard while the p99 B→C comparison is a `WARN` (`:151-154`), honestly, because whether
stalls fired is probabilistic and a flaky hard assertion on a random tail teaches you to ignore
failures; `D_CACHE > 0.8` (`:156`) and `E_LIMITED > 0` (`:159`) prove cache and limiter live over a real
Raft KV, not just the in-memory fake; and the cancellation check (`:162-179`) has a real Python client
cancel a real stream that a real worker logs.

Three layers, each catching what the previous cannot, as in Phase 4 §9: in-memory fakes with injected
clocks for exactness; in-process gRPC with real streaming for concurrency and cancellation; real OS
processes for what only appears under real latency — which is where both war stories came from.

## 12. What is NOT implemented, on purpose

- **No real model.** The measured path uses `infer/mock` and `--backend mock`. A scope limit, not a
  fudge, because the latency model is **written down** (`mock.go:8-19`) and matched
  character-for-character by the Python worker (`py/infer_worker.py:21-32`). The `hf` backend
  (`:196-242`) is real and untested here.
- **No continuous batching, no paged attention.** The **single biggest real-world throughput lever**
  and it is absent: vLLM-style continuous batching runs many sequences per forward pass and admits new
  ones at token boundaries, typically worth multiples of throughput — far more than anything here. It
  is deliberately **orthogonal to routing**: batching decides *which of the requests already at a
  worker* run next; routing decides *which worker a request goes to*. They compose, and they interact,
  which is the interesting part — a worker's effective capacity becomes a function of how batchable its
  queue is, so `MaxInflightPerWorker` as a scalar is the wrong shape for a real scheduler and would
  want to be "estimated queue time."
- **No token-level rate limiting.** The bucket counts **requests**: `Take(ctx, tenant, 1)`
  (`server.go:250`). Real LLM APIs limit on tokens, because a 10-token and a 10,000-token request are
  not the same load. What would change: `n` becomes a token estimate; `Rate`/`Burst` become
  tokens/second and a token burst; and the hard part, the true cost is unknown until generation ends,
  which needs Exercise (a)'s reserve/settle.
- **No admission queue.** Over quota the gateway **sheds** rather than queues — the right default
  (Phase 4's `sched/api.go:44-46`) but it means a burst that would drain in 200 ms is rejected instead
  of briefly held. Nor is there *concurrency* admission control: `MaxInflightPerWorker` steers placement
  but never refuses, and with everything saturated `Pick` returns the affine worker anyway
  (`router.go:322-324`).
- **No multi-turn conversation affinity.** Routing hashes the first 256 chars (`router.go:237-249`). A
  growing chat keeps the same leading system prompt so it keeps landing on the same worker by accident
  — but what you want cached is the *whole conversation so far*, which no two turns share as a 256-char
  prefix hash. Session stickiness is Exercise (c).
- **No cache invalidation or versioning by model.** The exact key is `sha256(normalised prompt)`
  (`semcache.go:167-173`) — no model name, temperature, sampling params or system-prompt version.
  Swapping the model serves the old model's answers until TTL. The fix is one line; the reason it is
  absent is that there is one model.
- **No hedge budget.** `HedgeAfter` is a fixed duration (`api.go:95-97`) with no cap on the fraction of
  requests that may hedge, so under fleet-wide slowness *every* request would hedge and double the load
  on an already-slow fleet. Exercise (e).
- **No TLS, authn or authz.** Every connection is `insecure.NewCredentials()` (`server.go:94`), and
  `tenant` is a client-supplied string (`proto/gateway/v1/gateway.proto:22`) — so **any client can
  claim any tenant's quota**. The limiter is resource protection, not a security boundary; making it
  one means authenticating the caller and deriving the tenant from the credential, never the body.
- **No cache GC.** Stale entries are overwritten, never deleted (`semcache.go:66-73`) — same shape as
  Phase 3's unfreed `outgoing` and Phase 4's DONE records, same reason it is left separable.

## 13. Exercises

**(a) Rate-limit on TOKENS, with reserve/settle.** Output length is unknown when you must decide to
admit. Sketch the two phases: at admission `reserve` `est = prompt_tokens + max_tokens` in one CAS and
write a reservation record keyed by request id; at end of stream `settle` `est - actual` back with a
second CAS. Then answer the three questions that make it a distributed problem rather than arithmetic.
*Gateway dies between reserve and settle?* The reservation carries a lease and is reclaimable — Phase
4's reaper applied to tokens; note the bucket is a bare counter, so the reclaimer needs the record to
know how much to return. *Settle retried?* It must be idempotent on request id, so the reservation
record, not the bucket, is the dedup point and the settle CAS goes through it. *`actual > est`?* You
already served it: choose between clamping `max_tokens` at admission (what real APIs do) and letting the
bucket go into debt. Validate with `TestConcurrentGatewaysShareOneBudget`'s shape: N gateways, exact
totals.

**(b) Bounded-load consistent hashing.** Implement Mirrokni–Thorup–Zadimoghaddam (2016) behind
`router.Pick`'s signature: a ring, and on placement walk forward from the prefix's position to the first
worker below `(1+ε) * average_load`. The theorem is the point — every worker is provably within `(1+ε)`
of average *and* key movement on join/leave is still bounded — which is exactly the guarantee §10's
trade-off lacks. Then **measure against run B**: same 240 requests, 16 concurrent, 12 prefixes, 4
workers, 32 cache blocks; report hit rate, p99 TTFT, req/s and per-worker counts, and sweep ε. The
interesting result is the hit-rate-versus-p99 curve as ε goes from 0 (pure least-loaded) to ∞ (pure
affinity): find where B's 0.60 hit rate is reachable at A's 967 ms p99.

**(c) Session affinity for multi-turn chat.** Add `conversation_id` to `GenerateRequest` and route on
`hash(conversation_id)` when present, falling back to the prompt prefix otherwise. Then confront the
cost: sessions are long-lived, so a hot conversation pins a worker for its lifetime, and
`MaxInflightPerWorker` spill now *breaks* the thing affinity was for (the new worker has none of the
conversation's KV cache). Decide whether spill is allowed at all and at what threshold. Then handle what
the registry makes unavoidable: a worker leaves mid-conversation and the next turn must re-prefill the
whole history elsewhere — measure that cost at turn 10 versus turn 1.

**(d) A real embedder behind the same interface.** Implement `Embedder` (`embed.go:11-18`) over a real
model (`all-MiniLM-L6-v2`, 384-dim, CPU) and swap it in at `cmd/gateway/main.go:101`. Nothing else
changes — that is the point of the interface. Then **re-measure precision and recall**: build a labelled
set of prompt pairs (true paraphrases; near-misses that must NOT hit, such as "enterprise" vs "trial"
refund policy; unrelated pairs), sweep `Threshold`, plot precision against recall, and compare with
`NGramEmbedder` on the same set. Expect recall to improve a lot and precision to become *harder* to
control, and say why. Then the second-order effect the package doc predicts (`semcache.go:56-64`):
embedding is now a model call on the critical path, so `Store` must go asynchronous and the near path
costs milliseconds it did not — re-run `BenchmarkLookupNear`.

**(e) Adaptive hedging.** Replace the fixed `HedgeAfter` with the *current* p95 of observed TTFT, kept
in a sliding window or t-digest per worker (or fleet-wide), plus a budget that refuses to hedge when
hedges in the last window exceed some fraction of requests. Cite the target from *The Tail at Scale*:
hedging at p95 took a Google service's p99 from **1800 ms to 74 ms for ~2% additional requests**.
Measure against run C (fixed 250 ms, 17.8 rps, 958 ms p99) and report the hedge *rate*, not just latency
— the fraction of requests duplicated is what makes hedging cheap.

**(f) Run the whole stack against the SHARDED KV.** `cmd/gateway` already takes `-ctrl` instead of `-kv`
(`cmd/gateway/main.go:50`, `:85-88`) and nothing above `kvapi.KV` changes. Start three `shardctrl` nodes
and two three-replica `shardkv` groups (Phase 3's manual test), point the gateway at the controller,
re-run the e2e. Then **explain which keys land in which shards** via `shard.Key2Shard`:
`ratelimit/<tenant>` — one key per tenant, so do `t0..t3` spread across groups, and what happens to a
single hot tenant? `workers/index` — **one key**, so the entire registry (every renewal from every
worker, every `Live` read from every gateway) sits on **one group's leader**: §4's hot spot made worse,
and the strongest argument for per-worker keys. `semcache/exact/<sha256>` — uniformly spread by
construction, the one part that shards beautifully. Predict which becomes the bottleneck before
measuring, then check.

## 14. Interview questions with model answers

1. **Design ChatGPT's serving layer.** Stateless gateways in front of a fleet of model workers, all
   shared state in a linearizable store. Per request, in order: per-tenant admission control (token
   bucket shared across gateways, enforced by CAS) because it is the cheapest rejection and must not be
   dodgeable; a cache lookup because a hit costs no GPU; placement preferring a worker that already
   holds this prompt's prefix in its attention KV cache, with load-aware spill so locality cannot pin
   one worker; then a streaming response with hedging for the tail. Workers discover themselves by
   renewing a lease in the same store, so a crashed worker leaves routing with no failure detector.
   Cancellation propagates end to end, because tokens after a cancel are wasted GPU and hedging makes
   that worse. Then name what you left out and why: continuous batching (the biggest throughput lever,
   orthogonal to all of the above), token-level limits, session affinity, a hedge budget.

2. **How do you rate-limit across 50 gateways?** One bucket per tenant in a linearizable store; every
   gateway does read-refill-decrement-**CAS**. The CAS is the enforcement point: two gateways taking the
   last token at the same instant means one loses, re-reads and finds nothing. Refill is lazy — a pure
   function of `(last stored state, now)` rather than a ticker, because no process owns the bucket and a
   ticker would need a leader and a lease. Refusals do not write, so the tenant you are actively
   refusing generates no store traffic. Clock skew makes the effective *rate* fuzzy but can never break
   the *burst* bound, which is enforced by the CAS'd value rather than by time. Explicitly reject
   per-gateway budgets divided by N: it breaks when N changes and starves a tenant whose traffic is
   sticky to one replica. At 50 gateways with a hot tenant, add a second tier — each gateway leases a
   small batch of tokens and spends it locally — trading exactness for round trips, and say so.

3. **What is prefix caching and how does routing interact with it?** An inference server keeps the
   attention key/value tensors for tokens it has processed, in fixed-size blocks identified by the hash
   of the token sequence up to and including each block. Two prompts sharing a leading run of blocks
   share those blocks, so prefill is charged only on the new suffix and a long shared system prompt is
   nearly free after the first request; decode is unaffected. That only helps if requests sharing a
   prefix reach the *same* worker, so routing must be a deterministic function of the prefix that every
   gateway computes identically with no coordination, and that moves few keys when the fleet changes.
   Rendezvous hashing gives both; `hash mod n` gives the first and reshuffles ~75% of keys when one of
   five workers leaves. And the thing most candidates miss: **affinity balances prefixes, not load** —
   see Q6.

4. **When does hedging help and when does it hurt?** It helps when the tail is *request-independent* — a
   GC pause, a queueing spike, a slow host — because a second attempt elsewhere genuinely does not
   suffer from it. It costs roughly `P(latency > delay)` extra work, so hedging at p95 costs ~5%; per
   *The Tail at Scale* that took a Google service's p99 from 1800 ms to 74 ms for ~2% extra requests. It
   hurts under saturation, where the duplicate work lands on the same overloaded fleet and forms a
   positive feedback loop — which is why production gates it on a budget. It hurts below p50, where you
   duplicate most traffic to shave nothing. And it is only *safe* when the operation is idempotent:
   hedging is retrying early, so everything that makes a retry safe applies. Generation has no side
   effects, so it is free here; the moment a request writes something you need a dedup key or a fence.
   Finally, **you must cancel the loser**, or the cost goes from `P(late)` extra prefill to `P(late)`
   extra *full generations*.

5. **How do you propagate cancellation through a streaming pipeline, and why does it matter?** One
   unbroken context chain: the client's cancel cancels the server stream's context; the gateway derives
   every internal goroutine's context from it with a `defer cancel()`, so returning for *any* reason
   tears down everything it started; that context is passed straight into the outbound RPC; the worker
   checks it before every token and in slices during prefill. It matters for three reasons. Money:
   tokens after a cancel are GPU time nobody reads, and users abandon streams constantly. Capacity: with
   hedging, every race produces a loser that would otherwise generate a whole response — the difference
   between hedging costing 5% and costing 100%. Liveness: an unconsumed stream blocks the worker's send
   path once its buffer fills, so "stop reading" is not cancellation. Test it by asserting the worker
   stopped *early* (token count), not merely that it eventually noticed.

6. **What is the trade-off between locality and load balance?** Direct tension: a perfectly sticky
   router cannot move work off a hot worker. Consistent or rendezvous hashing balances the **keyspace**
   and says nothing about the traffic behind the keys — with 12 prefixes over 4 workers I measured two
   workers taking ~85% of requests, which tripled the prefix-cache hit rate (0.20 → 0.60) while making
   throughput worse (20.4 → 13.4 rps) and p99 TTFT worse (967 → 1515 ms). Four ways out, increasing in
   quality: a load threshold that spills to the runner-up (cheap, continuous, what I did); more keys
   than workers, since the skew is a small-sample effect; virtual nodes (fixes keyspace balance, not
   popularity skew); and **bounded-load consistent hashing**, which caps each worker at `(1+ε)` of
   average with a proof and is the principled answer. Hedging also helps, for a reason worth stating:
   hedging to the *runner-up in the same ranking* warms a second copy of each hot prefix, so the fleet
   converges on a replication factor of 2 for hot keys — that took my p99 to 958 ms *and* the hit rate
   to 0.675.

7. **How would you rate-limit by tokens when you do not know the output length?** Two phases. Reserve
   `prompt_tokens + max_tokens` at admission in one CAS and write a reservation record keyed by request
   id; settle the unused difference at end of stream. Then handle the three failures: the gateway dying
   between them (the reservation carries a lease and is reclaimable by a reaper — Phase 4's pattern
   applied to tokens), the settle being retried (idempotent on request id, so the reservation record is
   the dedup point and the settle CAS goes through it), and actual exceeding estimate (clamp
   `max_tokens` at admission, which is why real APIs require it, or let the bucket go into debt). Note
   what it buys architecturally: reserve/settle is how you make a *pessimistic* limit on an *unknown*
   quantity — the same shape as two-phase commit against a counter.

8. **How do you cache LLM responses safely?** Split it. An exact-match layer keyed on a hash of the
   normalised prompt in the shared store, so a hit on any replica is a hit on all, costing one hash and
   one read with no embedding. A near-duplicate layer needs vectors and a similarity scan, neither of
   which belongs in a consensus-replicated store — vectors are large and a scan costs a linearizable
   read per candidate — so keep it local per replica and accept that near hits are per-replica knowledge
   accumulating as the replica serves traffic. The key must include everything that changes the answer:
   model id, sampling parameters, system-prompt version, tenant. Writes can be plain last-writer-wins
   rather than CAS, because either answer is valid for the prompt — say that deliberately, since it
   shows you know when *not* to reach for CAS. Then the honest part: a semantic cache can serve **a
   different prompt's** answer. The threshold is a point on a precision/recall curve, the risk gets
   *worse* with a better embedder because it rates more things similar, and which risk is acceptable is
   a product decision, not a systems one. Offer the mechanisms (threshold, TTL, per-tenant key scoping,
   exact-match-only as the default) and make someone else own the policy.

9. **How would you measure whether your routing change actually helped?** Design the regime first. My
   first attempt ran 4 prefixes over 4 workers with a default-size prefix cache; within seconds every
   worker had cached every prefix, the baseline already scored 0.9, and the comparison proved nothing.
   **A benchmark that cannot distinguish the thing it measures is worse than no benchmark**, because you
   will quote its number. The fix was cache-capacity-bound workers (32 blocks, ~3.5 system prompts) with
   12 prefixes, so locality is the binding constraint — which is also the real regime, since an
   attention KV cache is GPU-memory-bound and evicts constantly. Then run the load **concurrently**,
   because sequentially every worker is idle on arrival and least-loaded degenerates to a deterministic
   tie-break that looks exactly like affinity. Then measure *mechanism* and *outcome* separately — hit
   rate, mean prefill and distinct-workers-per-prefix for the mechanism; p50/p99 TTFT, throughput and
   per-worker counts for the outcome — and be ready for them to disagree, because mine did: affinity
   worked perfectly (spread 3.92 → 1.08) and made throughput worse. Had I reported only hit rate I would
   have shipped a regression.

10. **What breaks first when you 10x this system?** The single-key worker registry (`workers/index`).
    Every renewal is a read-modify-CAS of the whole worker list and every gateway re-reads it
    periodically, so at 10x workers both CAS contention and value size grow linearly, and on a sharded
    store the whole thing sits on one group's leader. Fix: per-worker keys plus a listing primitive, or
    a purpose-built discovery system. Second is the hot tenant's rate-limit bucket — one key, one CAS
    per request — which wants the two-tier lease-a-batch design. Third is hedging: with no budget,
    fleet-wide slowness makes every request hedge and doubles load on the thing already slow. Fourth is
    the semantic cache's local index, brute-force at ~1.44 ms per 5000 vectors — fine now because
    generation is 100x that, a problem at millions, and why `Index` is an interface. What does *not*
    break is the gateway tier: it is stateless, so it scales by adding processes. The shape of the
    answer: **the stateless parts scale, the single-key shared state does not**, and every one of those
    keys is a place where I traded simplicity for a ceiling I can name.

## 15. Your notes

Answer these in your own words, here, before starting Phase 6.

1. §10's central fact is that prefix routing tripled the cache hit rate and *lowered* throughput.
   Explain to someone who has only read §5 why those are not a contradiction, using the per-worker counts
   (15/21/105/99). Then state in one sentence each what `-max-inflight` and bounded-load consistent
   hashing do about it, and which you would reach for first.
2. The rate limiter needs a fence (the CAS) but not a lease; the worker registry needs a lease but not a
   fence; Phase 4's job queue needed both. Write down the property of each *problem* — not each
   implementation — that decides which mechanism it needs. Then apply your rule to Exercise (a)'s token
   reservation and predict which it needs before building it.
3. Both war stories in §9 are cases where a component worked correctly and the system was still wrong:
   the hash had perfect minimal disruption and terrible balance; least-loaded routing read exactly the
   field it was designed to read. Describe what kind of test catches this class of bug, and why
   `TestRendezvousMinimalDisruption`'s ±20% bound (`router_test.go:167-170`) does different work from its
   `moved == movedFromRemoved` assertion (`:164-166`).
4. Phase 4 §13 asked what "discard the result" means when the result is tokens already sent to a client.
   Answer it against the code: a hedge loser may already have produced tokens into its `a.tokens` channel
   (`server.go:549-553`) when it is cancelled at `:442-447`. Trace exactly what happens to those tokens
   and explain why the client can never observe an interleaving of two attempts' output. Then state what
   would have to change if the two attempts could produce *different* text (they cannot today — tokens
   are a function of `(promptHash, index)`, `mock.go:224-231`).
5. In one paragraph: Phase 6 is deterministic simulation, fault injection and tracing. Name the one thing
   in this phase you are least confident is correct under adversarial timing, and describe the
   seed-reproducible test you would write for it. (Candidates: the winner-selection loop's `pending`
   accounting at `server.go:411-436`; `localInflight`'s decrement on a goroutine that exits early; the
   registry's prune-while-renewing race; the limiter's `MaxAttempts` exhaustion under 50 gateways.)

## Reading

- Dean & Barroso, **"The Tail at Scale"** (CACM 56(2), 2013), §6-7 on hedged and tied requests. The
  1800 ms → 74 ms for ~2% extra requests figure is the one to remember; the "tied request" variant is
  what this gateway's cancel-the-loser approximates from the client side.
- **vLLM**: Kwon et al., "Efficient Memory Management for LLM Serving with PagedAttention" (SOSP 2023),
  for what `infer/mock`'s block cache models, plus vLLM's automatic-prefix-caching docs for the
  block-hash scheme `blockHashes` (`mock.go:169-178`) imitates — read the continuous-batching section
  alongside §12. **SGLang**: Zheng et al. (2024) for RadixAttention, prefix sharing as a radix tree
  rather than a flat block cache, and what that changes about what a router should hash on.
- **Rendezvous hashing**: Thaler & Ravishankar (1996) — read the independence assumption in the
  analysis, then re-read `router/router.go:270-282`. **Bounded-load consistent hashing**: Mirrokni,
  Thorup & Zadimoghaddam (2016) and Google's write-up — Exercise (b), and the principled answer to §10.
- **Feature hashing**: Weinberger et al. (ICML 2009) — the signed-hash trick in `NGramEmbedder.bump`
  (`semcache/embed.go:94-106`) and why the sign makes collisions cancel rather than accumulate.
- DDIA ch. 1's latency-percentile discussion, for why p99 is the number and why averaging percentiles
  across services is wrong — relevant to reading §10's table honestly. Envoy's `LEAST_REQUEST` and
  `RING_HASH`/`MAGLEV` docs are the same two families as `PickLeastLoaded` and `Pick`, with production's
  answers to the same trade-off.
