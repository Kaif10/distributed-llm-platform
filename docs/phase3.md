# Phase 3 — Sharding and rebalancing

Code: `shard/shard.go`, `shardkv/` (`api.go`, `raftcommand.go`, `server.go`), `shardctrl/`
(`machine.go`, `server.go`, `client.go`), `shardkv/grpctransport`, `cmd/shardctl`, `cmd/shardkv`,
`cmd/kvctl`, `proto/shardctrl/v1`, `proto/shardkv/v1`.
Run: `go test ./shardkv/...`, `go test ./shardctrl/...`, `go test -run Linearizability ./shardkv/...`.

Build note, honestly stated: this phase's scaffolding (proto, generated stubs, cmd wiring, test
harnesses) was produced with heavy parallel-agent orchestration. The two pieces that decide whether
the whole thing is *correct* — `applyConfig`/`applyMigration` in `shardkv/server.go` and the
rebalancing algorithm in `shardctrl/machine.go` — were written by hand, because these are exactly the
places where a subtly wrong invariant does not show up until a specific interleaving hits it in
production. Everything downstream of those two functions was reviewed against them, not the other
way around.

## 1. What Phase 3 builds and why

Phase 2 gave you a replicated log with one leader. Every write, no matter which key, funnels through
that one leader's consensus round. Adding replicas does not help — replicas make the *existing*
throughput durable, they do not add more of it. The only way to get more throughput is more leaders,
each responsible for a disjoint slice of the keyspace. That is sharding: partition the key space into
`shard.NShards` shards (`shard/shard.go:27`), assign each shard to exactly one replica group at a
time, and let groups run their consensus independently. Throughput now scales with the number of
groups, not with one leader's fsync rate.

Splitting the keyspace introduces a new object that also needs to be agreed upon: *who owns which
shard, right now*. That is a second, much smaller, piece of replicated state — the shard controller
(package `shardctrl`) — and a new kind of event a KV group's log has to account for: its own set of
owned shards changing out from under it while clients keep writing.

The guarantee, stated exactly, matching the precision of Phase 2's guarantee:

- **A key's operations remain linearizable across its whole lifetime**, including the interval during
  which its shard is migrating. There is no window where two groups both serve the key, and no window
  where the key is silently unreachable through the API that should serve it (`ErrNotReady` exists
  precisely to distinguish "not yet, retry" from "not ever, go elsewhere" — section 5).
- **A client's retry is recognized correctly even after its key's shard has moved to a different
  group.** The idempotency session table is part of the shard's state, not the group's, so it travels
  with the shard when the shard migrates (`shardkv/server.go:307-318`, and the migrated-session test
  in section 9). A CAS applied once before a move is answered from the cached result after the move,
  not re-evaluated against post-move state.

```
                     ┌───────────────────┐
                     │     shardctrl      │   Raft group; state = history of Configs
                     │ (its own Raft group)│   Config N: shard -> owning gid, gid -> addrs
                     └─────────┬──────────┘
                 Query(-1)     │      Join / Leave / Move
              (poll loop)      │      (operator, via shardctl)
        ┌──────────────────────┼───────────────────────┐
        │                      │                        │
        ▼                      ▼                        ▼
 ┌─────────────┐        ┌─────────────┐          ┌─────────────┐
 │ shardkv gid=100 │    │ shardkv gid=200 │      │   kvctl      │
 │  Raft group,    │    │  Raft group,    │      │ (client)     │
 │  owns shards    │    │  owns shards    │      │ resolves     │
 │  {1,3,5,7,9}    │    │  {0,2,4,6,8}    │      │ shard->gid   │
 └───────┬─────────┘    └───────┬─────────┘      │ via ctrl,    │
         │   HandlePullShard    │                │ talks to gid │
         │◄─────pull────────────┘                │ directly     │
         │  (ShardMigration RPC, grpctransport)   └──────────────┘
         │
   during migration of, say, shard 3 from gid=100 to gid=200:
     gid=100: applyConfig freezes shard 3 into `outgoing[3][N]`
     gid=200: applyConfig marks shard 3 `needed`, pollLoop pulls it,
              applyMigration installs it, drops `needed[3]`
```

The price, on top of Phase 2's per-write consensus-plus-fsync cost: a background poll against the
controller on every replica's leader, and, whenever ownership changes, a pull of the whole shard's
state across the network — the sharded-KV analogue of Phase 2's `InstallSnapshot`, at shard
granularity instead of whole-log granularity.

## 2. The three-kind Raft log

Compare with `raftkv`, whose log held exactly one kind of entry (`kvv1.LogEntry`). A shardkv group's
log holds three (`shardkv/raftcommand.go:12-16`):

- `opKV` — a client Put/Get/Delete/CAS, same shape as Phase 2.
- `opConfig` — "the configuration is now number N."
- `opMigration` — "here is shard S's data as of the end of config N-1, pulled from its previous
  owner."

All three go through the *same* Raft log, in the same total order, rather than config changes and
migrations being handled by some side channel. The package doc at `shardkv/api.go:25-33` states why:
"did this write happen before or after the shard moved" needs one unambiguous answer, on every
replica, with no possibility of disagreement. If config changes lived outside the log — say, applied
directly whenever the poll loop noticed a new config — two replicas of the same group could adopt the
new config at different points relative to an in-flight client write reaching each of them, and they
would diverge on whether that write landed under the old owner or the new one. Putting the config
change *in* the log at a specific index pins that question to "which side of this log index" —
exactly the same trick Raft itself uses to answer "which term was this entry proposed under."

`command` (`shardkv/raftcommand.go:28-45`) is gob-encoded, not protobuf, and the doc comment at
`raftcommand.go:18-27` explains why the two internal-vs-external command types get different
treatments:

- `command` never crosses a process boundary on its own — Raft's `Command` field is already an
  opaque `[]byte` to any RPC, so nothing outside this one binary ever needs to parse it, and gob's
  requirement ("something this binary can decode back") is exactly met without paying for a schema
  that has to evolve across versions or languages.
- `KVBytes` is still a proto-marshaled `kvv1.LogEntry`, not an embedded Go struct, because generated
  protobuf types carry internal reflection bookkeeping (`MessageState`) that gob is not designed to
  walk safely. Marshaling it once, ourselves, and storing the resulting `[]byte` keeps `command` a
  plain, fully gob-safe value while still reusing the exact wire type `raftkv` already validates
  against.

## 3. Walk `applyConfig` in detail

This is the crux of the phase — the function every other correctness property in this doc rests on.
Read it at `shardkv/server.go:264-300`.

**It is a pure function of `(cur, next, gid)`.** Given the group's current config, the next config,
and this group's own id, it decides, shard by shard, what changes. No I/O, no clock, no randomness —
the same discipline `Machine.Apply` observed in Phase 1/2, generalized to a richer piece of state.
Every replica in the group applies the same `next` at the same log index and reaches byte-identical
`cur`, `machines`, `needed`, and `outgoing` afterward.

**The `gid == 0` sentinel.** A shard's owner in a `Config` is a group id; `0` means "nobody owns this
yet" (`shardkv/api.go:58`, and `shardctrl`'s `initialConfig`, `shardctrl/machine.go:143-149`, starts
every shard there). It is not a real group and never will be — group ids are assigned by whoever runs
`shardctl join`, and `0` is deliberately outside that space.

**Losing a shard** (`server.go:270-283`). When `oldOwner == s.gid && newOwner != s.gid`, the group
takes a `Machine.Snapshot()` of exactly that shard's state — its key-value data *and* its dedup
sessions, the same reason Phase 2's `maybeSnapshot` had to include sessions
(`docs/phase2.md` §3.3) — and freezes it into `outgoing[i][next.Num]`
(`server.go:279-282`), keyed by the config number that took the shard away. That key choice is not
arbitrary: it is *exactly* the config number the new owner will ask for when it pulls (see
`HandlePullShard` below), so no further coordination between the two groups is needed to agree on
"which version of the shard" is being requested. The local `machines[i]` entry is deleted
(`:283`) so this group can no longer serve it even if asked.

**Gaining a shard** (`server.go:285-297`). Two cases:
- `oldOwner == 0`: the shard was never owned before (the very first real config after groups join a
  fresh cluster). There is nothing to fetch; `machines[i] = store.NewMachine()` creates it empty on
  the spot (`:293`).
- `oldOwner != 0`: a real handoff is required. The group records a `neededShard{FromGID, FromAddrs,
  AtConfig}` (`:295`) rather than blocking here. `FromAddrs` is read from `s.cur.Groups[oldOwner]` —
  the *old* config's view of the departing group's addresses — because the *new* config's `Groups`
  map might not even list a group that just left the cluster (`server.go:288-291`).

**Why apply-time does not block on `needed` being non-empty.** `applyConfig` sets `s.cur = next`
unconditionally at the end (`server.go:299`), even when it just populated `needed` with shards the
group cannot yet serve. This is a deliberate design choice with an alternative that was rejected:

- *Chosen design*: adopt the new config immediately at apply time; gate only the client-visible path.
  `applyKV` (`server.go:237-255`) checks `s.needed[cmd.Shard]` and returns `ErrNotReady` if the shard
  is still in flight (`:241-243`). The group's notion of "what config am I on" and "what can I
  actually serve" are two separate, independently-checkable facts.
- *Alternative*: defer adopting `next` until every shard it promises has actually arrived, so `cur`
  only ever describes a config the group can fully serve.

The alternative sounds safer but is not: it couples two things that do not need to be coupled, and it
breaks in a specific way. Suppose config N+1 also *removes* a shard this group holds (a group can gain
one shard and lose a different one in the same config bump). If adoption of N+1 waits for the gained
shard's migration to finish, the group keeps serving the *lost* shard under the old config for however
long the unrelated migration takes — a different key, on a different shard, held hostage by a pull
that has nothing to do with it. Worse, the poll loop (section 4) would have nothing well-defined to
advance from: "am I on N or not" would depend on progress of a migration that is itself driven by
"what config am I on." The chosen design breaks that circularity: config adoption is instantaneous and
unconditional, ownership bookkeeping (`needed`) is separate, and only client-visible serving is gated,
per shard, on that bookkeeping being clear. It is simpler and it is still correct, because the
`needed` check happens at the same apply-time, same-log-index granularity as everything else — no
replica can observe `cur` without also being able to compute `needed` consistently at that instant.

**`applyMigration`'s idempotency** (`server.go:307-318`). The guard is
`need.AtConfig != cmd.MigConfigNum` (`:309`): a migration entry is accepted only if the shard is
*still* marked needed, at *exactly* the config number this migration claims to satisfy. This defends
against two distinct duplicate-delivery scenarios, both structurally identical to Phase 1's dedup
lesson applied to the group's own internal traffic rather than a client's:

- A leader change re-proposes the same migration a second time (`proposeInternal`, `server.go:406-408`,
  deliberately does not wait for commit and just lets the next poll tick retry — see its comment).
  The second copy finds `needed[shard]` already deleted (from the first copy's apply) and is a no-op.
- A *later* config already moved the shard elsewhere before this stale migration for an *earlier*
  config arrives (possible after a slow or partitioned fetch). `need.AtConfig` no longer matches
  `cmd.MigConfigNum`, so the stale delivery is rejected even though `needed[shard]` might currently
  hold a *different* pending request for the same shard index.

**`HandlePullShard`'s "only a leader that has already applied the transition can answer" rule**
(`server.go:483-495`). Two conditions gate an answer: `s.IsLeader()` (`:484`) and the frozen snapshot
actually being present at `outgoing[shardID][configNum]` (`:489-494`). The leader check matters
because a stale leader (partitioned, about to be deposed) could otherwise hand out a shard's state
from a `machines` map its own cluster no longer agrees is current — the same "deposed leader answers a
local read" hazard Phase 2 solved for `Get` by routing it through the log (`docs/phase2.md` §5),
generalized here to "a deposed leader must not answer administrative reads about ownership either."
Being present in `outgoing` at that specific config number *is* the proof the transition has been
applied: the snapshot only exists there because `applyConfig` put it there, at the same log index that
made the group stop owning the shard. No separate coordination protocol between the two groups is
needed to establish "has the source actually let go of this shard" — the presence of the frozen bytes
at the right key is the whole proof.

## 4. The poll loop discipline

`pollLoop` (`shardkv/server.go:421-455`) runs on every replica but only acts when
`s.IsLeader()` (`:430`) — followers passively receive whatever the leader proposes, same as
Phase 2. Each tick does exactly one of two things, never both, and in a fixed priority:

1. If `needed` is non-empty, call `pullNeeded` (`:442-445`) and go around again — **do not** even ask
   the controller for a newer config while shards from the current one are still outstanding.
2. Only once `needed` is empty does it ask the controller for `curNum+1` (`:447-453`) and, if one
   exists, propose an `opConfig` entry for it.

The comment at `server.go:410-419` names this "one config at a time, migrations fully drained before
asking for the next," and states the reason precisely: it is what keeps the log's implicit dependency
— config N+1 depends on migrations promised at config N — actually satisfied on disk, in the log,
not just in the poll loop's head.

Concretely, what would go wrong if a group raced ahead to config N+2 before finishing N's incoming
shards: `applyConfig` computes gains and losses as a diff against `s.cur`, one step at a time
(`next.Num != s.cur.Num+1` is rejected outright, `server.go:265-267`), so the group cannot actually
apply N+2 while sitting at N — that specific race is structurally blocked by the check. But suppose
the poll loop tried anyway, proposing N+1 while shards from N are still `needed`, purely to see what
breaks: a shard the group is *still waiting to receive* from N could be *reassigned again* under N+1,
to a *third* group, before this group ever finished migrating it in. Now `needed[shard].AtConfig` is
stale relative to what N+1 actually wants (the shard is not even owned by this group under N+1
anymore, so there is nothing to reconcile it against), and whichever migration eventually shows up for
the N-vintage pull lands on a group that has already moved past caring about that shard, is silently
dropped by the idempotency guard in section 3, and the shard's state — that specific version of it —
is never actually delivered anywhere. The one-thing-at-a-time discipline is what guarantees that
every shard this group is ever told to own gets a chance to actually arrive before the bookkeeping
that describes it can be superseded.

## 5. Ownership checks happen twice

Exactly the shape of Phase 2's term-recheck lesson (`docs/phase2.md` §4, bug 5), generalized from
"is my term still current" to "do I still own this shard":

- **Propose time** (`server.go:339-361`), cheap, no log write. If `s.cur.Owner(cmd.Shard) != s.gid`,
  return `ErrWrongGroup` immediately (`:346-349`); if the shard is `needed`, return `ErrNotReady`
  immediately (`:350-353`); if the request is a retry already answered, return the cached result from
  `Dedup` without touching the log at all (`:356-359`). None of this is authoritative — it exists
  purely to avoid paying for a round of consensus on a request this replica already knows cannot
  succeed.
- **Apply time** (`applyKV`, `server.go:237-255`), authoritative, the only check that matters for
  correctness. The exact same two conditions are re-checked (`:238-243`) against whatever `s.cur` and
  `s.needed` are *at the log index this entry actually lands at* — which can differ from what they
  were at propose time, because a config change can commit in between.

The two errors tell a client to do two different things, and the distinction is worth stating exactly
because it is easy to conflate:

- **`ErrWrongGroup`** (mapped to gRPC `FailedPrecondition`, `server.go:617-618`): this group is not,
  and as far as it knows was never supposed to be, the owner. The client's map of shard-to-group is
  stale; it must re-query the controller and retry against whoever actually owns the shard now. No
  amount of waiting on *this* group helps.
- **`ErrNotReady`** (mapped to `Unavailable`, `server.go:619-620`, grouped with the other "retry the
  same server shortly" errors): this group *is* the current, correct owner under the config it knows
  about — the client's routing was right — but the migration has not finished landing. Retrying the
  *same* server shortly is exactly right; no other group has been told to serve the shard either, so
  the data is not lost, just not locally available yet.

## 6. The rebalancing algorithm (shardctrl)

`rebalance` (`shardctrl/machine.go:360-430`) computes a new shard assignment from the current one
plus the sorted list of group ids that should exist after a Join or Leave. In plain terms:

1. Fix a target shard count per group *before touching any assignment*: `NShards / len(gids)`, with
   the remainder (`NShards % len(gids)`) handed out one extra shard each to the groups earliest in
   ascending gid order (`machine.go:370-378`).
2. Shed only true excess: walk groups in ascending gid order; a group already at or below its target
   is never touched; a group over its target gives up its own excess shards, in ascending shard-index
   order, into a reassignment pool (`:395-408`).
3. Fill the pool — unassigned shards first, then shed shards — by always handing the next shard to
   whichever group is currently furthest below its target, ties broken by ascending gid
   (`:410-428`).

Determinism here matters exactly as much as it did for `Machine.Apply` in Phase 1/2, and for the same
underlying reason: **two replicas of the controller must compute byte-identical configs from the same
log entry with no communication between them.** If `rebalance` ever depended on Go map iteration
order (ranging over `groups` directly instead of going through `sortedGids`,
`machine.go:314-325`, first) or on the *history* of how the current assignment came to be rather than
just its current value and the target gid set, two replicas could legally reach two different, both
internally-consistent, shard assignments for the same config number. That is a config-safety violation
with the same shape as State Machine Safety in Phase 2: not "the wrong answer" but "two different
answers to the same question, agreed upon by nobody." `TestDeterminism`
(`shardctrl/machine_test.go:150-185`) pins this directly: it replays the same command sequence into
two independent `Machine`s built from freshly copied (not shared) command values and requires
byte-identical configs at every step. `TestJoinBalance` (`:199-214`) and `TestJoinMinimalMovement`
(`:221-249`) pin the other two guarantees named above — every join reaches max-min shard count spread
of at most 1, and joining a fourth group to three already-balanced ones moves exactly the new group's
target count of shards, nothing more.

## 7. Two real bugs found by testing, and the general lesson each teaches

These came out of an actual multi-process run — three `shardctl`-managed controller nodes and two
independent three-replica `shardkv` groups, all as separate OS processes on one machine, driven by
`kvctl -ctrl ... bench` — not a unit test. They are two of the more useful things in this document,
so they are written up as war stories rather than compressed into a changelog line.

### Bug 1 — the `groupClusters` cache that wasn't one

The symptom was a `kvctl bench -n 500` run that reported **500 puts, 508 retries** — almost exactly
one retry per put. A ratio that close to 1:1 is a red flag, not noise: it is too clean to be ordinary
network jitter or an occasional leader hiccup, and it is worth stopping and asking *why* before
shrugging it off as "sharded mode has some overhead."

The cause was in `shardedClient.do` (`cmd/kvctl/main.go:416-438`): every call resolved the owning
group from the (cached) controller config and then called `s.clusterForGroup(gid, addrs)`. The
*original* version of that function built a brand-new `cluster[kvv1.KVClient]` on every single call.
A `cluster` is exactly the object that remembers "which replica answered as leader last time"
(`cluster.cur`, updated by `markGood`, `cmd/kvctl/main.go:200-206`) — the entire point of it existing
is to let the *next* request skip the "guess wrong, get told who the leader is, retry" round trip that
`doOn`'s retry loop otherwise has to pay on a cold guess. Throwing that object away and building a
fresh one every call means every single request starts from `cur = 0` — a coin flip (worse, a 1-in-3
flip with three replicas) on whether it happens to land on the leader first try. With 3-replica groups
that predicts almost exactly what was measured: most calls need one extra attempt to find the leader.

The fix is `groupClusters map[int64]*cluster[kvv1.KVClient]` (`cmd/kvctl/main.go:315`), keyed by group
id and reused for the life of the `shardedClient`, with the doc comment at `main.go:306-314` spelling
out exactly this reasoning for future readers. After the fix, the same benchmark reported **500 puts,
16 retries** — the residual 16 being genuine leader changes and transient contention, not a structural
guess-every-time tax.

**The lesson, stated generally**: a retry-avoidance cache is only useful if it survives across the
calls it exists to help. A "cache" that gets rebuilt on every call that would consult it is not a
cache; it is a local variable with a comment claiming otherwise. This class of bug does not show up in
a single-process test with an in-memory fake network (`shardkv_test.go`'s `testCluster.do` keeps its
`group` struct, with its own `lastLeader`, for the whole test) — it needed the real thing, real
processes, real gRPC round trips, for the retry cost to be visible in wall-clock numbers instead of
buried in test noise.

### Bug 2 — the shared client identity in `bench`

Fixing bug 1 made requests fast enough that a *second*, previously-invisible bug started firing:
concurrent `bench` workers began aborting the whole run with `store.ErrStaleRequest`
(`FailedPrecondition`, `shardkv/server.go:621-622`).

The cause: every `-c` concurrent goroutine in `bench` was using the *same* process-wide `clientID` and
the *same* shared `atomic.Uint64` request-id counter (`newMeta()`, `cmd/kvctl/main.go:65-67`), even
though `bench` spawns `-c` goroutines that issue puts *concurrently*, not one at a time. Phase 1's own
documented dedup contract (`kv/store`'s package doc, restated in `docs/phase1.md` §2.6's "one
outstanding request per client" note) is explicit: the table remembers one request id per client
identity, and it is correct only if a client never has more than one request outstanding at a time.
Ten goroutines sharing one client identity is ten *real* clients pretending, via a shared counter, to
be one logical client with sequential requests — the exact assumption the store's dedup design leans
on, violated by the caller, not the callee.

With that assumption violated, two goroutines' puts can commit in an order that does not match the
order their shared counter handed out request ids in (goroutine A grabs id 41, goroutine B grabs id
42, but B's commits first). The one that lands second, now carrying a *lower* id than what the store
has already recorded as `lastRequestID` for that shared identity, is correctly rejected as stale — this
is `store.ErrStaleRequest` doing exactly its documented job, a **safe** failure with no data
corruption, no double-application, nothing silently wrong. But it is still a bug, because it took down
the entire benchmark on what was actually ordinary concurrent traffic against a working system, not a
fault the harness was trying to inject.

The fix: each `bench` worker goroutine mints its **own** `workerClientID` via `randomHexID()` and
tracks its **own** `workerReqID` counter, entirely private to that goroutine
(`cmd/kvctl/main.go:609-621`, with the comment there walking through exactly this reasoning). Each
goroutine still issues its own puts one at a time (sequentially, within itself), so the
one-outstanding-request-per-client invariant holds again — now that "per client" correctly means "per
goroutine issuing requests independently" rather than "per OS process."

**The lesson, stated generally**: "at most one outstanding request per client" is a contract about
*real* clients, not about however many identity strings a caller happens to allocate. A safe rejection
at the callee is not proof the callee is fine and the caller is broken elsewhere — a fail-safe
rejection is still a bug in the *caller* when the caller's own design (sharing one identity across
goroutines that run concurrently and independently) is what violated the documented invariant in the
first place. And this bug is a good example of why fixing one bug can *reveal* another: the retry
storm from bug 1 had been operating as an accidental mutex, serializing enough of the concurrent
traffic through repeated retries that the real races underlying bug 2 rarely got the chance to
actually interleave. Making the system fast surfaced a correctness bug that slowness had been hiding.

After both fixes, confirmed under real multi-process load — three `shardctl` nodes and two
three-replica `shardkv` groups, all separate OS processes — the same class of benchmark ran clean
twice: **1000 puts, 32 retries**, twice in a row.

| Stage | Puts | Retries |
|---|---|---|
| Before either fix | 500 | 508 |
| After fixing `groupClusters` | 500 | 16 |
| After also giving each bench worker its own identity (repeated) | 1000 | 32, 32 |

## 8. What is NOT implemented, on purpose

Stated plainly as scope limits, not bugs to be embarrassed about:

- **No shard GC.** `outgoing[shard][configNum]` snapshots (`server.go:107, 279-282`) are kept forever
  once frozen. There is no acknowledgment protocol by which the new owner tells the old one "I have
  it, you can free that," so a long-lived group that sheds many shards over its lifetime accumulates
  an ever-growing `outgoing` map. This is a real, known memory leak in the current design, left
  unaddressed because building the ack protocol correctly (including what happens if the ack itself is
  lost, or arrives after yet another config moved the same shard again) is its own, separable design
  problem — Exercise (a).
- **No cross-shard transactions.** An operation touches exactly one key, hence exactly one shard, by
  construction (`shard.Key2Shard`, called once per KV request in `proposeKV`, `server.go:604`). There
  is no way to atomically move money between two keys that might live in different shards — that needs
  an actual distributed transaction protocol (two-phase commit at minimum, ideally something
  Spanner-shaped with real timestamps), matching the stated scope of a standard sharded-KV lab
  (`shard/shard.go:14-16`).
- **A fixed `NShards = 10`** (`shard/shard.go:27`), not consistent hashing. Simpler rebalancing
  algorithm, simpler migration protocol (a shard is always the same size class), at the cost of a hard
  ceiling on how finely load can be spread and a hash function baked into every replica and the
  controller alike. Consistent hashing with virtual nodes is the natural follow-up — Exercise (b).
- **Single-shot pull, no chunking.** `PullShard` (`shardkv/grpctransport/transport.go`) sends one
  shard's entire serialized `Machine` in one RPC, capped only by `MaxMessageSize` (64 MiB,
  `transport.go:37`). This mirrors Phase 2's `InstallSnapshot`, which also has no chunking. Fine for a
  fixed, modest shard count; a shard holding gigabytes of data would need to stream it in pieces the
  way a production system's snapshot transfer does.

## 9. How we know it works

**`shardkv` tests** (`shardkv/shardkv_test.go`):

| Test | What it proves |
|---|---|
| `TestBasicSingleGroup` :322 | a single group serves Put/Get/CAS correctly with sharding wiring in place but no migration involved — the baseline |
| `TestMigrationMovesDataAndSessions` :349 | the core Phase 3 correctness property: a key's value AND its dedup session move with the shard; a pre-move CAS's retry, replayed after the shard has moved, is answered `swapped=true` from the cached result, not re-evaluated against post-move state; the old owner provably refuses the shard afterward |
| `TestWritesContinueThroughRebalance` :448 | six concurrent writers keep succeeding across a live config change that splits ownership of half the shards to a second group; every write the client saw succeed resolves afterward through whichever group now owns it |
| `TestCrashDuringMigrationRecovers` :528 | a replica of the *incoming* group crashed before a migration even starts must, after restarting, agree with its groupmates that it owns the shard and serve it correctly — recovered from the group's own Raft log/snapshot, not from anything special done to help it |
| `TestLinearizabilityAcrossShardMoves` :669 | the Phase 3 exit criterion: 3 groups, 9 replicas, 6 clients, 12 seconds, a nemesis that reassigns shards roughly every half second on top of the usual unreliable network, checked with Porcupine exactly as in Phase 2 but with a live rebalance running throughout. Measured: **3191 operations across 3 groups, zero violations.** |

**`shardctrl` tests**: `machine_test.go` proves the rebalancer is deterministic
(`TestDeterminism` :150), balanced (`TestJoinBalance` :199), and minimal-movement
(`TestJoinMinimalMovement` :221), plus dedup and snapshot round-trips at the Machine level.
`server_test.go` proves the Raft-driven-server shape survives the same faults Phase 2 demanded of
`raftkv`: leader failover (`TestLeaderFailover` :310), a retried request across a leader change
applying exactly once (`TestRetryAcrossLeaderChangeAppliesOnce` :354), and a full-cluster crash
preserving the entire config history, not just the latest config (`TestFullClusterCrashPreservesHistory`
:406) — the last one matters because, unlike a KV store's map, a controller's whole *point* is that
old configs stay queryable (`Machine.Snapshot` keeps full history, `machine.go:479-483`), so a restart
losing history would be a much larger regression than losing an old Get's answer.

**The manual multi-process test** the mentor ran, with no automated harness: three `shardctl`-managed
controller nodes and two independent three-replica `shardkv` groups, all as separate OS processes.
Join group A; write 20 keys through `kvctl -ctrl ...`; join group B *while the cluster is live*;
watch the controller rebalance roughly half the shards onto B; read all 20 keys back through
`kvctl`, correctly, with zero manual intervention — no restart, no re-pointing kvctl at a different
address, nothing but time for the poll loops to converge. This is also where the two bugs in section 7
were found: they never showed up in the in-process simnet tests, only under real gRPC round trips with
real per-process latency.

## 10. Exercises

**(a) Shard GC.** Design an ack protocol: the new owner, once it has successfully applied an
`opMigration` for a shard, tells the old owner "config N's copy of shard S is safely installed," and
the old owner frees `outgoing[S][N]` only after that ack (and only that specific entry — an old owner
may hold several vintages of the same shard if it lost and regained it more than once). Handle the ack
itself being lost or duplicated the same way everything else in this phase handles duplicates: make
freeing idempotent, and make the old owner tolerate never receiving the ack at all (a leak that is
bounded by "how many times this group has ever lost this shard," not by client traffic, is a much
smaller problem than the current unbounded one).

**(b) Consistent hashing with virtual nodes**, replacing `NShards = 10` and `shard.Key2Shard`'s FNV
hash. Sketch it, and then explain in a paragraph why this changes the migration story *less* than it
looks like it should: the unit of migration is still "a contiguous range of hash space that maps to
one owner," which is still shard-sized (or virtual-node-sized) chunks of state pulled in one `Machine`
snapshot at a time — everything in `applyConfig`/`applyMigration`/`HandlePullShard` stays the same
shape. What actually changes is only the *assignment function* (which virtual nodes map to which
physical group) and the granularity knob (how many virtual nodes per group, which trades rebalance
smoothness for controller-state size). The migration protocol was never coupled to `NShards` being a
small fixed constant; it only needed "a deterministic, finite partition of the keyspace," which
consistent hashing still provides.

**(c) Automatic rebalancing off a load signal**, instead of only operator-driven Join/Leave/Move.
Sketch what the signal would be (shard hit rate? group CPU? both?), where it would be computed (a
group reports load to the controller; the controller decides), and what stops it from oscillating
(hysteresis, a minimum interval between automatic moves, a threshold rather than "always move toward
perfectly even").

**(d) Cross-shard read snapshot**: read two keys that might live in different shards, as of a single
consistent point in time. Sketch why this is hard *without* an actual distributed transaction
protocol: even if both reads are individually linearizable, there is no shared clock or shared log
between the two groups that lets you say "these two reads observed the same instant" — one group's
"now" and the other's "now" are only related by whatever real-time guarantee your clocks give you,
which for two independent Raft groups with no coordination is nothing. This needs something in the
shape of Spanner's TrueTime-bounded commit timestamps, or a two-phase read protocol, not a KV-store
feature.

**(e) Chaos: real network partitions between groups**, not just crashes, in the Porcupine test. Extend
`raft/simnet` (or build something in its shape) into a fake for `shardkv.Fetcher` and
`shardkv.Controller`, so a nemesis can partition group A from group B (migration pulls fail, `ok=false`,
retried later — exactly the "unreachable" path `ShardFetcher`'s doc already describes,
`shardkv/api.go:82-85`) independently of partitioning either group internally (which `simnet` already
does for each group's own Raft traffic). The interesting new case this would exercise: a migration
that is *ready* on the source side (frozen in `outgoing`) but cannot be *pulled* for an extended,
partition-length period, while ordinary client traffic against both groups' other shards keeps
running and must still be linearizable throughout.

## 11. Interview questions with model answers

1. **How would you shard a KV store, and what decides the shard count?** Hash (or range-partition)
   keys into a fixed number of shards, assign each shard to one replica group at a time via a small
   separately-replicated piece of metadata (the controller), and let each group run its own consensus
   independently so throughput scales with group count. The shard count is a trade-off: too few and
   you cannot spread load finely or rebalance smoothly when a group joins or leaves; too many and the
   controller's metadata and the per-migration bookkeeping grow past what is worth it for the flexibility
   gained. A fixed count (here, 10) is the simplest correct starting point; consistent hashing with
   virtual nodes removes the ceiling at the cost of a marginally more complex assignment function
   (Exercise b).

2. **How do you migrate a shard without losing writes or serving stale ones?** Put the migration
   itself through the same replicated log as client writes, so "before or after the move" has one
   answer per replica (section 2). On the losing side, freeze a snapshot of exactly the shard's state
   (data and dedup sessions) at the moment ownership changes (`applyConfig`'s loss branch,
   `server.go:270-283`); on the gaining side, refuse client traffic for that shard (`ErrNotReady`)
   until the frozen snapshot has actually been pulled and installed (`applyMigration`,
   `server.go:307-318`). Neither group ever believes it can serve the shard when it cannot, and the
   old owner has nothing left to lose track of once it has handed off, because the freeze happened at
   a specific, agreed log index.

3. **Why must config changes and data migrations go through the same replication mechanism as client
   writes?** Because "did this write land under the old owner or the new one" needs a single,
   agreed-upon answer across every replica of the group, and the only mechanism this system has for
   giving every replica the same answer to an ordering question is appending both kinds of event to
   one totally ordered log (section 2). A side channel for config changes (applied outside the log,
   whenever a replica happened to notice) would let two replicas of the same group observe a client
   write on different sides of the ownership change, which is a linearizability violation with the
   same shape as a stale read in Phase 2.

4. **What happens to a client's in-flight request when its key's shard moves mid-request?** If the
   request had not yet reached the old owner's log, the client (or its retry) eventually gets
   `ErrWrongGroup` from the old owner (or a timeout, or nothing at all if it never got that far) and
   must re-query the controller and retry against the new owner. If it committed on the old owner
   just before the config change, the write is captured in the frozen snapshot the old owner takes at
   that log index and travels to the new owner intact, sessions included, so a retry lands on the new
   owner and is recognized as already-applied rather than reapplied (section 7's `groupClusters` bug
   story is exactly a client navigating this).

5. **How do you avoid split-brain on shard ownership — two groups both believing they own the same
   shard?** Ownership is a pure function of the controller's current config, and the controller's
   configs form a single linearizable sequence (it is itself a Raft-replicated state machine,
   `shardctrl/server.go`). A group commits to a config only through its own log (`applyConfig`), one
   step at a time (`next.Num != s.cur.Num+1` is rejected, `server.go:265-267`), so it cannot be two
   configs ahead or behind what it has actually applied. Two groups disagreeing about who owns a shard
   would require the controller itself to have produced two different configs claiming the same shard
   for two different groups — which its own Raft log and deterministic `rebalance` function
   (section 6) prevent by construction.

6. **How would you scale this to 1000 shards / 100 groups?** Replace the fixed `NShards` /
   FNV-hash assignment with consistent hashing and virtual nodes, so shard count is decoupled from
   group count and rebalancing moves proportionally small chunks (Exercise b). At that scale the
   controller itself may need to shard its own metadata (or at least keep it small — a `(shard ->
   group)` map for 1000 shards is still tiny, but a config's group-address map and the rebalancing
   computation's `O(NShards)` passes both stay cheap; the harder scaling limit is usually the number
   of *migrations in flight at once* and the fan-out of `PullShard` RPCs during a large
   rebalance, which wants throttling and possibly a background rate limit rather than "move everything
   the algorithm says to move, all at once").

7. **What's the difference between this design and DynamoDB/Cassandra-style ownerless sharding versus
   a Spanner/CockroachDB-style range-sharded design with a metadata service?** Dynamo-style systems use
   consistent hashing with no single owner per key at all — any of several replicas can accept a write,
   conflicts are resolved later (vector clocks, last-write-wins, CRDTs), and the trade is availability
   and partition tolerance over strict single-copy consistency. This design is the other family: one
   group owns a shard at a time, that group is internally linearizable via Raft, and a separate
   metadata service (the controller) is the single source of truth for the assignment — structurally
   the same shape as Spanner's placement driver or CockroachDB's range metadata, at a much smaller
   scale. You get linearizability per key at the cost of unavailability for a shard whose group has
   lost quorum, rather than Dynamo's "always writable, reconcile later."

8. **How do you test a distributed system for correctness under rebalancing?** Record a concurrent
   history (call/return times per operation, across many clients) while a nemesis actively rebalances
   shards (and, separately, injects partitions and crashes) throughout the run, then check the history
   against a sequential model with a linearizability checker (Porcupine here) rather than asserting on
   individual outcomes (`TestLinearizabilityAcrossShardMoves`, section 9). The key methodological point
   carried over from Phase 2: treat operations whose outcome is unknown (timed out) as pending with an
   open return time rather than guessing "succeeded" or "failed," because guessing wrong in either
   direction can make an actually-broken history look legal or an actually-fine history look illegal.

9. **Tell me about a subtle bug you found in testing and how you found it.** (Free gift — this is
   section 7, told as an interview answer.) Benchmarking a real multi-process sharded cluster, I saw
   500 puts produce 508 retries — almost exactly one retry per put. That ratio was too clean to wave
   off as network noise, so I went looking instead of shrugging. The client's sharded mode was
   supposed to cache, per group, which replica last answered as leader, so repeat requests to the same
   group wouldn't have to rediscover it — but the cache was being rebuilt from scratch on every single
   call, which is not a cache, it's a coin flip on a 3-replica group every time. Fixing that (reusing
   one `cluster` object per group id for the client's lifetime) dropped the same benchmark to 500
   puts, 16 retries. That fix made requests fast enough that a second bug started firing: concurrent
   benchmark workers began aborting on `ErrStaleRequest`. It turned out all the concurrent worker
   goroutines shared one client identity and one request-id counter, which violates the store's own
   documented "one outstanding request per client" invariant — concurrent completions could commit out
   of order relative to that shared counter, and the store correctly, safely rejected the
   out-of-order one. The rejection itself was not a bug in the store; the bug was in the benchmark
   giving ten concurrent goroutines one shared identity instead of ten separate ones. Giving each
   goroutine its own client id and counter fixed it, and a rerun at 1000 puts came back clean at 32
   retries, twice. The general lesson I took from it: fixing one bug can unmask another that
   coincidentally depended on the first bug's symptom (here, the retry storm was accidentally acting
   like a mutex, hiding a real concurrency bug behind artificial slowness).

10. **What guarantee does the controller give you and why does it need to be Raft-replicated rather
    than, say, a single database row with a lock?** It needs to give every group and every client the
    same linearizable sequence of configs, including under the controller's own node failures — a
    single unreplicated row is a single point of failure for the entire cluster's ability to agree on
    who owns what, and a lock around it does nothing for the case where that node crashes mid-update.
    Running the controller as its own small Raft group gives it the same fault-tolerant, linearizable
    log discipline the data plane has, sized to the much smaller volume of administrative traffic
    (Join/Leave/Move/Query) rather than client read/write traffic — which is also why, unlike
    `raftkv`/`shardkv`, its log never needs compaction in this design (`shardctrl/server.go:54-68`):
    admin operations are orders of magnitude rarer than client operations.

## 12. Your notes

Answer these in your own words, here, before starting Phase 4.

1. Walk through, step by step, what `applyConfig` and `applyKV` each see, in order, for a Put that
   arrives at the exact log index one after a config change moves that key's shard away. What error
   does the client get, and what must it do next?
2. `outgoing` snapshots are never freed today. Sketch, in your own words, the exact sequence of
   messages an ack protocol (Exercise a) would need, and identify the one failure mode (a lost ack, a
   crash between receiving data and sending the ack, a shard that moves again before the ack arrives)
   you think is hardest to get right, and why.
3. Explain, to someone who has only read Phase 2, why a shardkv group's Raft log needs `opConfig` and
   `opMigration` entries at all instead of just letting each replica call the controller and update its
   own local `cur`/`machines` independently whenever it feels like it. Be concrete about what
   observable bad behavior that alternative produces.
4. The `groupClusters` bug and the shared-client-id bug were found in that order, and the second one
   was invisible until the first was fixed. Describe, in your own words, why a system that is "too
   slow to race" can hide a genuine correctness bug, and what that implies about trusting a clean test
   run at low concurrency or low load.
5. In one paragraph: what did Phase 2's single-Raft-group design get you "for free" (with respect to
   ownership, ordering, and dedup) that Phase 3 had to re-earn with new machinery (`applyConfig`,
   `needed`, `outgoing`), and what is the first new failure mode a fourth Phase — a scheduler built on
   top of this sharded store — will have to worry about that neither Phase 1 nor Phase 2 nor Phase 3
   alone ever had to (hint: a lease that outlives the shard move of the key it is stored under).

## Reading

- Ousterhout's 6.5840 Lab 4 handout (sharded KV) is the direct ancestor of this phase's scope and
  test shape; the "you may assume operations do not span shards" and "shard movement should not
  block other shards" constraints it states are exactly sections 1 and 4 of this document.
- DDIA ch. 6 (Partitioning): partitioning by hash vs range, and rebalancing strategies — read
  `shardctrl/machine.go`'s `rebalance` alongside the "fixed number of partitions" strategy it
  describes.
- The Spanner paper's directory/placement discussion, and CockroachDB's range-based sharding docs,
  for the "range-sharded plus a metadata service" family this design belongs to (interview question 7).
- Amazon's original Dynamo paper, for the opposite family (ownerless, hash-ring, eventual
  consistency) and why its trade-offs differ from this design's.
