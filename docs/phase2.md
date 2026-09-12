# Phase 2 — Raft consensus and the replicated KV store

Code: `raft/` (`api.go`, `raft.go`, `election.go`, `replication.go`, `snapshot.go`, `filepersister.go`),
`raft/simnet`, `raft/grpctransport`, `kv/store/machine.go`, `kv/raftkv`, `cmd/raftkv`, `cmd/kvctl`,
`proto/raft/v1/raft.proto`.
Run: `make test-raft` (3A-3D suite), `make test-lin` (Porcupine), `make test`.

## 1. What Phase 2 builds

The same state machine as Phase 1, with step 2 of the four-step pattern replaced. `store.Machine`
(`kv/store/machine.go:22-39`) is the map plus the dedup table, no I/O, no locks. In Phase 1 `Store`
fed it from a local WAL; now `raftkv.Server` feeds it from a log that a Raft group agrees on
(`kv/raftkv/server.go:4-16`). Nothing in `Machine.Apply` changed. That was the point of splitting it
out.

```
  kvctl  (one RequestMeta per logical request; retries reuse it; follows "leader=" hints)
    |
    | gRPC Put / Get / Delete / CAS
    v
  raftkv.Server (leader)                       raftkv.Server (follower)   raftkv.Server (follower)
    propose: Dedup fast path                          ^                          ^
       -> rf.Start(entry) ---- append, persist        |                          |
             |                                        |                          |
             |  replicator[i] --- AppendEntries ------+-- persist, ack           |
             |  replicator[j] --- AppendEntries ---------------------------------+-- persist, ack
             |                                        |                          |
             |  majority matchIndex >= N  =>  commitIndex = N (leader only counts own-term entries)
             v                                        v                          v
          applier ---> applyCh ---> applyLoop      applier -> applyLoop      applier -> applyLoop
                                       |                |                        |
                            Machine.Apply(entry)   Machine.Apply(entry)   Machine.Apply(entry)
                                       |
                        waiters[index]: term matches?  -> reply     else -> ErrLeaderChanged
```

The guarantee, stated exactly:

- **Linearizable reads and writes.** Every operation, including `Get`, occupies one index in a
  single totally ordered log and takes effect at that point, which lies between the client's call
  and its return. Section 6.4 says what that means and how we check it.
- **Survives f failures of 2f+1 nodes.** Three nodes tolerate one crash or one partition; five
  tolerate two. Progress needs a majority connected to each other; safety needs nothing.
- **No lost acknowledged writes.** A `Put` that returned `ok` is in the logs of a majority, fsynced
  on each, and every future leader's log contains it (Leader Completeness, section 2). Compare
  Phase 1: "on this disk". Now: "on a majority of disks, and the majority elects the future".

What it costs: every write is a round trip to a majority plus an fsync on each side, and every read
is a write (section 5). Section 6.5 has the numbers.

## 2. Raft in one page

**Terms.** A monotonically increasing logical clock (`currentTerm`, `raft/raft.go:74`). Each term
has at most one leader or none (a split vote). Every RPC carries the sender's term; any node that
sees a higher term adopts it and becomes a follower (`becomeFollower`, `raft.go:255-265`, called
from every handler and every reply path). Terms are how a node that was asleep in a partition
discovers its view of the world is stale.

**Roles.** Follower (passive; answers RPCs; times out into candidate), candidate (asks for votes),
leader (the only node that appends to the log; sends heartbeats). `role` at `raft.go:87`.

**The two safety properties** everything else protects (`raft.go:12-16`):

- *Election Safety*: at most one leader per term. Guaranteed by "one vote per term, persisted".
- *State Machine Safety*: if any server has applied entry i, no server ever applies a different
  entry i. Guaranteed by Log Matching + Leader Completeness + the commit rule.

**Why majorities.** A majority of 2f+1 is f+1 nodes. Any two majorities share at least one node.
So two candidates in one term cannot both win (the shared node voted once), and a candidate that
wins must have talked to at least one node that holds every committed entry (committed means "on a
majority"). The election restriction (section 3.2) turns "talked to" into "has a log at least as
good as", which is why a new leader never needs to fetch anything from followers.

**Figure 2, as five groups, and where each lives:**

| Group | Rule | Code |
|---|---|---|
| State | persistent: `currentTerm`, `votedFor`, `log[]`; volatile: `commitIndex`, `lastApplied`; leader-only: `nextIndex[]`, `matchIndex[]` | `raft.go:72-95`, `persistentState` `raft.go:339-345` |
| AppendEntries receiver | the five numbered rules | `replication.go:187-274` |
| RequestVote receiver | reply false if stale term; grant if not yet voted and log up to date | `election.go:70-107` |
| All servers / followers / candidates | apply committed entries; higher term demotes; timeout starts election; majority wins | `applier` `raft.go:406`, `becomeFollower` `:255`, `ticker` `:313`, `startElection` `election.go:11` |
| Leaders | heartbeats; append and replicate; back up `nextIndex` on rejection; advance `commitIndex` only for current-term entries | `replicator` `replication.go:12`, `sendAppendEntries` `:56`, `advanceCommitIndex` `:154` |

Timing constraint (`api.go:139-145`): `broadcastTime << electionTimeout << MTBF`. Defaults for
tests are 50 ms heartbeats and 250-500 ms timeouts (`api.go:155-161`); the real binary uses
100 ms and 500-1000 ms (`cmd/raftkv/main.go:38-40`).

## 3. Walk the code in request order

### 3.1 A write

1. **`raftkv.propose`** (`kv/raftkv/server.go:242-285`). Dedup fast path under `s.mu`
   (`:245-250`): a retry of something already applied is answered from the session table on any
   replica, no consensus. Otherwise marshal and call `rf.Start`.
2. **`Start`** (`raft.go:180-196`). Not leader: return false, client goes elsewhere. Leader: append
   `Entry{Term: currentTerm}`, set own `matchIndex`, **persist before returning** (`:189`), nudge
   every replicator (`:194`). Read `api.go:17-21`: `ok=true` means "appended and will try", not
   "committed". The service registers a waiter keyed by index, remembering the term Start reported
   (`server.go:261-269`). If a waiter already sits at that index it is from an earlier term of
   leadership and is failed with `ErrLeaderChanged` (`:263-267`).
3. **`replicator`** (`replication.go:12-52`). One goroutine per peer, woken by `trigger[peer]`
   (buffered 1 so nudges coalesce, `raft.go:103-105, 292-297`) or by the heartbeat timer. A slow
   peer never delays a fast one. It decides under the lock whether the peer needs a snapshot
   (`:35`, section 3.3), releases the lock, and **fires the RPC in a fresh goroutine** (`:44-50`).
   The comment at `:38-43` says why: a replicator that waited for a reply on a network that delays
   replies by seconds would stop heartbeating, the peer would time out, and leadership would churn
   with nothing down. The price is several RPCs to one peer in flight at once, which the reply
   handlers must tolerate (step 4).
4. **`sendAppendEntries`** (`replication.go:56-139`). Take the lock, re-check we are still leader
   and still hold the entries (`:58-61`), build args from `nextIndex[peer]-1` (`:62-70`), **unlock**
   (`:71`), RPC (`:73`), relock. Then, in order: higher term demotes us (`:80-85`); a reply from a
   previous life is dropped (`:88-90`); success advances `matchIndex` monotonically (`:96-102`,
   replies may be reordered) and calls `advanceCommitIndex`. A rejection counts only if it answers
   the probe we most recently issued (`:107-112`; an older rejection would undo progress a newer
   reply made), then uses the follower's conflict hints to jump `nextIndex` back in one step instead
   of one per round trip (`:122-136`, hints defined `api.go:75-88`) and re-sends at once (`:138`).
5. **`HandleAppendEntries`** (`replication.go:187-274`), the follower. The five Figure 2 rules:
   - (1) `:193-195` stale term, reply false with our term.
   - Accept the leader: `becomeFollower`, record `leaderID` for client hints, **reset the election
     timer** (`:199-202`). Persist is deferred and happens only if term or log changed (`:205-209`),
     before the reply is returned.
   - Snapshot adjustment `:215-224`: if `prevLogIndex` is below our compaction point, everything
     that far is committed and therefore equal to the leader's; skip that prefix.
   - (2) `:227-241` no entry at `prevLogIndex`, or wrong term there. Fill in `ConflictTerm` and
     `ConflictIndex` (first index of that term) so the leader can jump.
   - (3)+(4) `:249-262` walk the entries; keep every entry that already matches; truncate from the
     *first conflict* and append the rest. The comment at `:243-248` is bug 3 in section 4.
   - (5) `:267-271` `commitIndex = min(leaderCommit, prevLogIndex + len(entries))`, and signal the
     applier. Bug 4 in section 4.
6. **`advanceCommitIndex`** (`replication.go:154-175`). Scan from `lastIndex` down; stop at the first
   entry whose term is not `currentTerm` (`:156-159`); for a current-term entry count `matchIndex >= n`
   including ourselves; majority sets `commitIndex`, wakes the applier, broadcasts so followers learn
   the new commit index with the next heartbeat (`:167-172`). Bug 1 in section 4.
7. **`applier`** (`raft.go:406-456`). The only goroutine that sends on `applyCh`. It waits on
   `applyCond` (`:409-410`), delivers a pending snapshot first (`:417-426`), then batches
   `(lastApplied, commitIndex]` into messages, advances `lastApplied` **before** releasing the lock
   (`:428-445`, so a concurrent commit cannot re-deliver), then sends with the lock released
   (`:448-454`). Bug 6 in section 4.
8. **`raftkv.applyLoop`** (`server.go:173-220`). Unmarshal, `Machine.Apply` (`:205`), record
   `lastApplied`, then the waiter check (`:208-215`): if a waiter sits at this index and its term equals
   `msg.CommandTerm`, the client gets the result; if the terms differ, a different leader's entry
   landed at our index and the client gets `ErrLeaderChanged`. Finally `maybeSnapshot` (`:216`).
9. **Reply.** `toStatus` (`server.go:349-365`) maps `ErrNotLeader`, `ErrLeaderChanged`, `ErrTimeout`
   to `Unavailable` with a `leader=<addr>` hint when known. `kvctl` treats `Unavailable` as "retry the
   same request elsewhere" (`cmd/kvctl/main.go:139-153, 177-213`).

On followers steps 7-8 run identically; there is just no waiter to wake. That is why every replica
ends up with the same `Machine`.

### 3.2 Elections

- **`ticker`** (`raft.go:313-328`) fires every 10 ms; a non-leader past its deadline starts an
  election. A leader never times out (`:323`): it sends heartbeats, it does not wait for them.
- **`startElection`** (`election.go:11-63`). Candidate, `currentTerm++`, vote for self, **persist**
  (`:16`), new random deadline, fan out `RequestVote` in goroutines without the lock. Each reply:
  higher term demotes (`:40-44`); if we are no longer a candidate in that term the reply is from a
  finished election and is ignored (`:47-49`, bug 5); count; `votes > n/2` calls `becomeLeader`
  (`:58-60`), which resets `nextIndex` optimistically and `matchIndex` pessimistically
  (`raft.go:270-277`) and broadcasts at once so followers stop timing out.
- **`HandleRequestVote`** (`election.go:70-107`). Stale term: false (`:75-78`). Higher term: become
  follower, forget our vote (`:79-84`). Then the **election restriction, section 5.4.1** (`:92-93`):
  the candidate's last entry must have a higher term than ours, or the same term and an index at
  least as large. Grant only if `votedFor` is empty or already this candidate (`:95`), persist
  (`:97`), and reset the timer **only on grant** (`:98-102`, bug 2).

Why the restriction is enough: a committed entry is on a majority; a winner got votes from a
majority; the overlap node refuses any candidate whose log is behind its own; so the winner's log is
at least as up to date as a node that holds the entry. "At least as up to date" implies "contains
it" because of Log Matching (same index and term means same prefix).

### 3.3 Snapshots

- **Indexing trick** (`raft.go:76-80`). `log[0]` is a sentinel whose `Term` is `lastIncludedTerm`
  and whose absolute index is `lastIncludedIndex`. Absolute index `i` lives at
  `log[i - lastIncludedIndex]` (`termAt`, `raft.go:231`). With no snapshot the sentinel is index 0
  term 0, which is exactly the paper's "log starts at 1". Every accessor at `raft.go:225-244` goes
  through this, so `PrevLogIndex == lastIncludedIndex` has a term to compare against without a
  special case.
- **`Snapshot`** (`snapshot.go:14-26`), called by the service with "state as of `index`". Rejects
  anything already compacted or beyond `commitIndex` (`:20-22`). `compactTo` (`:31-42`) copies the
  retained suffix into a fresh slice so the old backing array is collectable, installs the new
  sentinel, and `persist` writes state and snapshot together.
- The service triggers it from `maybeSnapshot` (`server.go:224-235`) when `persister.StateSize()`
  passes `MaxRaftState`, holding `s.mu` so the machine is quiescent while `Machine.Snapshot`
  (`machine.go:192-206`) serialises data **and sessions**. Sessions must be in the snapshot or a
  follower restored from it would re-apply a retry that a log-replaying follower would reject.
- **`sendInstallSnapshot`** (`snapshot.go:47-86`) mirrors `sendAppendEntries`: lock, build, unlock
  (`:60`), RPC, relock, term check (`:69-74`), stale check (`:75-77`), then `matchIndex`/`nextIndex`
  jump to `LastIncludedIndex` (`:79-84`).
- **`HandleInstallSnapshot`** (`snapshot.go:96-148`). Stale term: reply. Snapshot not past our
  `commitIndex`: ignore (`:114-119`, installing it could move the service backwards). If our log
  agrees at the boundary keep the tail (`:121-124`, Figure 13 rule 6), otherwise replace the log
  with just a sentinel (`:125-132`). Set `commitIndex = lastApplied = LastIncludedIndex`, persist,
  queue a snapshot `ApplyMsg` (`:139-144`). The applier delivers it before any later command, and
  `applyLoop` calls `Machine.Restore` (`server.go:184-192`) only if it moves `lastApplied` forward.
- After a restart `readPersist` (`raft.go:386-395`) queues the persisted snapshot the same way: the
  service came up empty and needs its state back before any command.

### 3.4 Persistence

**What is persisted** (`raft.go:334-345`): `currentTerm`, `votedFor`, `log`, `lastIncludedIndex`,
`lastIncludedTerm`, plus the snapshot bytes in the same `Save`. Each is a promise made to another
node: "I will not vote again in this term", "I hold this entry, count me toward its majority". A
promise forgotten across a crash breaks the overlap argument. So `persist` runs before every RPC
reply and before `Start` returns (`raft.go:347-350`; call sites: `election.go:16, 42, 83, 97`;
`replication.go:82, 205-209`; `snapshot.go:24, 71, 116, 135`; `raft.go:189`). A failed `Save`
panics (`raft.go:362-366`), the same reasoning as fsyncgate in Phase 1: a node that cannot persist
cannot make promises.

**Why `commitIndex` is not persisted.** It is derived state. After a restart it is set to
`lastIncludedIndex` (`raft.go:384-385`); the next heartbeat carries `LeaderCommit` and the follower
catches up (`replication.go:267-271`); a leader recomputes it from `matchIndex`. Re-applying
entries after a restart is harmless because the service restarted empty too, and `Machine.Apply`
is deterministic. Persisting it would save a little re-application, nothing more.

**Atomicity: write-temp-then-rename** (`filepersister.go:16-24, 86-127`). State and snapshot go in
one file because two files cannot be replaced together: a crash between the writes leaves a log
whose `lastIncludedIndex` describes a snapshot that is not there. One buffer with magic, lengths,
both payloads and a CRC32C (`:35, 90-96`), written to `raft.state.tmp`, then `os.Rename` over
`raft.state` (`:117`). Rename replaces the directory entry in one step; a reader sees the old file
or the new one. `OpenFilePersister` verifies the checksum (`:65-76`).

**The fsync-before-rename subtlety** (`filepersister.go:107-113`). Rename is a metadata operation;
the file's data is still in the page cache. If the rename reaches the journal before the data
reaches the device and power is lost, you have a correctly named file that is empty or torn. The
`f.Sync()` before the rename closes that window. The remaining gap is the directory fsync
(`:120-122`): the rename itself is durable only after the directory is synced, which Go cannot do
on Windows. Dan Luu's "Files are hard" from the Phase 1 reading list is about exactly this sequence.

**The cost** (`filepersister.go:26-33`): every `Save` rewrites the whole log and snapshot. That is
O(log size) per append and is why the throughput in section 6.5 is what it is. Exercise (b).

## 4. The seven bugs everyone writes

1. **Committing an old-term entry by counting replicas (Figure 8).** Symptom: an entry from term 2
   reaches a majority under a term-4 leader, is applied, then a term-5 leader that never saw it
   overwrites index i; State Machine Safety broken. Caught by `TestFigure8` (`raft_test.go:748`) and
   `TestFigure8Unreliable` (`:825`); the harness reports "inconsistent apply at index"
   (`harness_test.go:418-423`). Avoided at `replication.go:156-159`: the scan returns at the first
   entry not of `currentTerm`. Old entries commit indirectly when a current-term entry after them
   commits. This is also why a new leader in practice appends a no-op; we do not, so an old entry can
   stay uncommitted until the next client write (see interview question 2).
2. **Resetting the election timer on a rejected vote.** Symptom: a node with a stale log keeps
   starting elections it cannot win, every request resets everyone's timer, and no one else ever
   times out; liveness dies. Shows up as `TestRejoin` (`raft_test.go:408`) and
   `TestFigure8Unreliable` failing to converge inside their budget. Avoided at `election.go:98-102`:
   reset only when granting.
3. **Truncating on a matching prefix from a stale AppendEntries.** Symptom: a delayed
   AppendEntries with entries [1..5] arrives after one with [1..8] was accepted; a follower that
   truncates to the shorter list throws away 6..8, which the leader may have already counted toward
   a commit. Caught intermittently by `TestFigure8Unreliable` and `TestUnreliableChurn`
   (`raft_test.go:1014`), where `SetUnreliable` delays requests by up to 27 ms
   (`simnet.go:85-87`). With the fire-and-forget replicator this is not a corner case: two
   AppendEntries to one peer are routinely in flight together. Avoided at `replication.go:249-262`:
   truncate at the first *conflicting* entry, never on length.
4. **`commitIndex = min(leaderCommit, lastIndex())`.** Symptom: the follower holds extra entries
   beyond what this RPC vouched for (from a previous leader); it marks them committed and applies
   them; a later leader overwrites them. Same tests as bug 3 catch it; the harness invariant fails.
   Avoided at `replication.go:267-271`: `lastNew = prevLogIndex + len(entries)`.
5. **Acting on stale RPC replies.** Symptom: votes collected in term 5 make a node leader in term
   7 (two leaders in one term, `checkOneLeader` fails, `harness_test.go:486-489`); or a success reply
   from a previous leadership moves `matchIndex` for a different log. Caught by `TestManyElections`
   (`raft_test.go:83`) and `TestFigure8`. Avoided by rechecking role and term after every relock:
   `election.go:47-49`, `replication.go:88-90`, `snapshot.go:75-77`. The same-term variant: with
   several probes in flight, an old rejection arriving after a newer success would drag `nextIndex`
   back and re-send entries the follower already has; `replication.go:107-112` accepts a rejection
   only for the probe most recently issued, and `:96-102` never moves `matchIndex` backwards.
6. **Holding `rf.mu` across an RPC or an `applyCh` send.** Symptom: hangs, or leadership churn. A
   partitioned peer's RPC blocks up to 7 s with `SetLongDelays` (`simnet.go:101-103`); the service's
   `applyLoop` calls `rf.Snapshot` which needs `rf.mu` while the applier holds `rf.mu` waiting on
   `applyCh`. Caught by any test that partitions (`TestFailNoAgree`, `raft_test.go:278`) and by
   `TestSnapshotBasic` (`:1090`). The rule is `raft.go:20-24`; the pattern is unlock at
   `replication.go:71`, `snapshot.go:60`, and the applier's `raft.go:446-454`. The milder cousin is
   holding not the lock but the *replicator loop* across the RPC: no deadlock, but no heartbeats to
   that peer while a slow reply is outstanding, so it times out and calls an election
   (`replication.go:38-43`; `TestFigure8Unreliable` with `SetLongReordering` is where it shows).
7. **Replying before persisting.** Symptom: vote for A, crash, restart with `votedFor = -1`, vote
   for B; two leaders in one term. Or ack an entry, crash, lose it, the leader counted you. Caught by
   `TestPersist1/2/3` (`raft_test.go:631, 674, 717`) and `TestFigure8Unreliable`, because the harness
   restarts a node from a *copy* of what it had saved and nothing else (`harness_test.go:238-241`).
   Avoided by the call sites listed in section 3.4.

## 5. Why reads go through the log

`Get` is proposed like a write (`server.go:310-319`, `Op_OP_GET` applied at `machine.go:173-180`).
If a `Get` read local state, two things break. A follower may be behind: write on the leader, read on
a follower, do not see your own write. And a leader may be deposed without knowing: a partitioned old
leader keeps answering from a map the rest of the cluster has moved past. Both are stale reads;
both are linearizability violations that Porcupine finds in seconds if you try it (comment out the
`propose` in `Get`, return `LocalGet`, run `make test-lin`).

Putting the read in the log orders it after every write that committed before it and makes the
deposed-leader case impossible: the read commits only if this node is still leader of a majority.
Cost: one round of consensus, one fsync per node, per read.

**Cheaper, still linearizable** (paper section 8; exercise (a)):

- *ReadIndex.* The leader records `commitIndex` as the read index, sends one heartbeat round and
  confirms a majority still accepts it as leader, waits until `lastApplied >= readIndex`, then
  answers from local state. No log entry, no fsync, one round trip. A new leader must first commit an
  entry from its own term before it knows its `commitIndex` is current (the no-op again).
- *Lease reads.* The leader assumes it stays leader for `electionTimeoutMin` after a heartbeat round
  succeeded and answers reads locally inside that window. Zero round trips; correctness depends on
  bounded clock drift between nodes, which is a real assumption and a real risk.

**Retries and the leader change.** `ErrLeaderChanged` (`server.go:57-60`) means "proposed, not
known to be committed". The entry may have committed on the old leader just before it lost the
majority, or it may have been overwritten. The client cannot tell, so it retries **with the same
`RequestMeta`** (`cmd/kvctl/main.go:167-176`; test client `raftkv_test.go:106-109`). Three cases:

- The original never committed: the retry is logged, applied once.
- The original committed and the new leader has applied it: the fast path (`server.go:245-250`)
  answers from the session table, nothing logged.
- The original committed but is not yet applied on the node the retry hits: the retry is logged
  too. Now the log holds the same `(client, request)` twice. `Machine.Apply` (`machine.go:127-138`)
  checks `Dedup` before mutating, so the second copy returns the stored result without applying.

`TestRetryAcrossLeaderChangeAppliesOnce` (`raftkv_test.go:295-346`) forces this five rounds in a
row with a CAS on a counter. Even rounds (`:315-317`) partition the leader *before* the request: the
old leader still believes it leads, accepts the entry, can never commit it, the client times out,
retries on the new leader, and when the old leader rejoins its entry is overwritten (this is the
`ErrLeaderChanged` path, or a `CommitTimeout`). Odd rounds (`:318-324`) race the partition against
the proposal 0-4 ms later, so the first attempt may or may not have committed. Either way the client
must see `swapped=true` and the counter advanced by exactly one (`:337-345`). A CAS is the right op
for this test because a re-evaluated duplicate would fail (expected value no longer matches), which
is exactly the observable difference between "deduplicated" and "applied twice".

## 6. How we know it works

### 6.1 The simulated network

`raft/simnet` (`simnet.go:1-8`) is labrpc from 6.5840 in Go generics. Every RPC is gob-encoded in
and out, so a handler never aliases the caller's memory (`:4-6`; this catches a leader mutating an
`Entries` slice it already handed out). What it injects (`:179-192`): partitions per node in both
directions (`Disconnect`), dropped requests and replies at 10% each plus 0-27 ms request delay
(`SetUnreliable`), reply reordering by 200-2200 ms with probability 2/3 (`SetLongReordering`), and
calls to unreachable nodes that hang up to 7 s before failing (`SetLongDelays`). A node can be
unbound and rebound to simulate crash and restart; in-flight calls to the old instance return
`ok=false` (`:54-56`).

### 6.2 The harness invariants

`raft/harness_test.go` (`:3-7`) runs n instances over a simnet, consumes every `applyCh`, and checks
in the background (`checkLogs`, `:413-431`): each node applies index `lastApplied+1` next (in
order, no gaps), and no two nodes apply different commands at the same index (State Machine Safety).
`checkOneLeader` (`:486-489`) fails on two leaders in one term (Election Safety). `nCommitted`
(`:547-550`) counts how many nodes applied an index, crashed nodes included. `crash1` (`:238-241`)
restarts a node from a copy of its persister so an unpersisted promise is really lost. `cleanup`
fails the test on any recorded violation (`:149-152, 177`).

### 6.3 The test list

| Group | Tests (`raft/raft_test.go`) | What they force |
|---|---|---|
| 3A election | `TestInitialElection` :20, `TestReElection` :48, `TestManyElections` :83 | leader appears; survives leader partition; old leader rejoins as follower; many concurrent elections |
| 3B replication | `TestBasicAgree` :117, `TestRPCBytes` :138, `TestFollowerFailure` :169, `TestLeaderFailure` :210, `TestFailAgree` :248, `TestFailNoAgree` :278, `TestConcurrentStarts` :326, `TestRejoin` :408, `TestBackup` :445, `TestCount` :516 | agreement with failures; no commit without a majority; concurrent `Start`; fast backtracking over a long bad tail; RPC counts bounded |
| 3C persistence | `TestPersist1` :631, `TestPersist2` :674, `TestPersist3` :717, `TestFigure8` :748, `TestUnreliableAgree` :800, `TestFigure8Unreliable` :825, `TestReliableChurn` :1010, `TestUnreliableChurn` :1014 | crash and restart everywhere; Figure 8 for 1000 iterations; churn with random crashes, partitions and unreliable network |
| 3D snapshots | `TestSnapshotBasic` :1090, `TestSnapshotInstall` :1094, `TestSnapshotInstallUnreliable` :1098, `TestSnapshotInstallCrash` :1102, `TestSnapshotInstallUnCrash` :1106 (all via `snapCommon` :1027) | log stays bounded; a lagging or restarted node is caught up by `InstallSnapshot` then by entries |

Service level (`kv/raftkv/raftkv_test.go`): `TestBasicOps` :203, `TestLeaderFailover` :265,
`TestRetryAcrossLeaderChangeAppliesOnce` :295, `TestSnapshotAndCrashRecovery` :348 (full-cluster
restart from persisted state; dedup state survives), `TestLinearizability` :486.

### 6.4 Linearizability and the Porcupine model

**What linearizable means.** Each operation has a call time and a return time, and they overlap
across clients. A history is linearizable if there is a single sequential order of all operations,
consistent with a correct sequential KV store, in which each operation appears to take effect at
one instant between its call and its return. Two consequences people get wrong: operations that
do not overlap must appear in real-time order (once anyone sees a write, nobody can later miss it),
and operations that do overlap may go either way, but every client must agree on which way.

It is the strongest single-object model and the one clients implicitly assume: "I got `ok`, so the
next `Get` from anyone sees it". Sequential consistency drops the real-time requirement; eventual
consistency drops the single order. A stale read from a follower violates linearizability but not
sequential consistency, which is why the difference matters here.

**What the model checks** (`raftkv_test.go:417-484`). `Partition` splits the history by key
(`:418-429`); keys are independent so this is sound and makes the search tractable. State is the
key's current value or a sentinel for absent (`:430`). `Step` says whether an operation's observed
output is legal from a given state and what the state becomes (`:431-471`): a `get` must return the
current value; a `put` always applies; a `cas` must report `swapped` exactly when the state equals
`expected` and must report the pre-state as `current`. `TestLinearizability` runs 8 clients for 12 s
on 5 unreliable nodes while a nemesis partitions, heals, crashes and restarts (`:503-538`), then
calls `porcupine.CheckOperationsVerbose` (`:610`). An illegal history writes an HTML visualisation
(`:619-624`).

**Why failed operations are recorded as pending** (`:586-591`). A client that timed out does not
know whether its `put` happened. Recording it as "did not happen" would be wrong if it did;
dropping it would let the checker accept a history where a later `get` returns a value nobody
"wrote". So its return time is set past the end of the run, and `Step` accepts either outcome
(`:445-450, 453-458`), which lets Porcupine place it anywhere after its call, or effectively
nowhere. This is the standard treatment (Jepsen calls it `:info`) and it is the only honest one.

### 6.5 Measured results (mentor's runs, 2026-09-12)

| Setup | Result |
|---|---|
| Real 3-process cluster (`cmd/raftkv`, `FilePersister`), `kvctl bench -c 8` | 339 puts/s, p50 22 ms, p99 56 ms |
| Phase 1 single node, group commit, `-c 64` (from `docs/phase1.md`) | 11312 puts/s, p50 ~5 ms |
| `kill -9` the leader, next write | succeeded 66 ms wall time after a 2 s wait for the election |
| Restart the killed node | re-elected leader and served writes from its persisted state |

Why 33x slower than Phase 1: every append rewrites the entire persisted state and fsyncs it, on the
leader and on each follower, and the client waits for the leader's write plus the slowest of the two
follower round trips (each including an fsync). Group commit is absent too: `Start` persists per
call. Exercise (b) attacks the first problem; batching entries per `AppendEntries` is already there
(`entriesFrom(prev+1)` at `replication.go:68` sends everything outstanding, and the fire-and-forget
replicator pipelines several in flight), so a WAL-backed persister gets most of it back.

## 7. Exercises

**(a) ReadIndex reads.** Add `rf.ReadIndex() (index uint64, ok bool)`: leader records
`commitIndex`, runs one heartbeat round and counts acks under a per-call sequence number (a stale
ack must not count), returns `ok=false` if it lost a majority. In `raftkv.Get`, wait until
`s.lastApplied >= index` then answer from `s.m`. Handle the new-leader case: refuse until an entry of
the current term is committed. Then run `make test-lin` with reads at 50% and compare `bench` for a
read-heavy mix. Stretch: lease reads, and write down precisely which clock assumption you added.

**(b) A WAL-backed persister.** Replace `FilePersister.Save` with: append new entries to a
`kv/wal` log; write term/vote to a small separate record; rewrite the snapshot file only when the
snapshot changes; truncate the WAL on compaction. The `Persister` interface (`api.go:132-137`)
hands you the full state, so first change `persist` to tell the persister what changed. Keep the
atomicity contract: after a crash, state and snapshot must agree. Re-measure section 6.5 and expect
the p50 to approach two fsyncs (leader, then the faster follower).

**(c) Pre-vote or leader stickiness.** A node that was partitioned comes back with a high term
and deposes a healthy leader for nothing (`TestReElection` exercises this). Pre-vote: before
incrementing `currentTerm`, ask peers "would you vote for me?"; only proceed on a majority of yes.
Or the section 4.2.3 check: a follower that heard from a leader within `electionTimeoutMin` rejects
`RequestVote`. Measure disruptions per hour with a nemesis that partitions one node repeatedly.

**(d) Membership change, reading only.** Read paper section 6 (joint consensus) and the etcd
implementation's single-server change rule. Write one page: why a direct switch from C_old to C_new
can elect two leaders; why the joint configuration entry takes effect when appended, not when
committed; what a removed server does to the cluster's elections and how pre-vote fixes it.

**(e) Run it.** Three terminals with the commands in `cmd/raftkv/main.go:3-7`, `-v` on. Start
`kvctl -addr 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 bench -n 5000 -c 8`. Identify the leader
from the logs and `kill -9` it mid-run. Watch the followers' logs for `-> candidate`, `-> LEADER`,
watch the bench finish, and read `retries: N` (`cmd/kvctl/main.go:362`). Then `get` a few
`bench-*` keys: every acknowledged put is there. Restart the killed node and watch it install a
snapshot or catch up on entries depending on `-maxraftstate`.

## 8. Interview questions with model answers

1. **Explain Raft in two minutes.** A replicated log. Time is divided into terms; each term has at
   most one leader, elected by a majority vote, and only the leader appends. Clients send commands
   to the leader; it appends them, replicates to followers with `AppendEntries`, and once a majority
   has stored an entry it is committed and every node applies it in log order. Followers detect a
   dead leader by a randomized timeout and elect a new one; a candidate only gets votes from nodes
   whose logs are no newer than its own, so the new leader already has everything committed. Two
   invariants: one leader per term (votes are persisted, one per term) and no two nodes apply
   different entries at the same index (leader commits only its own-term entries by counting;
   followers truncate only on conflict). Snapshots bound the log.

2. **Why can't a leader commit old-term entries by counting?** Figure 8: entry from term 2 sits on
   S1, S2. S5 becomes leader in term 3 with a different entry at that index, crashes. S1 is leader in
   term 4, replicates the term-2 entry to S3, majority. If it commits it and dies, S5 can be elected
   (its last term 3 beats term 2 on S2, S3) and overwrite the "committed" entry. The fix: majority
   replication of a current-term entry implies (via Log Matching) majority replication of everything
   before it, and a current-term entry cannot lose an election to anything older. `advanceCommitIndex`
   at `replication.go:156-159`. Practical corollary: new leaders append a no-op to commit the old tail
   promptly; we rely on the next client write instead.

3. **How does Raft ensure a new leader has all committed entries?** Leader Completeness via the
   election restriction (`election.go:92-93`). Committed means on a majority; a winner heard from a
   majority; the overlap node holds the entry and grants votes only to logs at least as up to date as
   its own (last term, then last index). Log Matching turns "at least as up to date" into "contains
   every entry I have". So the leader never pulls from followers; data flows only leader to follower.

4. **What happens to a client request during a leader change?** `Start` returned an index; the
   leader may or may not have replicated it before losing the majority. Possibilities: never
   committed (a new leader overwrites the index; our waiter sees a different term at that index and
   returns `ErrLeaderChanged`, `server.go:208-215`); committed but ack lost. Either way the client
   retries with the same `RequestMeta`, and the session table in the state machine (`machine.go:100-138`)
   makes the retry return the original result rather than re-execute. A request without a request
   id cannot be retried safely, so `Meta == nil` is opt-out (`machine.go:98-99`).

5. **Raft vs Paxos vs Multi-Paxos.** Single-decree Paxos agrees on one value with two phases
   (prepare/promise, accept/accepted); leaderless, any proposer. Multi-Paxos amortises phase 1 by
   electing a distinguished proposer and running phase 2 per slot; it permits gaps and out-of-order
   acceptance, and the paper leaves leader election, log management and reconfiguration
   underspecified, which is why every implementation differs. Raft is Multi-Paxos with decisions
   made: a strong leader, contiguous logs, log-comparison elections, term numbers instead of
   ballots, and a specified membership-change protocol. Same fault model and message complexity in
   the steady state; Raft trades some flexibility (no out-of-order commit, follower logs never
   ahead of the leader) for a proof and an implementation you can test against Figure 2.

6. **What does linearizable mean and how would you test it?** Each operation appears to take
   effect atomically at some instant between its call and its return, and there is one total order
   consistent with all of them. Test: record a concurrent history with call and return timestamps
   from many clients under fault injection, then run a checker (Porcupine, Knossos) that searches for
   a valid sequential witness against a model of the object; treat timed-out operations as pending
   with an open return time. Partition the history by key for tractability. A single stale read
   shows up as "no linearization exists".

7. **Why is the election timeout randomized?** Split votes. With equal timeouts, followers that
   lose a leader time out together, all become candidates, split the vote, all time out again, and
   the cycle repeats. Random timeouts in [T, 2T] make one node usually go first and win before the
   others start (`raft.go:303-309`). The spread must exceed the broadcast time or randomization does
   nothing. The remaining issue is the disruptive rejoining node, which needs pre-vote (exercise c).

8. **How do snapshots interact with far-behind followers?** The leader's `nextIndex` for the
   follower walks back until it is below `lastIncludedIndex`; the entries are gone, so the leader
   ships the snapshot (`replication.go:35-47`, `snapshot.go:47`). The follower installs it, sets
   `commitIndex = lastApplied = lastIncludedIndex`, and hands it to the service which replaces its
   state (`server.go:184-192`). Then normal replication resumes from `lastIncludedIndex+1`. Subtleties:
   ignore a snapshot that does not go past your `commitIndex`, keep the log tail if it agrees at the
   boundary, and never deliver a snapshot to the service after commands past it. Production systems
   chunk the transfer and throttle it so it does not starve heartbeats.

9. **What would you change for a WAN deployment?** Timeouts: heartbeats and election timeouts
   scale with cross-region RTT (tens to hundreds of ms), so failover takes seconds. Batching and
   pipelining `AppendEntries` to keep the link full. Pre-vote and leader stickiness because
   partitions are common. Learners or non-voting replicas per region for local reads, with
   follower reads via ReadIndex to keep them linearizable. Lease reads become risky with clock
   skew across regions unless you have bounded-error clocks. Snapshot transfer needs chunking and
   rate limiting. And consider whether one Raft group spanning regions is right at all versus
   per-region groups with a different consistency story between them.

10. **What is the cost of a linearizable read and how do you reduce it?** In this code: a log
    entry, a majority round trip, an fsync on every node. ReadIndex removes the entry and the
    fsync: one heartbeat round to confirm leadership, then a local read once `lastApplied` reaches
    the recorded `commitIndex`. Lease reads remove the round trip too, at the price of a clock
    assumption. Follower reads: a follower asks the leader for a ReadIndex and serves locally once it
    has applied that far, spreading read load without giving up linearizability. If the application
    can accept staleness, say so explicitly and bound it; do not call it linearizable.

## 9. Your notes

Answer these in your own words, here, before starting Phase 3.

1. Draw the Figure 8 sequence with five nodes and show, step by step, which line of
   `advanceCommitIndex` prevents the bad commit and which entry eventually commits it indirectly.
2. List every place `persist` is called and, for each, state the promise that would be broken if
   the node crashed after replying but before persisting.
3. A delayed `AppendEntries` from the current leader arrives carrying entries [4..6] after the
   follower already accepted [4..9] and set `commitIndex = 8`. Walk through `HandleAppendEntries`
   rules 3, 4 and 5 and say what the log and `commitIndex` are afterwards, and why.
4. Write the exact sequence of events in `TestRetryAcrossLeaderChangeAppliesOnce` for the case
   where the first attempt committed on the old leader but the reply was lost. Which code path
   answers the retry, and why does the answer say `swapped=true`?
5. In one paragraph: what did Phase 1's single-node design get you for free that you had to earn
   again here (determinism, dedup, "log then apply"), and what is the first new failure Phase 3's
   sharding introduces that a single Raft group could never have?

## Reading

- Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm" (extended version).
  Read Figure 2 with `replication.go` and `election.go` open; section 5.4 twice; section 8 before
  exercise (a).
- Ongaro's thesis, chapter 4 (membership changes) and section 6.4 (read-only queries), for
  exercises (a), (c) and (d).
- "Students' Guide to Raft" (Kleppmann's students, MIT 6.824 TA blog): the seven bugs above are its
  checklist with our line numbers.
- Herlihy and Wing, "Linearizability: A Correctness Condition for Concurrent Objects", sections 1-2,
  and the Porcupine README for how the checker searches.
- Jepsen's "Consistency Models" page for where linearizable sits relative to what Phase 3 and 5
  will offer.
