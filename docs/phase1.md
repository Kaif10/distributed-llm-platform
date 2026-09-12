# Phase 1 — Single-node durable KV store

Code: `kv/wal`, `kv/store`, `kv/server`, `cmd/kvserver`, `cmd/kvctl`, `proto/kv/v1/kv.proto`.
Run: `make test`, `make test-crash`, `make test-exercises`.

## 1. What Phase 1 builds and why it matters

An in-memory `map[string][]byte` fronted by gRPC, where every mutation goes through one pattern
(`kv/store/store.go:4-18`):

1. Serialize the command as a `LogEntry`.
2. Append it to the log and wait until it is durable (fsync).
3. Apply it to the in-memory map.
4. Reply to the client.

Recovery is "replay the log through step 3". That is the whole store.

Why it matters: this is *exactly* the shape of a Raft state machine. In Phase 2 the `LogEntry`
messages, `apply`, and replay are unchanged. Only step 2 changes: "fsync locally" becomes "a
majority of replicas have durably logged it". If `apply` is not deterministic here, Raft replicas
diverge there. If dedup is not rebuilt from the log here, a new leader double-applies retries there.
Every Phase 1 mistake is a Phase 2 mistake with five nodes and a partition on top.

## 2. Concepts

### 2.1 Write-ahead logging and the durability invariant

**What.** Before mutating state you cannot afford to lose, write a description of the mutation to an
append-only file and force it to disk. Only then mutate, only then ack. The invariant everything
else rests on is `kv/wal/wal.go:10`:

> if Append returned nil, the record survives any crash after that point.

**Failure prevented.** Acknowledged-then-lost writes. A client that got `ok` from `Put` has made
decisions based on that ack; if a crash erases it, the system lied.

**Why append-only.** Appends confine crash damage to the tail of the file. In-place updates can
leave a record half-old, half-new anywhere, and you cannot tell which half is which. Append-only
plus checksums means "everything before the first bad record is trusted" (`wal.go:24-28`).

**Interview:** *"A client got a success response from your write API. The server then lost power.
What can the client assume?"* Model answer: only what the server promised at ack time. Ack after
`write()` but before `fsync()`: nothing, the data was in the page cache. Ack after a successful
fsync of the log: the record is on the device and will be replayed; the in-memory state is a pure
function of the log, so it is reconstructible. Expect the follow-up "and if the file was just
created?" (directory fsync, below).

### 2.2 fsync

**What it actually does.** `write()` copies bytes into the kernel's page cache and returns. The disk
has not been touched. `fsync(fd)` (Go: `f.Sync()`, on Windows `FlushFileBuffers`) blocks until the
dirty pages for that file *and* the file's metadata (notably its new length) have been handed to the
device, and asks the device to flush its own volatile write cache. That round trip is the durability
boundary. `wal.go:142-150` is the only line in the project that makes anything durable.

**Why it is slow.** ~0.5-2 ms on a consumer SSD, ~10 ms on spinning disk, worse on some cloud block
devices: 3-4 orders of magnitude slower than the memcpy in `write()`. Because `Store.commit` holds
one mutex across append+fsync+apply (`store.go:44-54`), throughput is bounded at ~1/fsync-latency
writes per second regardless of cores or clients. Measure it: `kvctl bench -c 1` with and without
`-nosync`.

**Group commit.** N writers arrive during one in-flight fsync: write all N records, one fsync, wake
all N. Latency per writer stays ~1 fsync; throughput becomes N per fsync. Postgres, InnoDB and etcd
all do it. It is Exercise 3.

**Two things to know for interviews.**
- New files: fsyncing `kv.wal` persists its contents, not the directory entry that names it. On
  Linux a crash right after creation can leave a fully fsynced file that does not appear in the
  directory. Production WALs fsync the parent directory after create/rename. `wal.Open` does not; it
  is a known gap, fix it if you want.
- fsyncgate (Postgres, 2018): on Linux, if fsync fails once (EIO), the kernel may drop the dirty
  pages and *clear* the error, so a retried fsync returns success while the data is gone. The only
  correct reaction to an fsync error is to treat the file as unknown: crash and recover from the
  log, do not retry. `wal.Append` returns the error; note the store keeps running afterwards.
  Production code panics here.

**Interview:** *"Why not just call fsync less often?"* Model answer: you can, but then you must not
ack until the fsync that covers the write has completed. Delaying the ack is group commit (safe).
Acking early is `-nosync` (loses acknowledged writes on power loss). The knob is *how many writes
share an fsync*, never *whether the ack waits*.

### 2.3 Torn writes: truncate the tail, error on the middle

**What.** `Append` issues one `WriteAt` of header+payload (`wal.go:134-139`). Multi-sector writes
are not atomic on most filesystems, so a crash can leave the file ending in a partial header, a
header with a partial payload, or a full-length record of garbage (filesystems that extend the
length before the data lands).

**Recovery rules** are all in `scan` (`wal.go:178-238`). Read them in this order:
- `wal.go:190-195` — clean EOF, or a partial header at EOF: stop, this is the end.
- `wal.go:202-209` — length larger than `maxPayloadSize`: the header is garbage. Tolerated only if
  nothing follows.
- `wal.go:212-216` — payload shorter than length: torn tail, stop.
- `wal.go:219-229` — checksum mismatch. If nothing follows, torn tail, stop. If more bytes follow,
  return `ErrCorrupt`.

Then `Open` truncates to the last good offset and fsyncs (`wal.go:94-105`) so the next append lands
on a clean boundary.

**Why the asymmetry.** `w.off` advances only after a successful append (`wal.go:151`). So if a
complete record exists at offset Y > X, the append at X returned nil before the append at Y started.
Record X was acknowledged. A bad checksum there is not a crash artifact; it is real data loss (disk
bit rot, a misdirected write, someone editing the file). Silently skipping it would hide the loss.
`TestMidFileCorruptionIsAnError` (`wal_test.go:131-156`) pins this; `TestTornTail`
(`wal_test.go:83-127`) cuts the last record at every byte boundary and requires exactly n-1
survivors.

**Interview:** *"Your log has a checksum failure in the middle. Do you skip it?"* Model answer: no.
A torn write can only be the last thing written, so anything with valid data after it was
acknowledged. Fail loudly and let an operator (in Phase 2, a replica with an intact copy) repair it.
Skipping silently loses a committed write and replays later entries against the wrong state.

### 2.4 Checksums (CRC32C)

**What.** A 32-bit CRC with the Castagnoli polynomial over the payload (`wal.go:59, 136`). Chosen
because it is hardware-accelerated (SSE4.2 `crc32` instruction) and has good burst-error detection;
it is what ext4, Btrfs, iSCSI, and RocksDB use.

**Protects against:** bit flips, partial writes, garbage from pre-allocated space, a length field
that points at the wrong bytes (the payload then fails its CRC).

**Does not protect against:**
- Tampering. CRC is linear and forgeable; it is a corruption detector, not an integrity guarantee
  (that needs an HMAC).
- A valid record *in the wrong place*. The CRC covers only the payload (`wal.go:20`), not an offset
  or sequence number, so a misdirected write of a real record elsewhere would verify. Fix: checksum
  a monotonic sequence number (or the previous record's CRC) along with the payload.
- A whole acknowledged record vanishing from the tail. Nothing in the format says how many records
  there should be. A sequence number fixes this too; the crash test gets it by putting one in the
  payload (`wal_test.go:186-210`).
- A corrupted-but-plausible length. The header is not checksummed; it is caught only indirectly when
  the misread payload fails its CRC.

**Interview:** *"Why a checksum per record rather than one for the whole file?"* Model answer:
recovery needs to know *where* the valid prefix ends. A per-file checksum says the file is bad, not
which record; per-record checksums let recovery keep everything before the damage and decide whether
the damage is at the tail.

### 2.5 Determinism and "log order must equal apply order"

**What.** `apply` (`store.go:159-178`) must be a pure function of (current state, entry). No clock
reads, no randomness, no map iteration order, no reading anything outside `s.data` and `e`. Replay
calls the same function with the same entries in the same order and must reach the same state
(`store.go:12-15`).

**Failure prevented.** Live state and replayed state disagreeing. The subtle version is ordering,
not purity: if goroutine A appends `Put(k,1)`, goroutine B appends `Put(k,2)`, and then B's apply
runs before A's, the live map says `k=1` but a replay says `k=2`. The store prevents this by holding
`mu` across both the append and the apply (`store.go:44-54`, `store.go:146-156`).
`TestConcurrentWritersReplayConsistently` (`store_test.go:74-103`) is the breaking test: 8
goroutines hammer one key, then the test compares the live value with the value after reopen.

The WAL keeps its own invariant for the same reason: `Append` holds a mutex across the disk write so
file order equals call order (`wal.go:68-71`).

**Interview:** *"You want to add a TTL to keys. Where do you read the clock?"* Model answer: in the
caller, never in `apply`. Stamp the entry with an absolute expiry before logging so `apply` sees a
constant; if `apply` called `time.Now()`, a replay hours later would expire keys differently than
the live run. Same rule in Raft: the leader decides, the log records the decision, followers apply
the decision.

### 2.6 Idempotency and "exactly-once"

**What.** A client that times out cannot tell "never arrived" from "applied, reply lost" (DDIA ch.
8), so it must retry. Retrying a non-idempotent operation (`Delete` returning `existed`, any CAS, an
increment) changes the answer or the state. Exactly-once delivery is impossible; what you can build
is:

> at-least-once delivery (client retries) + server-side deduplication = effectively-once.

**RequestMeta** (`kv.proto:23-26`): `(client_id, request_id)`. The client picks a random `client_id`
at start and increments `request_id` per *logical* request; a retry reuses the pair
(`cmd/kvctl/main.go:30-41`). `LogEntry.meta` (`kv.proto:84`) carries it into the log. The server
remembers the last request applied per client plus its `result` (`store.go:133-137`) and answers a
duplicate from that table without logging or applying.

**Why the table must be rebuilt from the log.** If the server restarts between the original and the
retry, an empty table re-applies the retry. Since `Meta` is inside the logged entry and `Open`
replays through `apply` (`store.go:72-79`), populating the table inside `apply` rebuilds it for
free. `TestDedupSurvivesReopen` (`store_exercises_test.go:119-138`) is the breaking test: a retry
after restart must not delete a value another client wrote in between. In Raft the same argument
gives dedup that survives failover: every replica rebuilds the same table from the same log.

**The content-hash trap.** "Why not dedupe by hashing the request body?" Because two distinct
requests can be byte-identical and both intended: `Put(counter, "1")` twice with a `Delete` between,
or two clients incrementing the same key. The server cannot tell "same intent, retried" from "same
bytes, new intent". Identity must be assigned by the party that knows the intent: the client. That
is why `RequestMeta` exists.

**Interview:** *"What does exactly-once mean in your system, precisely?"* Model answer: each
`(client_id, request_id)` is applied to the state machine at most once, and every retry observes the
result of that one application. It is a property of the state machine plus a client contract (unique
ids, reuse on retry, one outstanding request per client); delivery is still at-least-once. Kafka's
idempotent producer and Stripe's `Idempotency-Key` are the same construction.

### 2.7 Compare-and-swap

**What.** `CompareAndSwap(key, expected, expectAbsent, value)` writes `value` only if the current
value is `expected` (or the key is absent when `expectAbsent`), and reports what it saw
(`kv.proto:51-64`). The compare and the write happen atomically inside `apply`, under the lock, in
log order.

**Failure prevented.** Lost updates from read-then-write races. Two clients both `Get` `ctr=5`, both
`Put 6`. With CAS the second one fails and retries against `6`.

**What it builds.** Lock: `CAS(lock, absent, owner)`. Lease: value holds `(owner, expiry, version)`,
renewal is `CAS(expected=old, value=new)`. Counter: `Get`, `CAS(old, old+1)`, loop. Fencing token: a
version every successful CAS increments, so downstream services can reject a stale holder (Phase 4).
Token bucket (Phase 5): a CAS loop on `(tokens, last_refill)`. Get/Put alone cannot build any of
these because "check, then write" is not atomic.

**Interview:** *"Design a distributed lock with a KV store."* Model answer: CAS on an absent key to
acquire; store owner and expiry so a crashed holder does not hold it forever; release with
`CAS(expected=mine, ...)` so you cannot release someone else's lock after your lease expired; hand
out an increasing token per acquisition and require every protected write to carry it, because the
lock alone cannot stop a paused holder (GC, network) from acting after expiry.

## 3. Walkthrough

Read in this order; each layer only depends on the ones before it.

1. `proto/kv/v1/kv.proto` — service messages carry `RequestMeta`; the storage `LogEntry` is a
   superset of every request. Note `OP_UNSPECIFIED = 0` and that `apply` panics on unknown ops
   (`store.go:173-176`).
2. `kv/wal/wal.go` — format (`:16-18`), invariant (`:10`), `scan` (`:178-238`). Then `wal_test.go`:
   `TestTornTail` and `TestCrashRecovery` (`:212-277`) *prove* the invariant rather than assert it.
3. `kv/store/store.go` — the four-step pattern, the mutex comment (`:44-54`), `commit` (`:140-157`),
   `apply` (`:159-178`). `Get` returns a copy (`:102-103`) and never touches the log.
4. `kv/server/server.go` — deliberately thin. Internalize the `toStatus` comment (`:69-71`): the
   gRPC code tells the client whether a retry is safe, and a retry must reuse the same
   `RequestMeta`.
5. `cmd/kvserver/main.go` — `-nosync`, and the recovery log line (`:33`): recovery is linear in log
   size, which is why Phase 2 needs snapshots. `cmd/kvctl/main.go` — where `RequestMeta` is minted
   (`:30-41`) and `bench` (`:110-153`). `kvctl` does not retry; dedup only matters once something
   does.

## 4. Exercises

Run with `make test-exercises` (`go test -race -tags exercises ./kv/store/`). When all pass, delete
the `//go:build exercises` line at the top of `kv/store/store_exercises_test.go` so they run under
`make test` forever.

### Exercise 1 — CompareAndSwap

Implement `Store.CompareAndSwap` (`store.go:126-128`) and the `Op_OP_CAS` case in `apply`
(`store.go:169-172`). Tests: `TestCASBasic`, `TestCASReplay`.

Spec:
- `commit` a `LogEntry{Op: OP_CAS, Key, Value, Expected, ExpectAbsent, Meta}` and return `r.swapped,
  r.current, err`.
- In `apply`: let `cur, ok := s.data[key]`. Match if `(expectAbsent && !ok)` or `(!expectAbsent &&
  ok && bytes.Equal(cur, expected))`. On match, set the value. Return `swapped` and `current` = the
  value that was there *before* the swap (nil if absent). Return a copy of `current` on the no-swap
  path; the live slice must not escape the lock.

Hints:
- The decision belongs in `apply`, not in `CompareAndSwap`. Replay runs `apply` with the state as it
  was at that point in the log, so the decision reproduces itself. If you decide before logging and
  log only winners, a failed CAS leaves no trace and Exercise 2 cannot return the original
  `swapped=false` to a retry after restart (it would be re-evaluated against newer state and might
  succeed).
- A failed CAS may be logged. It costs an fsync and is harmless: replaying it fails again.
  `TestCASReplay` accepts either design as long as the state matches.
- `apply` may look at nothing except `s.data` and `e`.

### Exercise 2 — Idempotent retries

Add a per-client dedup table and use it in `commit` (`store.go:149-151`) and `apply`. Tests:
`TestDuplicateDeleteReturnsOriginalResult`, `TestDuplicateCASIsNotReapplied`,
`TestDedupSurvivesReopen`.

Spec:
- State: `clients map[string]clientState` with `clientState{lastRequestID uint64; lastResult
  result}`. Guarded by `mu` like everything else.
- In `commit`, under the lock, before `Append`: if `e.Meta != nil` and
  `clients[e.Meta.ClientId].lastRequestID == e.Meta.RequestId`, return `lastResult` without logging
  or applying.
- In `apply`, after computing `result`: if `e.Meta != nil`, store `{e.Meta.RequestId, result}` for
  that client. Doing it in `apply` means replay rebuilds the table automatically (`store.go:72-79`),
  which is what `TestDedupSurvivesReopen` demands.
- `Meta == nil` means "no dedup requested": always log and apply. The existing tests and internal
  callers pass nil.
- `RequestId < lastRequestID` (an old request arriving late) cannot happen under the assumption
  below. Pick a behaviour and document it; returning `lastResult` or an error are both defensible,
  silently applying it is not.

The one-outstanding-request assumption: the table remembers *one* request per client, so it is
correct only if a client never sends N+1 before N is answered. MIT 6.5840 Lab 4 and the Raft paper
(section 8) assume the same, for the same reason: the table is O(clients) not O(requests) and
"duplicate" is one integer comparison. Relaxing it means a bounded sliding window of ids per client.
The remaining leak, clients that vanish forever, needs session expiry, which needs leases (Phase 4).

### Exercise 3 (stretch) — Group commit

Goal: N concurrent `commit`s share one fsync. Measure before and after with `kvctl bench -n 2000 -c
1` and `-c 32` against a `kvserver` *without* `-nosync`. With `-c 1` nothing should change; with `-c
32` throughput should rise several-fold while p50 latency stays near one fsync.

Shape that preserves every invariant:
1. Under `mu`: write the record bytes (no fsync), push the entry and a reply channel onto a pending
   batch, release `mu`. This fixes log order.
2. One committer goroutine: take the whole pending batch, fsync once, then under `mu` apply the
   entries *in log order* and send each result to its channel.
3. `commit` blocks on its channel. The ack still waits for durability and apply is still in log
   order. A duplicate of an entry still in flight must wait for it, not be re-logged.

Never apply before the fsync completes: a `Get` could observe a value a crash then erases. Step 2 is
literally Raft's apply loop: one goroutine draining committed entries in index order.

## 5. Break it

`make test-crash` runs `TestCrashRecovery` 200 times (`wal_test.go:212-277`; the roadmap bar is
1000, pass `-crash-iterations=1000` once). A helper process appends sequence numbers and prints each
only after `Append` returned nil. The parent kills it at a random moment, reopens the log, and
checks: the log is a contiguous prefix `0,1,2,...`; every printed number is present; the log never
shrank across iterations.

What it proves: the tail-repair rules in `scan` are right for every byte offset a kill can land on,
`off` advances only after success, and recovery leaves the log appendable.

What it cannot prove:
- **fsync correctness.** `kill -9` does not lose the page cache; the kernel still writes the dirty
  pages out. This test passes with `NoSync: true`. Only power loss, a kernel panic, or a VM hard
  reset exercises fsync.
- **One disk, one filesystem.** No misdirected writes, no bit rot, no filesystem that reorders the
  length update relative to the data (`wal.go:220-221` handles that by reasoning, not by test).
- **Directory durability.** The file already exists when the helper runs.

See it yourself:

1. The cost. `kvserver -data data/a`, then `kvctl bench -n 500 -c 1`. Restart with `-nosync`, run
   again. The p50 gap is one fsync; write the numbers down.
2. The loss. A process kill will *not* show it (above); do not be fooled by a clean recovery. To
   actually lose writes, run `kvserver -nosync` inside a Linux VM (Hyper-V or VirtualBox, data on
   the VM disk), run `kvctl bench -n 5000` inside it, and hard power off the VM ("Turn off", not
   "Shut down") during or seconds after the bench. Restart and compare the `recovered N keys` log
   line with what the bench acknowledged. Repeat without `-nosync`: every acknowledged put is there.
   (`cmd/kvserver/main.go:25` is honest about what the flag does.)

## 6. Exit criteria

- [ ] `make test` green with `-race`.
- [ ] `make test-crash` green; one run at `-crash-iterations=1000`.
- [ ] Exercises 1 and 2 green; build tag removed; `make test` still green.
- [ ] `kvctl put/get/del/cas` round trip against a running `kvserver`, then restart the server and
      `get` again.
- [ ] Bench numbers recorded (sync vs nosync, and if you did Exercise 3, before/after).
- [ ] Section 7 answered in your own words.

## Reading

- DDIA ch. 3, "Data Structures That Power Your Database": hash indexes, the append-only log, crash
  recovery and partially written records. Read it with `wal.go` open.
- Dan Luu, "Files are hard" (danluu.com/file-consistency): what fsync, rename and directory sync
  actually guarantee, and how often real software gets it wrong.
- fsyncgate: the PostgreSQL wiki page "Fsync Errors", or the ATC'20 paper "Can Applications Recover
  from fsync Failures?" for per-filesystem behaviour.
- Raft paper section 5.3 (Log Matching) as a preview: the log you just built, plus a term per entry,
  plus a rule for when a prefix is safe to apply.

## 7. Your notes

Answer these in your own words, here, before starting Phase 2.

1. State the durability invariant of the WAL and list every line of code it depends on. Which of
   those lines would you have to change on macOS, and why?
2. Explain to a non-engineer why a bad checksum at the end of the log is fine and a bad checksum in
   the middle is an emergency.
3. Write out the interleaving that `TestConcurrentWritersReplayConsistently` would catch if `commit`
   released the lock between `Append` and `apply`.
4. A client sends `Delete(a)` with `(c1, 7)`, the reply is lost, the server restarts, client `c2`
   writes `a`, then `c1` retries. Walk through exactly what each layer does with your Exercise 2
   implementation and what `c1` receives.
5. In one paragraph: what stays the same and what changes when step 2 of the pattern becomes
   "replicate to a majority" instead of "fsync locally"? What new failure appears that Phase 1 could
   never have?
