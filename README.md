# dsys — a distributed LLM inference platform, built from the log up

A learning project that grows, phase by phase, from a single-node write-ahead
log into a Raft-replicated, sharded control plane serving LLM inference.
See [ROADMAP.md](ROADMAP.md) for the plan and `docs/phaseN.md` for each phase.

## Quickstart

All tooling is project-local under `.tools/` (Go, protoc, make, gcc) and
`.venv/` (Python). Nothing is installed system-wide.

```powershell
. .\env.ps1            # PowerShell: activate Go/protoc/make
make test              # unit tests
make test-crash        # kill-mid-write durability test, 200 iterations
make build             # bin/kvserver.exe, bin/raftkv.exe, bin/kvctl.exe
make test-raft         # Raft suite: elections, replication, persistence, snapshots
make test-lin          # Porcupine linearizability check under partitions and crashes
```

```bash
source ./env.sh        # Git Bash equivalent
```

Three-node replicated cluster (one terminal each), then talk to any node:

```powershell
.\bin\raftkv.exe -id 0 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
.\bin\raftkv.exe -id 1 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
.\bin\raftkv.exe -id 2 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
.\bin\kvctl.exe -addr 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put k v
.\bin\kvctl.exe -addr 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 bench -n 500 -c 8
```

Kill the leader's terminal mid-bench and watch `retries:` climb while every put still lands exactly once.

Sharded cluster (Phase 3): a controller cluster plus two independent replica
groups, each group a full Raft cluster of its own.

```powershell
# controller (3 terminals)
.\bin\shardctrl.exe -id 0 -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
.\bin\shardctrl.exe -id 1 -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
.\bin\shardctrl.exe -id 2 -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003

# group 100 (3 terminals)
.\bin\shardkv.exe -gid 100 -id 0 -peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
.\bin\shardkv.exe -gid 100 -id 1 -peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
.\bin\shardkv.exe -gid 100 -id 2 -peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003

# register the group and talk to the cluster
.\bin\shardctl.exe -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 join 100=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
.\bin\kvctl.exe -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 put k v
```

Start a second group (`-gid 200`, ports 9101-9103) and `shardctl join 200=...`
while writes are in flight: the controller rebalances shards onto it live,
each shard's data and dedup sessions migrate, and every key keeps resolving
through `kvctl` with no manual intervention.

Single-node server (Phase 1):

```powershell
.\bin\kvserver.exe -addr :7001 -data .\data\node1
.\bin\kvctl.exe put greeting hello
.\bin\kvctl.exe get greeting
.\bin\kvctl.exe bench -n 2000 -c 8
```

Python client (same proto, generated stubs in `py/`):

```powershell
.\.venv\Scripts\Activate.ps1
python py\client.py
```

## Layout

```
proto/      gRPC + log entry definitions (source of truth for Go and Python)
gen/        generated Go code (make proto)
kv/wal      append-only write-ahead log with crash recovery
kv/store    durable KV: log-then-apply state machine
kv/server   gRPC front end (single node)
raft/       Raft: election, replication, persistence, snapshots (no libraries)
raft/simnet simulated network: partitions, drops, delays, reordering
raft/grpctransport  Raft RPCs over gRPC for real multi-process clusters
kv/raftkv   replicated KV: Machine driven by Raft, Porcupine linearizability test
shard/      the shared shard-key mapping (NShards, Key2Shard)
shardctrl/  shard controller: its own tiny Raft-replicated service assigning
            shards to groups, with a deterministic rebalancing algorithm
shardkv/    one shard-owning replica group: Machine driven by Raft, plus a
            3-kind log (client ops, config changes, migrations) and a poll
            loop that pulls shards in from their previous owner
shardkv/grpctransport  cross-group shard migration RPC, and a Controller
            adapter around the shardctrl client
cmd/        kvserver (single node), raftkv (replica), shardctrl, shardkv,
            kvctl (leader- and shard-aware client), shardctl (admin CLI)
py/         Python client + generated stubs
docs/       per-phase notes, exercises, interview prep
```

## Status

- [x] Phase 0: toolchain
- [x] Phase 1: durable single-node KV (WAL, CAS, idempotent retries, group commit)
- [x] Phase 2: Raft consensus, replicated KV, linearizability-checked
- [x] Phase 3: sharding, live migration, deterministic rebalancing
- [ ] Phase 4: scheduler
- [ ] Phase 5: LLM layer
- [ ] Phase 6: chaos + observability
- [ ] Phase 7: write-up
