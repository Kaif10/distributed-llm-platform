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
cmd/        kvserver (single node), raftkv (replica), kvctl (leader-aware client)
py/         Python client + generated stubs
docs/       per-phase notes, exercises, interview prep
```

## Status

- [x] Phase 0: toolchain
- [x] Phase 1: durable single-node KV (WAL, CAS, idempotent retries, group commit)
- [x] Phase 2: Raft consensus, replicated KV, linearizability-checked
- [ ] Phase 3: sharding
- [ ] Phase 4: scheduler
- [ ] Phase 5: LLM layer
- [ ] Phase 6: chaos + observability
- [ ] Phase 7: write-up
