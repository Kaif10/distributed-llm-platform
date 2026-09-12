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
make build             # bin/kvserver.exe, bin/kvctl.exe
```

```bash
source ./env.sh        # Git Bash equivalent
```

Run a server and talk to it:

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
kv/server   gRPC front end
cmd/        kvserver, kvctl
py/         Python client + generated stubs
docs/       per-phase notes, exercises, interview prep
```

## Status

- [x] Phase 0: toolchain
- [x] Phase 1: durable single-node KV (WAL, CAS, idempotent retries, group commit)
- [ ] Phase 2: Raft
- [ ] Phase 3: sharding
- [ ] Phase 4: scheduler
- [ ] Phase 5: LLM layer
- [ ] Phase 6: chaos + observability
- [ ] Phase 7: write-up
