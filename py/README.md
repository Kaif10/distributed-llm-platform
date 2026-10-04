# Python side

A tiny gRPC client for the `kv.v1` service, plus generated stubs.

## Setup

From the repo root (`python -m venv .venv` once, then):

```powershell
# PowerShell
.\.venv\Scripts\Activate.ps1
```

```bash
# Git Bash
source .venv/Scripts/activate
```

```
pip install -r py/requirements.txt
```

## Regenerate stubs

After editing `proto/kv/v1/kv.proto`, run from the repo root:

```
python -m grpc_tools.protoc -I proto --python_out=py/dsys_kv --grpc_python_out=py/dsys_kv --pyi_out=py/dsys_kv proto/kv/v1/kv.proto
```

Output lands in `py/dsys_kv/kv/v1/` (`kv_pb2.py`, `kv_pb2.pyi`, `kv_pb2_grpc.py`).
Generated files are not edited by hand. The grpc stub imports `from kv.v1 import kv_pb2`,
so `client.py` adds `py/dsys_kv` to `sys.path` before importing.

## Run the demo

Start the Go server in one terminal (e.g. `go run ./cmd/kvserver`, listening on `127.0.0.1:7001`), then:

```
python py/client.py            # defaults to localhost:7001
python py/client.py host:port  # or point it elsewhere
```

It puts `demo/hello`, gets it, deletes it, and prints each result.

## Use from your own code

```python
import sys; sys.path.insert(0, "py")
from client import KVClient

with KVClient("localhost:7001") as kv:
    kv.put("k", b"v")
    print(kv.get("k"))
```

`KVClient` also takes a raftkv cluster's client addresses (`"a:7001,b:7002,c:7003"` or a
list): it follows "not leader" hints and moves past dead replicas on its own.

Every mutating call carries a `RequestMeta(client_id, request_id)`. A retry of a
failed call must reuse the same meta so the server can deduplicate it. The client's own
retries already do; if a call still fails it raises `KVRequestError` (a `grpc.RpcError`)
whose `.meta` you pass back as `meta=` to retry that same request later.

Tests (in-process gRPC servers): `.venv/Scripts/python.exe -m unittest discover -s py/tests -v`

## Scheduler worker (`sched.v1`)

`py/worker.py` is a pull-model worker for the job queue in `proto/sched/v1/sched.proto`,
with the same contract as the Go library in `sched/worker`: claim a job under a lease,
heartbeat every `lease/3` carrying the fencing token `gen`, and treat `FAILED_PRECONDITION`
from Heartbeat/Complete/Fail as "the job is no longer mine" (log, count as fenced, move on;
never retry, never re-claim). Read `sched/api.go`'s package doc for why.

Regenerate stubs after editing the proto (output lands in `py/dsys_sched/sched/v1/`):

```
python -m grpc_tools.protoc -I proto --python_out=py/dsys_sched --grpc_python_out=py/dsys_sched --pyi_out=py/dsys_sched proto/sched/v1/sched.proto
```

Run the demo worker against a scheduler (defaults to `localhost:9001`; `LEASE_MS` overrides the lease):

```
python py/worker.py host:port
```

Use it from your own code. A handler gets the job and a `threading.Event` that is set when
the worker has been fenced off the job; check it between expensive steps. Handlers may run
more than once for the same job, so side effects must be idempotent on `job.id`.

```python
import sys; sys.path.insert(0, "py")
from worker import SchedWorker

def handle(job, fenced) -> bytes:
    return job.payload.upper()

SchedWorker("localhost:9001", handle, name="py-1", lease_ms=5000).run()
```

## Inference worker and gateway client (`infer.v1`, `gateway.v1`)

Phase 5 adds two services. `infer.v1.Inference` is what the gateway speaks to a model host;
`gateway.v1.Gateway` is what clients speak to the gateway (rate limit -> semantic cache ->
prefix-aware routing -> hedging). Read `proto/infer/v1/infer.proto`, `proto/gateway/v1/gateway.proto`
and the package doc in `gateway/api.go`.

Regenerate stubs after editing either proto (output lands in `py/dsys_infer/infer/v1/` and
`py/dsys_gateway/gateway/v1/`):

```
python -m grpc_tools.protoc -I proto --python_out=py/dsys_infer --grpc_python_out=py/dsys_infer --pyi_out=py/dsys_infer proto/infer/v1/infer.proto
python -m grpc_tools.protoc -I proto --python_out=py/dsys_gateway --grpc_python_out=py/dsys_gateway --pyi_out=py/dsys_gateway proto/gateway/v1/gateway.proto
```

### `py/infer_worker.py`

An inference worker: serves `Inference` and keeps itself registered with the gateway under a
lease (`RegisterWorker` every `lease_ms/3`; a worker that dies drops out of routing when its
lease lapses, the Phase 4 idea reused for service discovery). Cancellation is honoured on every
token: a cancelled stream stops generating and bumps the `cancelled` counter.

```
python py/infer_worker.py --addr 127.0.0.1:7600 --gateway 127.0.0.1:7500[,127.0.0.1:7501]
```

| flag | default | meaning |
|---|---|---|
| `--addr` / `--advertise` | `127.0.0.1:7600` | listen address (advertised as-is unless `--advertise`) |
| `--gateway` | `127.0.0.1:7500` | comma-separated gateways; on failure the next one is tried |
| `--lease-ms` | 3000 | registration lease; renewed every third of it |
| `--concurrency` | 4 | gRPC thread pool |
| `--backend` | `mock` | `mock` or `hf` |
| `--kv-cache-blocks` | 512 | mock: LRU size of 64-char prefix blocks |
| `--token-ms` | 12 | mock: inter-token interval |
| `--stall-prob` / `--stall-ms` | 0.0 / 400 | mock: tail stall added to prefill (what hedging is for) |
| `--stats-every` | `10s` | log claimed/completed/cancelled/prefix-hit-rate |

The mock's latency model matches the Go mock in `infer/mock` exactly, because `cmd/llmbench`
mixes both behind one gateway: `prefill_ms = 20 + 0.25 * (len(prompt) - cached_prefix_chars)`
where the cached prefix is the longest run of leading 64-char blocks this worker has prefilled
before (an LRU of cumulative block hashes stands in for the attention KV cache). Same prompt
twice on the same worker: second time `prefix_cache_hit=true` and a much lower `prefill_ms`.
That is the effect prefix-aware routing in the gateway is meant to produce fleet-wide.

`--backend hf --model <name>` streams greedy tokens from a HuggingFace causal LM. It needs
`pip install torch transformers` in the venv (not in `requirements.txt`) and is untested here.

### `py/gateway_client.py`

```python
import sys; sys.path.insert(0, "py")
from gateway_client import GatewayClient, RateLimited

with GatewayClient("localhost:7500") as gw:
    try:
        for tok in gw.generate("hello", tenant="t0", max_tokens=16):
            print(tok.text, end="")   # tok.index == 0 carries cached/worker/hedged/ttft_ms/...
    except RateLimited as e:
        print("over quota:", e.hint)
```

`generate()` yields tokens; breaking out of the loop cancels the stream, which cancels the
worker. `start()` returns the raw call for explicit `.cancel()`. The demo
(`python py/gateway_client.py [host:port]`) streams one prompt, prints the first token's
metadata, then starts a second stream, reads three tokens, cancels it and shows the gateway's
`cancelled` counter.
