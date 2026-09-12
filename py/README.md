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

Start the Go server in one terminal (e.g. `go run ./cmd/kvserver`, listening on `:7001`), then:

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

Every mutating call carries a `RequestMeta(client_id, request_id)`. A retry of a
failed call must reuse the same meta so the server can deduplicate it.
