"""Tiny Python client for the kv.v1 gRPC service (see proto/kv/v1/kv.proto).

Run the demo against a local kvserver:
    .venv/Scripts/python.exe py/client.py [host:port]
"""
from __future__ import annotations

import os
import sys
import uuid

import grpc

# The generated kv_pb2_grpc.py does `from kv.v1 import kv_pb2`, a bare import
# rooted at the `protoc -I proto` directory. That does not resolve once the
# stubs live under the dsys_kv package. Rather than editing generated code,
# put py/dsys_kv on sys.path so the bare import resolves. (py/ itself is added
# too so `import client` works from anywhere.)
_HERE = os.path.dirname(os.path.abspath(__file__))
_PKG_DIR = os.path.join(_HERE, "dsys_kv")
for _p in (_HERE, _PKG_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dsys_kv.kv.v1 import kv_pb2, kv_pb2_grpc  # noqa: E402


class KVClient:
    """Thin wrapper around kv_pb2_grpc.KVStub.

    Every mutating call (put / delete / cas) is stamped with a RequestMeta:
    a random client_id chosen once per KVClient, and a request_id that grows
    by one per *logical* request. The server deduplicates on
    (client_id, request_id), which is what turns at-least-once delivery into
    exactly-once semantics.

    IMPORTANT: a retry of a failed call MUST reuse the same RequestMeta. Never
    mint a new request_id for a retry -- the server would see a brand-new
    request and could apply the mutation twice. Each mutating method accepts an
    optional `meta=` for exactly this purpose: capture the meta from the failed
    attempt (via `next_meta()`) and pass it back in on retry.
    """

    def __init__(
        self,
        target: str = "localhost:7001",
        channel: grpc.Channel | None = None,
        timeout: float | None = 5.0,
    ) -> None:
        self._channel = channel or grpc.insecure_channel(target)
        self._stub = kv_pb2_grpc.KVStub(self._channel)
        self.client_id = uuid.uuid4().hex
        self._request_id = 0
        self.timeout = timeout

    # -- request identity ---------------------------------------------------

    def next_meta(self) -> kv_pb2.RequestMeta:
        """Allocate a RequestMeta for a NEW logical request."""
        self._request_id += 1
        return kv_pb2.RequestMeta(client_id=self.client_id, request_id=self._request_id)

    # -- RPCs ---------------------------------------------------------------

    def put(self, key: str, value: bytes, meta: kv_pb2.RequestMeta | None = None) -> None:
        req = kv_pb2.PutRequest(meta=meta or self.next_meta(), key=key, value=value)
        self._stub.Put(req, timeout=self.timeout)

    def get(self, key: str) -> bytes | None:
        """Return the value, or None if the key is absent."""
        resp = self._stub.Get(kv_pb2.GetRequest(key=key), timeout=self.timeout)
        return resp.value if resp.found else None

    def delete(self, key: str, meta: kv_pb2.RequestMeta | None = None) -> bool:
        """Return True if the key existed."""
        req = kv_pb2.DeleteRequest(meta=meta or self.next_meta(), key=key)
        return self._stub.Delete(req, timeout=self.timeout).existed

    def cas(
        self,
        key: str,
        value: bytes,
        expected: bytes = b"",
        expect_absent: bool = False,
        meta: kv_pb2.RequestMeta | None = None,
    ) -> tuple[bool, bytes]:
        """Compare-and-swap. Returns (swapped, current_value)."""
        req = kv_pb2.CASRequest(
            meta=meta or self.next_meta(),
            key=key,
            expected=expected,
            expect_absent=expect_absent,
            value=value,
        )
        resp = self._stub.CompareAndSwap(req, timeout=self.timeout)
        return resp.swapped, resp.current

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> "KVClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:7001"
    key, value = "demo/hello", b"world"
    try:
        with KVClient(target) as kv:
            print(f"client_id={kv.client_id} target={target}")
            kv.put(key, value)
            print(f"put    {key!r} = {value!r}")
            print(f"get    {key!r} -> {kv.get(key)!r}")
            print(f"delete {key!r} -> existed={kv.delete(key)}")
            print(f"get    {key!r} -> {kv.get(key)!r}")
    except grpc.RpcError as e:
        print(f"RPC failed: {e.code().name}: {e.details()} (is kvserver running on {target}?)")
        sys.exit(1)
