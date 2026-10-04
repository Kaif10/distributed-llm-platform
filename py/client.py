"""Tiny Python client for the kv.v1 gRPC service (see proto/kv/v1/kv.proto).

Run the demo against a local kvserver or a raftkv cluster's client addresses:
    .venv/Scripts/python.exe py/client.py [host:port[,host:port...]]
"""
from __future__ import annotations

import os
import re
import sys
import threading
import time
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

# Same contract as the Go client (kv/client): a follower answers UNAVAILABLE
# "not leader leader=<addr>"; UNAVAILABLE / DEADLINE_EXCEEDED mean "retry,
# possibly elsewhere, with the SAME RequestMeta".
_LEADER_HINT = re.compile(r"leader=([^\s,;)\]]+)")
_RETRYABLE = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
_BACKOFF_MIN, _BACKOFF_MAX = 0.01, 0.2


class KVRequestError(grpc.RpcError):
    """A request that failed for good: the last attempt's error, plus the
    RequestMeta every attempt used (None for reads). If the outcome matters,
    retry later with `meta=err.meta`: the server deduplicates on it, so the
    mutation applies at most once even if one of the failed attempts did
    reach it."""

    def __init__(self, last: grpc.RpcError, meta: kv_pb2.RequestMeta | None) -> None:
        super().__init__(f"{last.code().name}: {last.details()}")
        self.last = last
        self.meta = meta

    def code(self) -> grpc.StatusCode:
        return self.last.code()

    def details(self) -> str:
        return self.last.details()


class KVClient:
    """Wrapper around kv_pb2_grpc.KVStub for one server or one Raft group.

    `target` is one address or several (a list, or a comma-separated
    string) of the SAME cluster's client addresses. A request goes to the
    replica that last worked; on UNAVAILABLE or a per-attempt timeout it
    follows the follower's leader hint if there is one, otherwise tries the
    next address, with backoff, until `retry_timeout` seconds have passed.

    Every mutating call (put / delete / cas) is stamped with a RequestMeta:
    a random client_id chosen once per KVClient, and a request_id that grows
    by one per *logical* request. The server deduplicates on
    (client_id, request_id), which is what turns at-least-once delivery into
    exactly-once semantics.

    Retries reuse the request's meta: that is what makes them safe, and the
    default path (no `meta=` argument) does it for you. If a call still
    fails it raises KVRequestError (a grpc.RpcError) whose `.meta` is the
    meta every attempt used; to retry that request later, pass it back as
    `meta=err.meta`. Never retry by calling again without it: that mints a
    new request_id, and the server would treat it as a brand-new request
    that can apply a second time.

    Thread-safe. The store's dedup table assumes at most one outstanding
    mutation per client_id at a time, so give each thread that writes
    concurrently its own KVClient (as the Go client's Session() does).
    """

    def __init__(
        self,
        target: str | list[str] = "localhost:7001",
        channel: grpc.Channel | None = None,
        timeout: float | None = 5.0,
        retry_timeout: float = 10.0,
    ) -> None:
        if isinstance(target, str):
            target = [a.strip() for a in target.split(",") if a.strip()]
        self.targets = list(target)
        if not self.targets:
            raise ValueError("KVClient needs at least one target address")
        self.timeout = timeout  # per attempt
        self.retry_timeout = retry_timeout  # per logical request, retries included
        self.client_id = uuid.uuid4().hex
        self._lock = threading.Lock()  # guards everything below
        self._request_id = 0
        self._cur = 0
        self._channels: dict[str, grpc.Channel] = {}
        if channel is not None:  # caller-supplied channel: one fixed target
            self._channels[self.targets[0]] = channel
            self.targets = self.targets[:1]

    # -- request identity ---------------------------------------------------

    def next_meta(self) -> kv_pb2.RequestMeta:
        """Allocate a RequestMeta for a NEW logical request."""
        with self._lock:
            self._request_id += 1
            rid = self._request_id
        return kv_pb2.RequestMeta(client_id=self.client_id, request_id=rid)

    # -- retry machinery ----------------------------------------------------

    def _pick(self) -> tuple[int, kv_pb2_grpc.KVStub]:
        with self._lock:
            i = self._cur
            addr = self.targets[i]
            ch = self._channels.get(addr)
            if ch is None:
                ch = self._channels[addr] = grpc.insecure_channel(addr)
        return i, kv_pb2_grpc.KVStub(ch)

    def _advance(self, failed: int, hint: str | None) -> None:
        with self._lock:
            if hint:
                if hint not in self.targets:
                    self.targets.append(hint)
                self._cur = self.targets.index(hint)
            elif self._cur == failed:  # another thread may have moved on already
                self._cur = (failed + 1) % len(self.targets)

    def _call(self, method: str, req, meta: kv_pb2.RequestMeta | None):
        deadline = time.monotonic() + self.retry_timeout
        backoff = _BACKOFF_MIN
        followed_hint = False
        while True:
            i, stub = self._pick()
            try:
                return getattr(stub, method)(req, timeout=self.timeout)
            except grpc.RpcError as e:
                if e.code() not in _RETRYABLE:
                    raise
                m = _LEADER_HINT.search(e.details() or "")
                self._advance(i, m.group(1) if m else None)
                if time.monotonic() + backoff > deadline:
                    raise KVRequestError(e, meta) from e
                # Jump straight to the first hint (fresh information); back
                # off otherwise, including on later hints, so two replicas
                # with stale hints cannot ping-pong us in a tight loop.
                if m and not followed_hint:
                    followed_hint = True
                    continue
                time.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)

    # -- RPCs ---------------------------------------------------------------

    def put(self, key: str, value: bytes, meta: kv_pb2.RequestMeta | None = None) -> None:
        meta = meta or self.next_meta()
        self._call("Put", kv_pb2.PutRequest(meta=meta, key=key, value=value), meta)

    def get(self, key: str) -> bytes | None:
        """Return the value, or None if the key is absent."""
        resp = self._call("Get", kv_pb2.GetRequest(key=key), None)
        return resp.value if resp.found else None

    def delete(self, key: str, meta: kv_pb2.RequestMeta | None = None) -> bool:
        """Return True if the key existed."""
        meta = meta or self.next_meta()
        return self._call("Delete", kv_pb2.DeleteRequest(meta=meta, key=key), meta).existed

    def cas(
        self,
        key: str,
        value: bytes,
        expected: bytes = b"",
        expect_absent: bool = False,
        meta: kv_pb2.RequestMeta | None = None,
    ) -> tuple[bool, bytes]:
        """Compare-and-swap. Returns (swapped, current_value)."""
        meta = meta or self.next_meta()
        req = kv_pb2.CASRequest(
            meta=meta,
            key=key,
            expected=expected,
            expect_absent=expect_absent,
            value=value,
        )
        resp = self._call("CompareAndSwap", req, meta)
        return resp.swapped, resp.current

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            channels, self._channels = list(self._channels.values()), {}
        for ch in channels:
            ch.close()

    def __enter__(self) -> "KVClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:7001"
    key, value = "demo/hello", b"world"
    try:
        with KVClient(target) as kv:
            print(f"client_id={kv.client_id} target={kv.targets}")
            kv.put(key, value)
            print(f"put    {key!r} = {value!r}")
            print(f"get    {key!r} -> {kv.get(key)!r}")
            print(f"delete {key!r} -> existed={kv.delete(key)}")
            print(f"get    {key!r} -> {kv.get(key)!r}")
    except grpc.RpcError as e:
        print(f"RPC failed: {e.code().name}: {e.details()} (is kvserver/raftkv running on {target}?)")
        sys.exit(1)
