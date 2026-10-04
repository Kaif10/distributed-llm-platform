"""KVClient: thread-safe request ids, leader hints, safe retries.

Run: .venv/Scripts/python.exe -m unittest discover -s py/tests -v
"""
from __future__ import annotations

import os
import socket
import sys
import threading
import unittest
from concurrent import futures

import grpc

_PY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PY not in sys.path:
    sys.path.insert(0, _PY)

from client import KVClient  # noqa: E402
from dsys_kv.kv.v1 import kv_pb2, kv_pb2_grpc  # noqa: E402


def dead_addr() -> str:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"127.0.0.1:{port}"


class FakeLeader(kv_pb2_grpc.KVServicer):
    """Dedups on (client_id, request_id) like the real store. drop_replies
    makes the first N Puts apply but answer UNAVAILABLE (a lost reply)."""

    def __init__(self, drop_replies: int = 0) -> None:
        self.data: dict[str, bytes] = {}
        self.seen: set[tuple[str, int]] = set()
        self.metas: list[tuple[str, int]] = []
        self.applied = 0
        self.drop_replies = drop_replies
        self.lock = threading.Lock()

    def Put(self, request, context):
        m = (request.meta.client_id, request.meta.request_id)
        with self.lock:
            self.metas.append(m)
            if m not in self.seen:
                self.seen.add(m)
                self.data[request.key] = request.value
                self.applied += 1
            if self.drop_replies > 0:
                self.drop_replies -= 1
                context.abort(grpc.StatusCode.UNAVAILABLE, "leader changed")
        return kv_pb2.PutResponse()

    def Get(self, request, context):
        with self.lock:
            v = self.data.get(request.key)
        return kv_pb2.GetResponse(value=v or b"", found=v is not None)


class FakeFollower(kv_pb2_grpc.KVServicer):
    def __init__(self, leader: str) -> None:
        self.leader = leader
        self.calls = 0

    def _redirect(self, context):
        self.calls += 1
        context.abort(grpc.StatusCode.UNAVAILABLE, f"not leader leader={self.leader}")

    def Put(self, request, context):
        self._redirect(context)

    def Get(self, request, context):
        self._redirect(context)


class KVClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.servers: list[grpc.Server] = []

    def tearDown(self) -> None:
        for s in self.servers:
            s.stop(None)

    def start(self, servicer) -> str:
        srv = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        kv_pb2_grpc.add_KVServicer_to_server(servicer, srv)
        port = srv.add_insecure_port("127.0.0.1:0")
        srv.start()
        self.servers.append(srv)
        return f"127.0.0.1:{port}"

    def test_request_ids_unique_across_threads(self) -> None:
        old = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)  # make interleavings likely
        try:
            kv = KVClient(dead_addr())
            ids: list[int] = []
            lock = threading.Lock()

            def work() -> None:
                mine = [kv.next_meta().request_id for _ in range(20000)]
                with lock:
                    ids.extend(mine)

            ts = [threading.Thread(target=work) for _ in range(8)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            kv.close()
        finally:
            sys.setswitchinterval(old)
        self.assertEqual(len(set(ids)), len(ids), "duplicate request ids handed out")
        self.assertEqual(max(ids), len(ids))

    def test_follows_leader_hint(self) -> None:
        leader = FakeLeader()
        laddr = self.start(leader)
        follower = FakeFollower(laddr)
        faddr = self.start(follower)
        # Only the follower is configured: the leader is reachable only by hint.
        with KVClient(faddr, timeout=2) as kv:
            kv.put("k", b"v")
            self.assertEqual(kv.get("k"), b"v")
        self.assertEqual(leader.data, {"k": b"v"})
        self.assertGreaterEqual(follower.calls, 1)

    def test_skips_dead_replica(self) -> None:
        leader = FakeLeader()
        laddr = self.start(leader)
        with KVClient([dead_addr(), laddr], timeout=2) as kv:
            kv.put("k", b"v")
        self.assertEqual(leader.applied, 1)

    def test_default_path_retries_with_same_meta(self) -> None:
        # The reply to the first attempt is lost after the write applied.
        leader = FakeLeader(drop_replies=1)
        laddr = self.start(leader)
        with KVClient(laddr, timeout=2) as kv:
            kv.put("k", b"v")  # no meta= given: the default path
        self.assertEqual(len(leader.metas), 2)
        self.assertEqual(leader.metas[0], leader.metas[1], "retry minted a new request id")
        self.assertEqual(leader.applied, 1)

    def test_exhausted_error_carries_meta_for_manual_retry(self) -> None:
        with KVClient(dead_addr(), timeout=0.5, retry_timeout=1.0) as kv:
            with self.assertRaises(grpc.RpcError) as cm:
                kv.put("k", b"v")
            meta = cm.exception.meta
            self.assertEqual((meta.client_id, meta.request_id), (kv.client_id, 1))
            # refused connection or per-attempt timeout, depending on gRPC's
            # reconnect backoff; both are the retryable kind
            self.assertIn(cm.exception.code(), (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED))

    def test_manual_retry_with_returned_meta_is_deduplicated(self) -> None:
        leader = FakeLeader()
        laddr = self.start(leader)
        with KVClient(laddr, timeout=2) as kv:
            meta = kv.next_meta()
            kv.put("k", b"v", meta=meta)
            kv.put("k", b"v", meta=meta)  # e.g. after a KVRequestError
        self.assertEqual(leader.applied, 1)


if __name__ == "__main__":
    unittest.main()
