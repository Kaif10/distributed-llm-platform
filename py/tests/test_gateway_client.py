"""GatewayClient failover against in-process gRPC servers.

Run: .venv/Scripts/python.exe -m unittest discover -s py/tests -v
"""
from __future__ import annotations

import os
import socket
import sys
import unittest
from concurrent import futures

import grpc

_PY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PY not in sys.path:
    sys.path.insert(0, _PY)

from gateway_client import GatewayClient  # noqa: E402
from dsys_gateway.gateway.v1 import gateway_pb2, gateway_pb2_grpc  # noqa: E402


def dead_addr() -> str:
    """An address nothing listens on: bind an ephemeral port, then free it."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"127.0.0.1:{port}"


class FakeGateway(gateway_pb2_grpc.GatewayServicer):
    def __init__(self, name: str, n: int = 3, fail_after: int | None = None) -> None:
        self.name, self.n, self.fail_after = name, n, fail_after
        self.calls = 0

    def Generate(self, request, context):
        self.calls += 1
        for i in range(self.n):
            if self.fail_after is not None and i == self.fail_after:
                context.abort(grpc.StatusCode.UNAVAILABLE, "gateway went away mid-stream")
            yield gateway_pb2.Token(text=f"{self.name}-{i}", index=i, worker=self.name)


def serve(servicer) -> tuple[grpc.Server, str]:
    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    gateway_pb2_grpc.add_GatewayServicer_to_server(servicer, srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()
    return srv, f"127.0.0.1:{port}"


class GatewayFailoverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.servers: list[grpc.Server] = []

    def tearDown(self) -> None:
        for s in self.servers:
            s.stop(None)

    def start(self, servicer) -> str:
        srv, addr = serve(servicer)
        self.servers.append(srv)
        return addr

    def test_dead_first_gateway_fails_over(self) -> None:
        live = FakeGateway("live")
        addr = self.start(live)
        with GatewayClient([dead_addr(), addr]) as gw:
            texts = [t.text for t in gw.generate("hi", timeout=5)]
        self.assertEqual(texts, ["live-0", "live-1", "live-2"])
        self.assertEqual(live.calls, 1)

    def test_start_handle_fails_over_and_cancels(self) -> None:
        addr = self.start(FakeGateway("live", n=50))
        with GatewayClient([dead_addr(), addr]) as gw:
            call = gw.start("hi", timeout=5)
            first = next(iter(call))
            self.assertEqual(first.text, "live-0")
            call.cancel()
            with self.assertRaises(grpc.RpcError) as cm:
                for _ in call:
                    pass
            self.assertEqual(cm.exception.code(), grpc.StatusCode.CANCELLED)
            self.assertIs(gw.last_call, call)

    def test_no_failover_after_tokens_were_delivered(self) -> None:
        # Once the caller has seen tokens, silently restarting the stream on
        # another gateway would duplicate output; the error must surface.
        broken = FakeGateway("broken", n=3, fail_after=1)
        spare = FakeGateway("spare")
        a, b = self.start(broken), self.start(spare)
        with GatewayClient([a, b]) as gw:
            got = []
            with self.assertRaises(grpc.RpcError) as cm:
                for t in gw.generate("hi", timeout=5):
                    got.append(t.text)
        self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(got, ["broken-0"])
        self.assertEqual(spare.calls, 0)

    def test_all_dead_raises_connection_error(self) -> None:
        with GatewayClient([dead_addr(), dead_addr()]) as gw:
            with self.assertRaises(ConnectionError):
                list(gw.generate("hi", timeout=5))


if __name__ == "__main__":
    unittest.main()
