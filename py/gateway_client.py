"""Python client for the gateway.v1 service (proto/gateway/v1/gateway.proto).

GatewayClient.generate() yields Token messages as they stream in. The FIRST
token carries the routing metadata (cached / worker / hedged / hedge_won /
ttft_ms / prefix_cache_hit / prefill_ms); every token carries text.

Two things the demo shows on purpose:

  * Cancellation. `call.cancel()` on the streaming call propagates through
    the gateway to the worker, which stops generating (its `cancelled`
    counter goes up). Stop reading and walk away is NOT enough: an
    unconsumed stream holds the worker until its buffer fills.
  * Rate limiting. RESOURCE_EXHAUSTED means the tenant's token bucket is
    empty; the details string is the gateway's retry hint. Back off, do not
    hammer.

Run the demo against a gateway (defaults to localhost:7500):
    .venv/Scripts/python.exe py/gateway_client.py [host:port[,host:port]]
"""
from __future__ import annotations

import os
import sys
import threading
import time
from typing import Iterator

import grpc

_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.join(_HERE, "dsys_gateway")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dsys_gateway.gateway.v1 import gateway_pb2, gateway_pb2_grpc  # noqa: E402


class RateLimited(Exception):
    """The tenant is over quota. `hint` is the gateway's retry advice."""

    def __init__(self, hint: str) -> None:
        super().__init__(hint)
        self.hint = hint


class GatewayClient:
    """Thin wrapper over GatewayStub across one or more gateway replicas.

    Gateways are stateless (shared state is in the KV), so a stream that
    fails with UNAVAILABLE before its first token moves to the next address
    (see _FailoverStream)."""

    def __init__(self, addrs: str | list[str] = "localhost:7500") -> None:
        if isinstance(addrs, str):
            addrs = [a.strip() for a in addrs.split(",") if a.strip()]
        self.addrs = list(addrs)
        self._channels = [grpc.insecure_channel(a) for a in self.addrs]
        self._stubs = [gateway_pb2_grpc.GatewayStub(c) for c in self._channels]
        self._idx = 0
        self.last_call: grpc.Call | None = None  # the most recent stream, for .cancel()

    def _stub(self) -> gateway_pb2_grpc.GatewayStub:
        return self._stubs[self._idx % len(self._stubs)]

    def start(self, prompt: str, tenant: str = "default", max_tokens: int = 32, no_cache: bool = False,
              timeout: float | None = 60.0) -> "_FailoverStream":
        """Open the stream and return a call-like handle (iterable of Token,
        with .cancel()). Use this when you need the handle; generate() is the
        convenient form.

        A server-streaming call never fails when it is created: a dead
        gateway only shows up as UNAVAILABLE on the first read. So failover
        lives in the returned handle's iterator, not here."""
        req = gateway_pb2.GenerateRequest(tenant=tenant, prompt=prompt, max_tokens=max_tokens, no_cache=no_cache)
        call = _FailoverStream(self, req, timeout)
        self.last_call = call
        return call

    def generate(self, prompt: str, tenant: str = "default", max_tokens: int = 32,
                 no_cache: bool = False, timeout: float | None = 60.0) -> Iterator[gateway_pb2.Token]:
        """Yield tokens. Raises RateLimited on RESOURCE_EXHAUSTED; other gRPC
        errors propagate. Breaking out of the iterator cancels the stream."""
        call = self.start(prompt, tenant, max_tokens, no_cache, timeout)
        try:
            for tok in call:
                yield tok
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                raise RateLimited(e.details()) from None
            if e.code() == grpc.StatusCode.CANCELLED:
                return
            raise
        finally:
            call.cancel()  # no-op once complete; stops the worker if we left early

    def stats(self) -> gateway_pb2.StatsResponse:
        return self._stub().Stats(gateway_pb2.StatsRequest(), timeout=5.0)

    def close(self) -> None:
        for c in self._channels:
            c.close()

    def __enter__(self) -> "GatewayClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class _FailoverStream:
    """A Generate stream that moves to the next gateway if the current one
    fails with UNAVAILABLE BEFORE delivering any token (connection refused,
    gateway down). After the first token it never fails over: restarting
    the generation elsewhere would hand the caller duplicated output, so a
    mid-stream error propagates. DEADLINE_EXCEEDED is not retried either:
    the caller's whole timeout is already spent.

    Behaves like the grpc call it wraps (iterate it, .cancel() it; other
    attributes such as .code() are delegated to the current call)."""

    def __init__(self, client: GatewayClient, req: gateway_pb2.GenerateRequest, timeout: float | None) -> None:
        self._client, self._req, self._timeout = client, req, timeout
        self._lock = threading.Lock()  # cancel() may come from another thread
        self._cancelled = False
        self._delivered = False
        self._tried = 1
        self._call = client._stub().Generate(req, timeout=timeout)

    def __iter__(self) -> "_FailoverStream":
        return self

    def __next__(self) -> gateway_pb2.Token:
        while True:
            call = self._call
            try:
                tok = next(call)
            except grpc.RpcError as e:
                with self._lock:
                    if (self._delivered or self._cancelled
                            or e.code() != grpc.StatusCode.UNAVAILABLE):
                        raise
                    if self._tried >= len(self._client._stubs):
                        raise ConnectionError(
                            f"no gateway reachable among {self._client.addrs}: {e.details()}") from e
                    self._client._idx += 1
                    self._tried += 1
                    self._call = self._client._stub().Generate(self._req, timeout=self._timeout)
                continue
            self._delivered = True
            return tok

    def cancel(self) -> bool:
        with self._lock:
            self._cancelled = True
            return self._call.cancel()

    def __getattr__(self, name: str):
        return getattr(self._call, name)


# -- demo -------------------------------------------------------------------

def _print_meta(tok: gateway_pb2.Token) -> None:
    print(f"\n  first token: cached={tok.cached} worker={tok.worker!r} hedged={tok.hedged} "
          f"hedge_won={tok.hedge_won} ttft_ms={tok.ttft_ms} prefix_cache_hit={tok.prefix_cache_hit} "
          f"prefill_ms={tok.prefill_ms}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:7500"
    prompt = ("You are a terse assistant for a distributed systems course. Answer in one sentence. "
              "Question: why must a hedged request's loser be cancelled?")
    try:
        with GatewayClient(target) as gw:
            print(f"gateway={gw.addrs}")

            # 1. Stream a prompt; the first token says where it came from.
            print("streaming:", end=" ", flush=True)
            t0 = time.monotonic()
            first = None
            n = 0
            for tok in gw.generate(prompt, tenant="demo", max_tokens=24):
                if first is None:
                    first = tok
                print(tok.text, end="", flush=True)
                n += 1
            print(f"\n  {n} tokens in {(time.monotonic() - t0) * 1000:.0f}ms")
            if first is not None:
                _print_meta(first)

            # 2. Cancellation: read three tokens, cancel, show that it stopped.
            print("\ncancel demo: reading 3 tokens then cancelling")
            call = gw.start(prompt + " (variant)", tenant="demo", max_tokens=200, no_cache=True)
            got = []
            for tok in call:
                got.append(tok.text)
                if len(got) == 3:
                    call.cancel()
                    break
            try:
                next(iter(call))
                print("  unexpected: stream still yielding after cancel")
            except (grpc.RpcError, StopIteration) as e:
                code = e.code().name if isinstance(e, grpc.RpcError) else "EOF"
                print(f"  read {got!r}, cancelled, stream now reports {code}: worker stopped generating")

            # 3. Show the gateway's own counters (cancelled should have gone up).
            try:
                st = gw.stats()
                print(f"\ngateway stats: requests={st.requests} cache_hits={st.cache_hits} hedges={st.hedges_launched}/"
                      f"{st.hedges_won} cancelled={st.cancelled} rate_limited={st.rate_limited} "
                      f"live_workers={st.live_workers} routed_to={dict(st.routed_to)}")
            except grpc.RpcError as e:
                print(f"stats unavailable: {e.code().name}")
    except RateLimited as e:
        print(f"\nrate limited: {e.hint}")
        sys.exit(2)
    except ConnectionError as e:
        print(f"{e} (is a gateway running on {target}?)")
        sys.exit(1)
    except grpc.RpcError as e:
        print(f"RPC failed: {e.code().name}: {e.details()} (is the gateway running on {target}?)")
        sys.exit(1)
