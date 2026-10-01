"""Inference worker: serves infer.v1.Inference (proto/infer/v1/infer.proto)
and keeps itself registered with the gateway.

This is the shape a real model host takes, minus the model. Two things
matter here and both come straight from earlier phases:

  * Registration is a LEASE (Phase 4 applied to service discovery). Every
    lease_ms/3 we call Gateway.RegisterWorker with our id, address, load and
    lease. If we die, we simply stop renewing and the gateway drops us from
    routing when the lease lapses. No deregister RPC, no "graceful leave"
    protocol: the absence of heartbeats IS the signal, and it is the only one
    that survives a kill -9.

  * Cancellation is mandatory (see infer.proto). The gateway hedges slow
    requests onto a second worker and cancels the loser; a client that goes
    away cancels its stream. Either way the worker must stop generating, or
    every hedge race burns double the GPU time. We check context.is_active()
    before every token and during prefill.

Backends:
  --backend mock  (default) A mock LLM whose LATENCY MODEL MATCHES THE GO MOCK
                  in infer/mock exactly, because the Phase 5 benchmark mixes
                  Go and Python workers behind one gateway:
                    prompt -> 64-char blocks; an LRU of cumulative block-prefix
                    hashes stands in for the attention KV cache; the longest
                    leading run of blocks already in the LRU is "cached".
                    prefill_ms = 20 + 0.25 * (len(prompt) - cached_prefix_chars)
                    (+ --stall-ms with probability --stall-prob: a GC pause /
                    batch-queueing stall, i.e. the tail the gateway hedges;
                    it delays the first token but is not reported as prefill).
                    Then max_tokens tokens, --token-ms apart.
  --backend hf    A real HuggingFace causal LM (default SmolLM2-135M-Instruct),
                  greedy, streamed, with genuine prefix KV-cache reuse; see
                  py/hf_backend.py. --kv-cache-blocks bounds its cache in
                  32-token entries and --stall-prob/--stall-ms inject the
                  same tail stalls as the mock. Needs torch + transformers
                  (see requirements.txt).

Run:
    .venv/Scripts/python.exe py/infer_worker.py --addr 127.0.0.1:7600 --gateway 127.0.0.1:7500
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import random
import socket
import sys
import threading
import time
from collections import OrderedDict
from concurrent import futures
from typing import Iterator

import grpc
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Same trick as worker.py / client.py: the generated *_pb2_grpc.py files do
# bare `from infer.v1 import ...` / `from gateway.v1 import ...` imports
# rooted at `protoc -I proto`, so each stub package dir goes on sys.path.
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.join(_HERE, "dsys_infer"), os.path.join(_HERE, "dsys_gateway")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dsys_gateway.gateway.v1 import gateway_pb2, gateway_pb2_grpc  # noqa: E402
from dsys_infer.infer.v1 import infer_pb2, infer_pb2_grpc  # noqa: E402

tracer = trace.get_tracer("infer.worker")


def init_tracing(otlp_endpoint: str, worker_id: str) -> None:
    """Wires the global OTel TracerProvider to export spans via OTLP/gRPC to
    otlp_endpoint. Called only when --otlp-endpoint is non-empty; otherwise
    `tracer` above stays the SDK-less default no-op tracer (every span a
    silent, allocation-cheap no-op), matching dsys/obs.InitTracing's Go-side
    behaviour for an empty -otlp-endpoint.
    """
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "infer-worker", "worker.id": worker_id}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)))
    trace.set_tracer_provider(provider)

log = logging.getLogger("infer.worker")

BLOCK_CHARS = 64          # prefix-cache granularity; must match infer/mock
PREFILL_BASE_MS = 20.0    # fixed cost of a forward pass
PREFILL_PER_CHAR_MS = 0.25

# Token text is deterministic in (prompt hash, index) so a hedge race's two
# attempts produce identical streams and the benchmark can compare outputs.
_WORDS = (
    "the of and to in is that it was for on are as with his they at be this "
    "from have or one had by word but not what all were we when your can said "
    "there use an each which she do how their if will up other about out many "
    "then them these so some her would make like him into time has look two "
    "more write go see number no way could people my than first water been "
    "call who oil its now find long down day did get come made may part over"
).split()


class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.claimed = 0      # Generate calls started
        self.completed = 0    # streams that reached done=true
        self.cancelled = 0    # streams the caller cancelled mid-way
        self.prefix_hits = 0  # first tokens with prefix_cache_hit
        self.inflight = 0

    def snapshot(self) -> str:
        with self.lock:
            rate = self.prefix_hits / self.claimed if self.claimed else 0.0
            return (f"claimed={self.claimed} completed={self.completed} cancelled={self.cancelled} "
                    f"inflight={self.inflight} prefix_hit_rate={rate:.2f}")


# -- mock backend ------------------------------------------------------------

class PrefixCache:
    """LRU of cumulative block-prefix hashes: the hash of blocks[0..i] for
    each i of every prompt we have prefilled. Only FULL blocks count, so a
    prompt shorter than 64 chars never hits and the cached_prefix_chars of a
    repeat prompt is len(prompt) rounded down to a block."""

    def __init__(self, capacity: int) -> None:
        self.capacity = max(1, capacity)
        self._lru: OrderedDict[bytes, None] = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def hashes(prompt: str) -> list[bytes]:
        n = len(prompt) // BLOCK_CHARS
        return [hashlib.sha256(prompt[: (i + 1) * BLOCK_CHARS].encode("utf-8")).digest() for i in range(n)]

    def cached_prefix_blocks(self, hashes: list[bytes]) -> int:
        """Longest leading run of blocks whose cumulative hash is present.
        Touches the hits so a hot prefix stays resident (LRU semantics)."""
        run = 0
        with self._lock:
            for h in hashes:
                if h not in self._lru:
                    break
                self._lru.move_to_end(h)
                run += 1
        return run

    def insert(self, hashes: list[bytes]) -> None:
        with self._lock:
            for h in hashes:
                self._lru[h] = None
                self._lru.move_to_end(h)
            while len(self._lru) > self.capacity:
                self._lru.popitem(last=False)


class MockBackend:
    def __init__(self, args: argparse.Namespace) -> None:
        self.cache = PrefixCache(args.kv_cache_blocks)
        self.token_ms = args.token_ms
        self.stall_prob = args.stall_prob
        self.stall_ms = args.stall_ms
        self.rng = random.Random(args.seed)
        self.rng_lock = threading.Lock()
        self.model = args.model or "mock"

    def generate(self, req: infer_pb2.GenerateRequest, active) -> Iterator[infer_pb2.Token]:
        prompt = req.prompt
        hashes = PrefixCache.hashes(prompt)
        cached_chars = BLOCK_CHARS * self.cache.cached_prefix_blocks(hashes)
        prefill_ms = PREFILL_BASE_MS + PREFILL_PER_CHAR_MS * (len(prompt) - cached_chars)
        with self.rng_lock:
            stalled = self.stall_prob > 0 and self.rng.random() < self.stall_prob
        # A stall delays the first token but is not reported as prefill (same
        # as the Go mock and the hf backend): prefill_ms measures the cache's
        # effect, TTFT measures the tail.
        wait_ms = prefill_ms + (self.stall_ms if stalled else 0.0)
        # Prefill: nothing streams until it is done. Sleep in slices so a
        # cancel during a long (or stalled) prefill also stops us promptly.
        if not _sleep_unless_cancelled(wait_ms / 1000.0, active):
            return
        self.cache.insert(hashes)

        seed = int.from_bytes(hashlib.sha256(prompt.encode("utf-8")).digest()[:8], "big")
        max_tokens = req.max_tokens if req.max_tokens > 0 else 64
        for i in range(max_tokens):
            if not active():
                return
            if i > 0 and not _sleep_unless_cancelled(self.token_ms / 1000.0, active):
                return
            word = _WORDS[(seed + i * 7919) % len(_WORDS)]
            tok = infer_pb2.Token(text=word + " ", index=i)
            if i == 0:
                tok.prefill_ms = int(round(prefill_ms))
                tok.prefix_cache_hit = cached_chars > 0
                tok.cached_prefix_chars = cached_chars
            if i == max_tokens - 1:
                tok.done = True
                tok.finish_reason = "length"
            yield tok


def _sleep_unless_cancelled(seconds: float, active, slice_s: float = 0.02) -> bool:
    """Sleep `seconds`, polling active() every slice. False if cancelled."""
    deadline = time.monotonic() + seconds
    while True:
        if not active():
            return False
        left = deadline - time.monotonic()
        if left <= 0:
            return True
        time.sleep(min(left, slice_s))


# -- hf backend -------------------------------------------------------------
# The real-model path lives in hf_backend.py: a HuggingFace causal LM with
# GENUINE prefix KV-cache reuse (the thing vLLM/SGLang do), so a reported
# prefix_cache_hit means computation actually skipped, not a simulated flag.
# It is imported lazily so the default mock backend needs neither torch nor
# transformers installed.


def make_hf_backend(args):
    from hf_backend import HFBackend

    return HFBackend(args, infer_pb2)


# -- gRPC service ------------------------------------------------------------

class InferenceServicer(infer_pb2_grpc.InferenceServicer):
    def __init__(self, worker_id: str, backend, stats: Stats) -> None:
        self.worker_id = worker_id
        self.backend = backend
        self.stats = stats

    def Generate(self, request: infer_pb2.GenerateRequest, context: grpc.ServicerContext):
        st = self.stats
        with st.lock:
            st.claimed += 1
            st.inflight += 1
        # context.is_active() flips to False the moment the peer cancels or
        # its deadline passes; that is the signal the whole design rests on.
        active = context.is_active

        # Extract the gateway's W3C traceparent from gRPC metadata (set by
        # dsys/obs.GRPCDialOption on the Go gateway's client conn) so this
        # span nests under the gateway.attempt span that triggered it. With
        # no tracing configured on either side this is simply a no-op empty
        # context, and start_as_current_span is a cheap no-op span.
        carrier = dict(context.invocation_metadata())
        parent_ctx = extract(carrier)

        finished = False
        with tracer.start_as_current_span(
            "worker.Generate", context=parent_ctx,
            attributes={"request_id": request.request_id, "tenant": request.tenant},
        ) as span:
            try:
                for tok in self.backend.generate(request, active):
                    if tok.index == 0:
                        span.set_attributes({
                            "prefill_ms": tok.prefill_ms,
                            "prefix_cache_hit": tok.prefix_cache_hit,
                            "cached_prefix_chars": tok.cached_prefix_chars,
                        })
                        if tok.prefix_cache_hit:
                            with st.lock:
                                st.prefix_hits += 1
                    yield tok
                    if tok.done:
                        finished = True
            finally:
                span.set_attribute("cancelled", not finished)
                with st.lock:
                    st.inflight -= 1
                    if finished:
                        st.completed += 1
                    else:
                        st.cancelled += 1
                if not finished:
                    log.debug("request %s cancelled by peer, generation stopped", request.request_id)

    def Health(self, request, context):
        with self.stats.lock:
            inflight = self.stats.inflight
        return infer_pb2.HealthResponse(worker_id=self.worker_id, inflight=inflight, model=self.backend.model)


# -- gateway registration (the lease loop) -----------------------------------

def register_loop(stop: threading.Event, gateways: list[str], worker_id: str, addr: str,
                  lease_ms: int, model: str, stats: Stats) -> None:
    """Renew our registration every lease_ms/3. On failure move to the next
    gateway address; any replica will do, they all write the same registry."""
    interval = lease_ms / 3 / 1000.0
    idx = 0
    channels: dict[str, grpc.Channel] = {}
    registered_with: str | None = None
    while True:
        target = gateways[idx % len(gateways)]
        ch = channels.get(target)
        if ch is None:
            ch = channels[target] = grpc.insecure_channel(target)
        with stats.lock:
            inflight = stats.inflight
        req = gateway_pb2.RegisterWorkerRequest(worker_id=worker_id, addr=addr, inflight=inflight,
                                                lease_ms=lease_ms, model=model)
        try:
            gateway_pb2_grpc.GatewayStub(ch).RegisterWorker(req, timeout=max(1.0, interval))
            if registered_with != target:
                log.info("registered with gateway %s (lease %dms)", target, lease_ms)
                registered_with = target
        except grpc.RpcError as e:
            # Warn once per outage (not once per attempt), then keep retrying
            # quietly: an unreachable gateway is expected during rolling restarts.
            lvl = logging.WARNING if registered_with is not None or idx == 0 else logging.DEBUG
            log.log(lvl, "register with %s failed: %s; trying next gateway", target, e.code().name)
            registered_with = None
            idx += 1
            # Try every gateway back to back, then wait a full interval before
            # the next sweep so a dead fleet does not get hammered.
            if stop.wait(interval if idx % len(gateways) == 0 else 0.05):
                break
            continue
        if stop.wait(interval):
            break
    for ch in channels.values():
        ch.close()


def stats_loop(stop: threading.Event, every: float, stats: Stats) -> None:
    while not stop.wait(every):
        log.info("stats: %s", stats.snapshot())


def parse_duration(s: str) -> float:
    s = s.strip().lower()
    for suffix, mult in (("ms", 0.001), ("s", 1.0), ("m", 60.0)):
        if s.endswith(suffix):
            return float(s[: -len(suffix)]) * mult
    return float(s)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--addr", default="127.0.0.1:7600", help="host:port to serve Inference on (also advertised)")
    ap.add_argument("--advertise", default="", help="address to advertise if different from --addr")
    ap.add_argument("--gateway", default="127.0.0.1:7500", help="comma-separated gateway addresses")
    ap.add_argument("--worker-id", default="", help="default py-<host>-<pid>")
    ap.add_argument("--lease-ms", type=int, default=3000)
    ap.add_argument("--concurrency", type=int, default=4, help="gRPC thread pool size")
    ap.add_argument("--backend", choices=("mock", "hf"), default="mock")
    ap.add_argument("--model", default="", help="model name (hf) or label (mock)")
    ap.add_argument("--kv-cache-blocks", type=int, default=512, help="prefix-cache capacity: mock = 64-char blocks, hf = 32-token entries")
    ap.add_argument("--token-ms", type=float, default=12.0, help="mock: inter-token interval")
    ap.add_argument("--stall-prob", type=float, default=0.0, help="probability of an injected tail stall (mock and hf)")
    ap.add_argument("--stall-ms", type=float, default=400.0, help="injected stall duration (mock and hf)")
    ap.add_argument("--seed", type=int, default=None, help="RNG seed for stalls")
    ap.add_argument("--torch-threads", type=int, default=2, help="hf: CPU threads for inference")
    ap.add_argument("--stats-every", default="10s")
    ap.add_argument("--otlp-endpoint", default="", help="OTLP/gRPC endpoint for traces, e.g. 127.0.0.1:4317 (empty = tracing disabled)")
    ap.add_argument("-v", "--verbose", action="store_true", help="debug logging (logs each cancel)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    worker_id = args.worker_id or f"py-{socket.gethostname()}-{os.getpid()}"
    if args.otlp_endpoint:
        init_tracing(args.otlp_endpoint, worker_id)
        log.info("tracing enabled: exporting to %s", args.otlp_endpoint)
    advertise = args.advertise or args.addr
    gateways = [g.strip() for g in args.gateway.split(",") if g.strip()]
    if not gateways:
        sys.exit("--gateway is required")

    backend = MockBackend(args) if args.backend == "mock" else make_hf_backend(args)
    stats = Stats()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.concurrency))
    infer_pb2_grpc.add_InferenceServicer_to_server(InferenceServicer(worker_id, backend, stats), server)
    server.add_insecure_port(args.addr)
    server.start()
    log.info("worker %s serving %s on %s, registering with %s", worker_id, backend.model, args.addr, gateways)

    stop = threading.Event()
    threads = [
        threading.Thread(target=register_loop, name="register", daemon=True,
                         args=(stop, gateways, worker_id, advertise, args.lease_ms, backend.model, stats)),
        threading.Thread(target=stats_loop, name="stats", daemon=True,
                         args=(stop, parse_duration(args.stats_every), stats)),
    ]
    for t in threads:
        t.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        # Stop renewing FIRST; the gateway forgets us within one lease.
        server.stop(grace=2.0).wait()
        log.info("stopped: %s", stats.snapshot())


if __name__ == "__main__":
    main()
