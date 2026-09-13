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
                    batch-queueing stall, i.e. the tail the gateway hedges).
                    Then max_tokens tokens, --token-ms apart.
  --backend hf    Optional, untested here: a real HuggingFace model, greedy,
                  streamed. Needs `pip install torch transformers` in .venv.

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

# Same trick as worker.py / client.py: the generated *_pb2_grpc.py files do
# bare `from infer.v1 import ...` / `from gateway.v1 import ...` imports
# rooted at `protoc -I proto`, so each stub package dir goes on sys.path.
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.join(_HERE, "dsys_infer"), os.path.join(_HERE, "dsys_gateway")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dsys_gateway.gateway.v1 import gateway_pb2, gateway_pb2_grpc  # noqa: E402
from dsys_infer.infer.v1 import infer_pb2, infer_pb2_grpc  # noqa: E402

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
        if stalled:
            prefill_ms += self.stall_ms
        # Prefill: nothing streams until it is done. Sleep in slices so a
        # cancel during a long (or stalled) prefill also stops us promptly.
        if not _sleep_unless_cancelled(prefill_ms / 1000.0, active):
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


# -- hf backend (optional, UNTESTED here: no torch/transformers in the venv) --

class HFBackend:
    """Greedy decoding from a HuggingFace causal LM, one token per yield,
    with a cancel check between steps. Kept deliberately small; real hosts
    would batch requests and use the model server's own KV cache."""

    def __init__(self, args: argparse.Namespace) -> None:
        try:
            import torch  # type: ignore
            from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
        except ImportError:
            sys.exit("--backend hf needs torch and transformers: "
                     "run `.venv/Scripts/pip install torch transformers` and retry")
        self.torch = torch
        self.model_name = args.model or "sshleifer/tiny-gpt2"
        self.model = self.model_name
        log.info("loading %s (this can take a while)", self.model_name)
        self.tok = AutoTokenizer.from_pretrained(self.model_name)
        self.lm = AutoModelForCausalLM.from_pretrained(self.model_name)
        self.lm.eval()
        self.lock = threading.Lock()  # one forward pass at a time; no batching here

    def generate(self, req: infer_pb2.GenerateRequest, active) -> Iterator[infer_pb2.Token]:
        torch = self.torch
        max_tokens = req.max_tokens if req.max_tokens > 0 else 64
        eos = self.tok.eos_token_id
        ids = self.tok(req.prompt, return_tensors="pt").input_ids
        past = None
        t0 = time.monotonic()
        with torch.no_grad():
            for i in range(max_tokens):
                if not active():
                    return
                with self.lock:
                    out = self.lm(input_ids=ids if past is None else ids[:, -1:], past_key_values=past, use_cache=True)
                past = out.past_key_values
                nxt = out.logits[:, -1, :].argmax(dim=-1, keepdim=True)
                ids = torch.cat([ids, nxt], dim=-1)
                tok = infer_pb2.Token(text=self.tok.decode(nxt[0]), index=i)
                if i == 0:
                    tok.prefill_ms = int((time.monotonic() - t0) * 1000)  # no prefix cache in this path
                stop = int(nxt) == eos or i == max_tokens - 1
                if stop:
                    tok.done = True
                    tok.finish_reason = "stop" if int(nxt) == eos else "length"
                yield tok
                if stop:
                    return


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
        finished = False
        try:
            for tok in self.backend.generate(request, active):
                if tok.index == 0 and tok.prefix_cache_hit:
                    with st.lock:
                        st.prefix_hits += 1
                yield tok
                if tok.done:
                    finished = True
        finally:
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
    ap.add_argument("--kv-cache-blocks", type=int, default=512, help="mock: LRU capacity in 64-char blocks")
    ap.add_argument("--token-ms", type=float, default=12.0, help="mock: inter-token interval")
    ap.add_argument("--stall-prob", type=float, default=0.0, help="mock: probability of a tail stall")
    ap.add_argument("--stall-ms", type=float, default=400.0, help="mock: stall duration added to prefill")
    ap.add_argument("--seed", type=int, default=None, help="mock: RNG seed for stalls")
    ap.add_argument("--stats-every", default="10s")
    ap.add_argument("-v", "--verbose", action="store_true", help="debug logging (logs each cancel)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    worker_id = args.worker_id or f"py-{socket.gethostname()}-{os.getpid()}"
    advertise = args.advertise or args.addr
    gateways = [g.strip() for g in args.gateway.split(",") if g.strip()]
    if not gateways:
        sys.exit("--gateway is required")

    backend = MockBackend(args) if args.backend == "mock" else HFBackend(args)
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
