"""A short, honest walkthrough of the serving path against a REAL model.

Used by scripts/record_demo.sh to record docs/media/llm.gif, but it is an
ordinary client: point it at any running gateway with a `--backend hf`
worker behind it. Every number it prints comes from the live system:

  1. a cold request (the worker has never seen this system prompt)
  2. the same system prompt with a new question: the semantic cache must
     not answer it (it is a different question), and the worker reuses the
     prompt's KV cache, prefilling only the new suffix
  3. the same question with a typo: a near-duplicate, so the gateway's
     semantic cache answers with no model call at all. Near-duplicate
     matching is OFF by default (exact matches only, because lexical
     similarity cannot see meaning); the recording runs the gateway with
     -cache-near to show it, and step 2 shows the guard that matters
  4. a client that hangs up after a few tokens: the cancel reaches the
     worker, which stops generating (checked on the worker itself)

Usage:
    .venv/Scripts/python.exe py/demo_llm.py --gateway 127.0.0.1:7950 --worker 127.0.0.1:7960
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import grpc

_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.join(_HERE, "dsys_infer"), os.path.join(_HERE, "dsys_gateway")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dsys_infer.infer.v1 import infer_pb2, infer_pb2_grpc  # noqa: E402
from gateway_client import GatewayClient  # noqa: E402

# Long enough to fill two 32-token cache blocks, so a second question that
# shares it can reuse real KV state.
SYSTEM = (
    "You are a helpful teaching assistant for a university course on distributed systems. "
    "You explain consensus, replication, sharding, leases and fault tolerance clearly and "
    "accurately, in plain English, in at most two short sentences. "
)
Q1 = "What does the leader do in the Raft consensus algorithm?"
Q2 = "Why does a distributed lock need a fencing token?"
WIDTH = 78


def prompt(question: str) -> str:
    return f"{SYSTEM}\n\nUser: {question}\nAssistant:"


def say(line: str = "") -> None:
    print(line, flush=True)


def ask(gw: GatewayClient, question: str, max_tokens: int = 40, no_cache: bool = False):
    """Stream one answer, printing it a wrapped line at a time as tokens
    arrive, and return the first token (which carries the metadata)."""
    say(f"> User: {question}")
    first, line, n = None, "  ", 0
    t0 = time.monotonic()
    for tok in gw.generate(prompt(question), tenant="demo", max_tokens=max_tokens, no_cache=no_cache):
        first = first or tok
        n += 1
        text = tok.text.replace("\n", " ")
        if line == "  ":
            text = text.lstrip()
        for ch in text:
            line += ch
            if len(line) >= WIDTH and ch == " ":
                say(line.rstrip())
                line = "  "
    if line.strip():
        say(line.rstrip())
    elapsed = (time.monotonic() - t0) * 1000
    if first is None:
        say("  (no tokens)")
        return None
    if first.cached:
        # A cached answer streams back as stored text chunks, not model
        # tokens, so report only the time.
        say(f"  [semantic cache hit: no model call, whole answer in {elapsed:.0f} ms]")
    else:
        say(f"  [worker={first.worker}  prefill={first.prefill_ms} ms  "
            f"prefix_cache_hit={str(first.prefix_cache_hit).lower()}  "
            f"ttft={first.ttft_ms} ms  {n} tokens in {elapsed:.0f} ms]")
    return first


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default="127.0.0.1:7950")
    ap.add_argument("--worker", default="127.0.0.1:7960", help="worker address, to verify the cancel")
    args = ap.parse_args()

    worker = infer_pb2_grpc.InferenceStub(grpc.insecure_channel(args.worker))
    model = worker.Health(infer_pb2.HealthRequest(), timeout=5).model

    with GatewayClient(args.gateway) as gw:
        say(f"# real model: {model.split('/')[-1]} on CPU, via the gateway + a 3-node Raft KV")
        say("# (135M parameters: the answers are only as good as that. The point is")
        say("#  the serving path, and every number below is measured live.)")
        say()
        say("# 1. cold: this worker has never seen the system prompt")
        a = ask(gw, Q1)
        say()
        say("# 2. same system prompt, different question. The semantic cache must")
        say("#    NOT answer it; the worker reuses the prompt's KV cache instead")
        b = ask(gw, Q2)
        if a is not None and b is not None and b.prefix_cache_hit and b.prefill_ms:
            say(f"  -> prefill {a.prefill_ms} ms -> {b.prefill_ms} ms "
                f"({a.prefill_ms / b.prefill_ms:.1f}x less prefill time)")
        say()
        say("# 3. same question with a typo: a near-duplicate, served from cache")
        ask(gw, Q2.replace("token", "tokn"))
        say()
        say("# 4. client hangs up after 4 of 200 tokens")
        call = gw.start(prompt("Explain the Paxos algorithm in detail."), tenant="demo",
                        max_tokens=200, no_cache=True)
        got = 0
        for _ in call:
            got += 1
            if got == 4:
                call.cancel()
                break
        time.sleep(0.5)
        inflight = worker.Health(infer_pb2.HealthRequest(), timeout=5).inflight
        say(f"  cancelled after {got} tokens; worker inflight={inflight} 0.5 s later")
        say("  -> the cancel reached the worker, which stopped generating" if inflight == 0
            else "  -> WARNING: worker still generating")


if __name__ == "__main__":
    main()
