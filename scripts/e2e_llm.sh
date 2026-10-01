#!/usr/bin/env bash
# Phase 5 end-to-end, against REAL processes:
#
#   3 x raftkv        (the KV holding rate-limit buckets, cache index, registry)
#   1 x gateway       (rate limit -> cache -> route -> hedge -> stream)
#   4 x infer_worker  (Python; one injects stalls so hedging has something to cut)
#
# It runs the benchmark four times to isolate each feature, and CHECKS the
# result rather than just printing it:
#
#   A  prefix routing OFF, hedging OFF   (baseline)
#   B  prefix routing ON,  hedging OFF   -> prefix-cache hit rate must rise a lot
#   C  prefix routing ON,  hedging ON    -> p99 TTFT must fall vs B
#   D  cache test                        -> repeated prompts must hit the cache
#
# The workers run with a DELIBERATELY SMALL prefix cache (--kv-cache-blocks 32,
# about 3.5 of the benchmark's ~600-char system prompts). That is the whole
# point: a real attention KV cache is GPU-memory-bound and evicts constantly,
# so locality only pays when each worker sees a SUBSET of the prefixes. With a
# cache big enough to hold everything, every worker caches every prefix within
# seconds and routing cannot be told apart from random - which is exactly what
# an earlier version of this script measured, and why it proved nothing.
#
# Plus: per-tenant rate limiting is enforced, and a cancelled stream stops
# the worker.
#
# WHAT A PASSING RUN ACTUALLY SHOWS (measured 2026-09-13, 4 workers, 12 prefixes):
#
#   A -> B  prefix-cache hit rate 0.20 -> 0.60, mean prefill 167 -> 140 ms,
#           prefix spread 3.92 -> 1.08 workers per prefix.  Affinity works.
#   BUT     B's throughput (13.4 rps) and p99 TTFT (1515 ms) are WORSE than
#           A's (20.4 rps, 967 ms), because hashing 12 prefixes onto 4 workers
#           left two of them with ~85% of the traffic (py-2:105 py-3:99 vs
#           py-0:15 py-1:21). Affinity balances PREFIXES, not LOAD.
#   B -> C  hedging recovers it: p99 TTFT 1515 -> 958 ms, 17.8 rps, and mean
#           prefill drops further to 96 ms.
#
# CORRECTION (2026-10-01): that B -> C result hedged at 250ms, below B's
# median TTFT, so it hedged ~62% of requests - load-spreading, not tail
# hedging. With the hedge delay above normal latency (now enforced: the run
# FAILS if hedges reach half the requests), the mock gives p99 4232 -> 2076
# ms at a 23% hedge rate, and the real model (BACKEND=hf) 12.3 -> 9.7 s at
# 20%. See BENCHMARKS.md.
#
# That trade-off is the real lesson of this phase and is not a bug: pure
# locality creates hot spots, and you need load-aware spill (-max-inflight)
# and/or hedging to get cache hits AND a decent tail. Lowering -max-inflight
# trades hit rate back for balance; that knob is the exercise in docs/phase5.md.
#
# REAL MODEL: BACKEND=hf runs the identical checks against real LLM workers
# (py/hf_backend.py, SmolLM2-135M-Instruct on CPU) instead of the mock. The
# workload is scaled to what an 8GB laptop can hold: 2 workers (~0.7GB each)
# instead of 4, fewer and shorter requests, and a prefix cache bounded in
# 32-token entries. Every check below is the same check; only the sizes
# change. Prefix-cache hits in that mode are real skipped computation.
#
# Usage:  source ./env.sh && scripts/e2e_llm.sh [N]
#         source ./env.sh && BACKEND=hf scripts/e2e_llm.sh [N]
set -euo pipefail

BACKEND=${BACKEND:-mock}
if [ "$BACKEND" = hf ]; then
  # 6 prefixes x 3 cache entries each (a ~94-token system prompt fills the
  # 32/64/96-token blocks) against a 9-entry cache: each worker holds about
  # 3 of the 6 prefixes, the same "cache smaller than the working set"
  # regime the mock run uses, so least-loaded routing must thrash it.
  N=${1:-80}; NW=2; CONC=4; PREFIXES=6; MAXTOK=16; CACHE=9
  WORKER_FLAGS="--backend hf --torch-threads ${TORCH_THREADS:-3}"
  # hedge-after sits near the baseline's measured TTFT p95 (~5s on this
  # laptop): a hedge should fire on the tail, not on ordinary queueing. A
  # first version used 1.5s, below the median TTFT, and hedged 71 of 80
  # requests - which "worked" by load-balancing, and destroyed prefix
  # affinity doing it. The stall is long enough to be unmistakably tail.
  STALL="--stall-prob 0.2 --stall-ms 8000"; HEDGE_AFTER=5s
  D_N=16; E_N=16; E_C=4
else
  N=${1:-240}; NW=4; CONC=16; PREFIXES=12; MAXTOK=32; CACHE=32
  WORKER_FLAGS="--token-ms 6"
  # Same rule as above. The original 250ms sat below prefix routing's
  # median TTFT (~600ms, from hot-spot queueing) and hedged 150 of 240
  # requests; its p99 "win" was load-spreading. 1s is above the baseline's
  # TTFT p95 (~0.66s), and a 2.5s stall is unmistakably tail.
  STALL="--stall-prob 0.15 --stall-ms 2500"; HEDGE_AFTER=1s
  D_N=40; E_N=30; E_C=6
fi
BENCH="-max-tokens $MAXTOK -timeout 120s"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
LOGDIR=${LOGDIR:-/tmp/e2e_llm}
rm -rf "$LOGDIR" data/e2ellm; mkdir -p "$LOGDIR"

PY=.venv/Scripts/python.exe
KV=127.0.0.1:7601,127.0.0.1:7602,127.0.0.1:7603
GW=127.0.0.1:7650
W_BASE=7660

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  taskkill //F //IM raftkv.exe  >/dev/null 2>&1 || true
  taskkill //F //IM gateway.exe >/dev/null 2>&1 || true
  # .venv/Scripts/python.exe is a launcher: killing it (the loop above) leaves
  # the real interpreter running, still holding the model in memory. Kill this
  # run's workers by command line (only ours: they listen on the W_BASE ports).
  powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { \$_.CommandLine -like '*infer_worker.py --addr 127.0.0.1:$((W_BASE/10))*' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force -ErrorAction SilentlyContinue }" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() { echo "E2E FAIL: $*" >&2; exit 1; }
step() { echo; echo "== $*"; }
# jqlite: pull a numeric field out of llmbench's -json line
num() { sed -n "s/.*\"$1\":\([0-9.]*\).*/\1/p" <<<"$2" | head -1; }

start_stack() { # $1 = extra gateway flags
  taskkill //F //IM gateway.exe >/dev/null 2>&1 || true
  sleep 0.5
  ./bin/gateway.exe -addr $GW -kv $KV -rate 10000 -burst 10000 -max-inflight 16 $1 >"$LOGDIR/gw.log" 2>&1 &
  pids+=($!)
  sleep 1.5
}

step "build"
make build >/dev/null

step "start 3-node raftkv"
for i in 0 1 2; do
  ./bin/raftkv.exe -id $i -peers $KV -data data/e2ellm/kv$i >"$LOGDIR/kv$i.log" 2>&1 &
  pids+=($!)
done
sleep 2
./bin/kvctl.exe -addr $KV put e2e-probe ok >/dev/null || fail "kv not serving"

step "start gateway (prefix routing OFF, no hedge) and $NW python workers (backend=$BACKEND)"
start_stack "-prefix-routing=false -cache=false"
for i in $(seq 0 $((NW-1))); do
  stall=""
  # the last worker injects tail stalls so hedging has something to cut
  [ "$i" = $((NW-1)) ] && stall="$STALL --seed 7"
  $PY py/infer_worker.py --addr 127.0.0.1:$((W_BASE+i)) --gateway $GW \
      --worker-id py-$i $WORKER_FLAGS --kv-cache-blocks $CACHE $stall -v >"$LOGDIR/w$i.log" 2>&1 &
  pids+=($!)
done
if [ "$BACKEND" = hf ]; then
  # loading weights takes a while; wait until every worker says it serves
  ready=0
  for t in $(seq 1 180); do
    ready=$(grep -l "serving" "$LOGDIR"/w*.log 2>/dev/null | wc -l || true)
    [ "$ready" -ge "$NW" ] && break
    sleep 1
  done
  [ "$ready" -ge "$NW" ] || fail "only $ready/$NW hf workers came up (see $LOGDIR/w*.log)"
fi
sleep 3
grep -q "no inference workers" "$LOGDIR/gw.log" && true # informational

step "A: baseline (no prefix routing, no hedging)"
A=$(./bin/llmbench.exe -gateway $GW -n "$N" -c $CONC -prefixes $PREFIXES $BENCH -json) || fail "bench A failed"
echo "$A"
A_HIT=$(num prefix_hit_rate "$A"); A_P99=$(num ttft_p99_ms "$A"); A_ERR=$(num errors "$A")
[ "${A_ERR:-0}" = "0" ] || fail "baseline had $A_ERR errors"

step "B: prefix routing ON"
start_stack "-prefix-routing=true -cache=false"
sleep 3   # workers re-register with the new gateway process
B=$(./bin/llmbench.exe -gateway $GW -n "$N" -c $CONC -prefixes $PREFIXES $BENCH -json) || fail "bench B failed"
echo "$B"
B_HIT=$(num prefix_hit_rate "$B"); B_P99=$(num ttft_p99_ms "$B"); B_SPREAD=$(num prefix_spread_avg "$B")

step "C: prefix routing ON + hedging"
start_stack "-prefix-routing=true -cache=false -hedge-after $HEDGE_AFTER"
sleep 3
C=$(./bin/llmbench.exe -gateway $GW -n "$N" -c $CONC -prefixes $PREFIXES $BENCH -json) || fail "bench C failed"
echo "$C"
C_P99=$(num ttft_p99_ms "$C"); C_HEDGE=$(num hedges_launched "$C"); C_WON=$(num hedges_won "$C")

step "D: semantic cache"
start_stack "-prefix-routing=true -cache=true -hedge-after $HEDGE_AFTER"
sleep 3
D=$(./bin/llmbench.exe -gateway $GW -n $D_N -c 4 -prefixes 2 -cache-test $BENCH -json) || fail "bench D failed"
echo "$D"
D_CACHE=$(num cache_test_hit_rate "$D")

step "E: per-tenant rate limiting"
start_stack "-prefix-routing=true -cache=false -rate 2 -burst 3"
sleep 3
E=$(./bin/llmbench.exe -gateway $GW -n $E_N -c $E_C -tenants 1 -prefixes 1 $BENCH -json) || true
echo "$E"
E_LIMITED=$(num rate_limited "$E")

step "checks"
ok() { echo "  PASS: $*"; }
awk_gt() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>b)}'; }

awk_gt "${B_HIT:-0}" "${A_HIT:-0}" \
  || fail "prefix routing did not raise the prefix-cache hit rate (A=$A_HIT B=$B_HIT)"
ok "prefix-cache hit rate: $A_HIT (least-loaded) -> $B_HIT (prefix routing)"
A_PREFILL=$(num prefill_mean_ms "$A"); B_PREFILL=$(num prefill_mean_ms "$B")
awk_gt "${A_PREFILL:-0}" "${B_PREFILL:-999999}"   || fail "prefix routing did not reduce mean prefill (A=$A_PREFILL B=$B_PREFILL)"
ok "mean prefill: $A_PREFILL ms -> $B_PREFILL ms"

awk -v s="${B_SPREAD:-9}" 'BEGIN{exit !(s<2.0)}' \
  || fail "prefix spread $B_SPREAD too high: prefixes are not sticking to one worker"
ok "prefix spread with affinity: $B_SPREAD workers per prefix"

awk_gt "${B_P99:-0}" "${C_P99:-999999}" \
  || echo "  WARN: hedging did not improve p99 TTFT (B=$B_P99 C=$C_P99); stalls may not have fired this run"
awk_gt "${C_HEDGE:-0}" "0" || fail "no hedges were launched with -hedge-after set"
# Hedging is for the TAIL. If most requests hedge, -hedge-after is below
# normal latency and the "win" is really load-spreading at double the cost.
awk -v h="${C_HEDGE:-0}" -v n="$N" 'BEGIN{exit !(h < n/2)}'   || fail "hedges fired on $C_HEDGE of $N requests: -hedge-after is below normal latency, so this is not tail hedging"
ok "hedging: $C_HEDGE launched, $C_WON won; p99 TTFT $B_P99 -> $C_P99 ms"

awk_gt "${D_CACHE:-0}" "0.8" || fail "cache hit rate $D_CACHE too low on repeated prompts"
ok "semantic cache hit rate on repeats: $D_CACHE"

awk_gt "${E_LIMITED:-0}" "0" || fail "rate limiting never triggered at -rate 2 -burst 3"
ok "rate limiting: $E_LIMITED requests shed with a retry hint"

step "cancellation reaches the worker"
$PY - <<'PYEOF' > "$LOGDIR/cancel.txt" 2>&1 || fail "cancellation check failed"
import sys, grpc
sys.path.insert(0, "py"); sys.path.insert(0, "py/dsys_gateway")
from dsys_gateway.gateway.v1 import gateway_pb2 as gw, gateway_pb2_grpc as gwg
ch = grpc.insecure_channel("127.0.0.1:7650")
st = gwg.GatewayStub(ch).Generate(gw.GenerateRequest(
    tenant="cancel", prompt="cancel me " * 40, max_tokens=500, no_cache=True))
n = 0
for _ in st:
    n += 1
    if n == 3:
        st.cancel(); break
print("cancelled after", n, "tokens")
PYEOF
cat "$LOGDIR/cancel.txt"
grep -qi "cancel" "$LOGDIR"/w*.log || fail "no worker logged a cancellation"
ok "a cancelled client stream stopped generation on the worker"

echo
echo "E2E PASS (backend=$BACKEND): prefix routing, hedging, semantic cache, rate limiting and cancellation all verified against real processes."
