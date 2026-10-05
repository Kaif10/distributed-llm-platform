#!/usr/bin/env bash
# The Phase 5 routing comparison, measured properly: repeated trials, fresh
# workers for every trial, capacity-matched admission, more than one slow
# worker, and optionally open-loop load. scripts/e2e_llm.sh stays the
# pass/fail CORRECTNESS gate; this script produces the published numbers.
#
# What an outside review found wrong with the single-run A/B/C table, and
# what this does instead:
#   - one trial                     -> TRIALS trials per phase, every trial
#                                      printed, plus mean and sample sd
#   - workers reused across phases  -> a fresh stack (KV, gateway, workers)
#     (warm caches leak between        for EVERY trial of every phase
#     phases)
#   - -max-inflight 16 against      -> -max-inflight 4, the Python worker's
#     workers that run 4 at once       real concurrency (--concurrency 4)
#   - only one worker stalls        -> two of four stall (prob 0.1, 2.5 s)
#   - closed loop only              -> OPEN=1 sends Poisson arrivals at RATE
#                                      req/s, latency from scheduled time
#
# Phases: A least-loaded; B pure prefix affinity; B2 bounded load (1.25);
#         C pure affinity + hedging at 1 s.
#
# Usage:  source ./env.sh && bash scripts/bench_routing.sh            # closed loop
#         source ./env.sh && OPEN=1 RATE=12 bash scripts/bench_routing.sh
set -euo pipefail

TRIALS=${TRIALS:-5}
N=${N:-240}
CONC=${CONC:-16}
OPEN=${OPEN:-0}
RATE=${RATE:-12}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
LOGDIR=${LOGDIR:-${TMPDIR:-/tmp}/bench_routing}
rm -rf "$LOGDIR" data/benchrt; mkdir -p "$LOGDIR"

PY=.venv/Scripts/python.exe
KV=127.0.0.1:7801,127.0.0.1:7802,127.0.0.1:7803
KVPEERS=127.0.0.1:17801,127.0.0.1:17802,127.0.0.1:17803
GW=127.0.0.1:7850
W_BASE=7860

kill_stack() {
  taskkill //F //IM raftkv.exe  >/dev/null 2>&1 || true
  taskkill //F //IM gateway.exe >/dev/null 2>&1 || true
  powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { \$_.CommandLine -like '*infer_worker.py --addr 127.0.0.1:$((W_BASE/10))*' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force -ErrorAction SilentlyContinue }" >/dev/null 2>&1 || true
  rm -rf data/benchrt
}
trap kill_stack EXIT

start_stack() { # $1 = gateway flags
  kill_stack
  for i in 0 1 2; do
    ./bin/raftkv.exe -id $i -peers $KVPEERS -client-addrs $KV -data data/benchrt/kv$i >/dev/null 2>&1 &
  done
  sleep 2
  ./bin/gateway.exe -addr $GW -kv $KV -rate 10000 -burst 10000 -max-inflight 4 -cache=false \
    -stats-every 0 -metrics-addr 127.0.0.1:0 $1 >"$LOGDIR/gw.log" 2>&1 &
  sleep 1
  for i in 0 1 2 3; do
    stall=""
    [ "$i" -ge 2 ] && stall="--stall-prob 0.1 --stall-ms 2500 --seed $((i+7))"
    $PY py/infer_worker.py --addr 127.0.0.1:$((W_BASE+i)) --gateway $GW --worker-id py-$i \
      --token-ms 6 --kv-cache-blocks 32 --concurrency 4 $stall >/dev/null 2>&1 &
  done
  sleep 4
}

if [ "$OPEN" = 1 ]; then
  LOAD="-rate $RATE"; MODE="open loop, Poisson arrivals at $RATE req/s"
else
  LOAD="-c $CONC"; MODE="closed loop, $CONC clients"
fi

make build >/dev/null
echo "== routing benchmark: $TRIALS trials x 4 phases, N=$N, $MODE, fresh stack per trial"
: > "$LOGDIR/results.jsonl"
for phase in A B B2 C; do
  case $phase in
    A)  flags="-prefix-routing=false" ;;
    B)  flags="-prefix-routing=true -load-factor 0" ;;
    B2) flags="-prefix-routing=true -load-factor 1.25" ;;
    C)  flags="-prefix-routing=true -load-factor 0 -hedge-after 1s" ;;
  esac
  for t in $(seq 1 "$TRIALS"); do
    start_stack "$flags"
    out=$(./bin/llmbench.exe -gateway $GW -n "$N" $LOAD -prefixes 12 -max-tokens 32 \
          -seed "$t" -timeout 60s -json) || { echo "phase $phase trial $t: llmbench failed"; exit 1; }
    printf '{"phase":"%s","trial":%d,"r":%s}\n' "$phase" "$t" "$out" >> "$LOGDIR/results.jsonl"
    echo "  $phase trial $t done"
  done
done
kill_stack

$PY - "$LOGDIR/results.jsonl" <<'PYEOF'
import json, statistics, sys
rows = [json.loads(l) for l in open(sys.argv[1])]
fields = [("req_per_s", "req/s", "{:.1f}"), ("prefix_hit_rate", "hit", "{:.2f}"),
          ("ttft_p50_ms", "p50 ms", "{:.0f}"), ("ttft_p99_ms", "p99 ms", "{:.0f}"),
          ("hedges_launched", "hedges", "{:.0f}"), ("errors", "errors", "{:.0f}")]
print()
print("| Phase | " + " | ".join(h for _, h, _ in fields) + " |")
print("|---|" + "---|" * len(fields))
for phase in ["A", "B", "B2", "C"]:
    rs = [r["r"] for r in rows if r["phase"] == phase]
    cells = []
    for key, _, fmt in fields:
        vals = [r.get(key, 0) or 0 for r in rs]
        m = statistics.mean(vals)
        sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
        cells.append(fmt.format(m) + " ± " + fmt.format(sd))
    print(f"| {phase} | " + " | ".join(cells) + " |")
print()
print("per-trial req/s:", {p: [round(r["r"]["req_per_s"], 1) for r in rows if r["phase"] == p] for p in ["A", "B", "B2", "C"]})
print("per-trial hit:  ", {p: [round(r["r"]["prefix_hit_rate"], 2) for r in rows if r["phase"] == p] for p in ["A", "B", "B2", "C"]})
PYEOF
echo "BENCH DONE"
