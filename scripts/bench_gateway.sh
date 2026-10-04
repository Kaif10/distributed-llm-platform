#!/usr/bin/env bash
# How many requests per second can the GATEWAY sustain, and what limits it?
#
# Workers here are Go mocks with ~zero prefill and token latency, so they are
# never the bottleneck. What remains on the request path is the gateway's own
# work against the Raft KV:
#
#   rate limiting   a Get + a CAS on the tenant's bucket     (2 consensus ops)
#   semantic cache  a Get on lookup, a Put on store           (2 more, if on)
#
# Every one of those is a full Raft round today (reads go through the log,
# DESIGN.md §4.8), so this measures what consensus on the hot path costs.
# Each scenario runs TRIALS times on a fresh gateway and reports every trial
# plus the mean, so run-to-run noise is visible rather than hidden.
#
# Usage:  source ./env.sh && bash scripts/bench_gateway.sh [TRIALS=3] [N=2000] [C=64]
set -euo pipefail

TRIALS=${1:-3}
N=${2:-2000}
C=${3:-64}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
LOGDIR=${LOGDIR:-${TMPDIR:-/tmp}/bench_gateway}
rm -rf "$LOGDIR" data/benchgw; mkdir -p "$LOGDIR"

KV=127.0.0.1:7701,127.0.0.1:7702,127.0.0.1:7703          # raftkv client addresses
KVPEERS=127.0.0.1:17701,127.0.0.1:17702,127.0.0.1:17703  # raftkv peer-only Raft addresses
GW=127.0.0.1:7750
EXTRA_GW_FLAGS=${EXTRA_GW_FLAGS:-}

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  taskkill //F //IM raftkv.exe     >/dev/null 2>&1 || true
  taskkill //F //IM gateway.exe    >/dev/null 2>&1 || true
  taskkill //F //IM mockworker.exe >/dev/null 2>&1 || true
  rm -rf data/benchgw
}
trap cleanup EXIT
fail() { echo "BENCH FAIL: $*" >&2; exit 1; }
num() { sed -n "s/.*\"$1\":\([0-9.]*\).*/\1/p" <<<"$2" | head -1; }

make build >/dev/null

for i in 0 1 2; do
  ./bin/raftkv.exe -id $i -peers $KVPEERS -client-addrs $KV -data data/benchgw/kv$i >"$LOGDIR/kv$i.log" 2>&1 &
  pids+=($!)
done
sleep 2
./bin/kvctl.exe -addr $KV put bench-probe ok >/dev/null || fail "kv not serving"

start_gateway() { # $1 = flags
  taskkill //F //IM gateway.exe >/dev/null 2>&1 || true
  sleep 0.5
  ./bin/gateway.exe -addr $GW -kv $KV -rate 1000000 -burst 1000000 -max-inflight 1000 \
    -stats-every 0 -metrics-addr 127.0.0.1:0 $EXTRA_GW_FLAGS $1 >>"$LOGDIR/gw.log" 2>&1 &
  pids+=($!)
  sleep 1.5
}

start_gateway "-cache=false"
for i in 0 1; do
  ./bin/mockworker.exe -addr 127.0.0.1:$((7760+i)) -gateway $GW -id mock-$i \
    -prefill-ms 0.01 -prefill-per-char 0.0001 -token-ms 0.01 >"$LOGDIR/w$i.log" 2>&1 &
  pids+=($!)
done
sleep 3

run_scenario() { # $1 = name, $2 = gateway flags, $3 = llmbench flags
  local name=$1 sum=0 vals=""
  for t in $(seq 1 "$TRIALS"); do
    start_gateway "$2"
    sleep 2   # workers re-register with the fresh gateway
    out=$(./bin/llmbench.exe -gateway $GW -n "$N" -c "$C" -tenants 16 -prefixes 8 \
          -max-tokens 4 -timeout 60s $3 -json) || fail "$name trial $t: llmbench failed"
    rps=$(num req_per_s "$out"); p50=$(num ttft_p50_ms "$out"); p99=$(num ttft_p99_ms "$out")
    errs=$(num errors "$out")
    [ "${errs:-0}" = "0" ] || fail "$name trial $t: $errs errors: $(grep -o '"error_kinds":{[^}]*}' <<<"$out")"
    printf '  %-26s trial %d: %7.1f req/s   ttft p50 %6.1f ms  p99 %7.1f ms\n' "$name" "$t" "$rps" "$p50" "$p99"
    sum=$(awk -v a="$sum" -v b="$rps" 'BEGIN{print a+b}')
    vals="$vals $rps"
  done
  awk -v s="$sum" -v n="$TRIALS" -v name="$name" -v vals="$vals" 'BEGIN{
    m=s/n; split(vals,v," "); ss=0; for(i in v){ss+=(v[i]-m)^2}
    sd=(n>1)?sqrt(ss/(n-1)):0
    printf "  %-26s mean    : %7.1f req/s  (sd %.1f over %d trials)\n", name, m, sd, n }'
}

echo "== gateway ceiling: N=$N per trial, C=$C concurrent, $TRIALS trials, workers ~instant"
run_scenario "limiter only"         "-cache=false"  ""
run_scenario "limiter + cache"      "-cache=true"   "-no-cache=false"
echo "BENCH DONE"
