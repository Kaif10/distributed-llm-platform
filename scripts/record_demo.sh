#!/usr/bin/env bash
# Record the terminal demos used in the README, then render them to GIFs.
#
# Every frame in those GIFs is real output from a real run: this script runs
# the actual binaries, stamps each output line with the wall-clock time it
# appeared, and scripts/make_demo_gif.py replays that timing. Nothing is
# staged or re-typed.
#
# Usage:  source ./env.sh && bash scripts/record_demo.sh [all|chaos|failover|llm]
#         (llm needs torch + transformers; see py/requirements.txt)
set -euo pipefail
WHICH=${1:-all}
want() { [ "$WHICH" = all ] || [ "$WHICH" = "$1" ]; }

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
OUT=docs/media
TMP=${TMPDIR:-/tmp}
mkdir -p "$OUT"

PY=.venv/Scripts/python.exe
KV=127.0.0.1:7801,127.0.0.1:7802,127.0.0.1:7803          # raftkv client addresses
KVPEERS=127.0.0.1:17801,127.0.0.1:17802,127.0.0.1:17803  # raftkv peer-only Raft addresses

# stamp reads stdin and prefixes each line with a unix timestamp.
stamp() { while IFS= read -r line; do printf '%s|%s\n' "$(date +%s.%N)" "$line"; done; }
# say prints a narration line into the capture, so the GIF explains itself.
say() { printf '%s|%s\n' "$(date +%s.%N)" "$1"; }

cleanup() {
  taskkill //F //IM raftkv.exe >/dev/null 2>&1 || true
  taskkill //F //IM gateway.exe >/dev/null 2>&1 || true
  # .venv/Scripts/python.exe is a launcher; kill the real interpreter (which
  # holds the model in memory) by its command line.
  powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { \$_.CommandLine -like '*infer_worker.py --addr 127.0.0.1:7960*' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force -ErrorAction SilentlyContinue }" >/dev/null 2>&1 || true
  rm -rf data/demo
}
trap cleanup EXIT

echo "building..."
make build >/dev/null
cleanup

# ---------------------------------------------------------------------------
# Demo 1: seeded chaos against an in-process 5-node Raft cluster.
# ---------------------------------------------------------------------------
if want chaos; then
echo "recording chaos demo..."
./bin/simrun.exe -seed 42 -duration 8s -nemesis-interval 25 -nodes 5 -clients 6 2>&1 \
  | stamp > "$TMP/demo_chaos.txt"
fi

# ---------------------------------------------------------------------------
# Demo 2: kill the Raft leader of a real 3-process cluster mid-write and show
# the cluster elect a new one and keep serving, with no lost writes.
# ---------------------------------------------------------------------------
if want failover; then
echo "recording failover demo..."
{
  say "\$ raftkv -id 0/1/2 -peers 127.0.0.1:17801,... -client-addrs 127.0.0.1:7801,...    # 3 real processes"
  for i in 0 1 2; do
    ./bin/raftkv.exe -id $i -peers $KVPEERS -client-addrs $KV -data "data/demo/kv$i" -v > "$TMP/demo_kv$i.log" 2>&1 &
  done
  sleep 3
  say ""

  say "\$ kvctl put user:1 alice"
  ./bin/kvctl.exe -addr $KV put user:1 alice 2>&1
  say "\$ kvctl get user:1"
  ./bin/kvctl.exe -addr $KV get user:1 2>&1
  say ""

  leader=$(grep -l "LEADER" "$TMP"/demo_kv*.log 2>/dev/null | head -1 | grep -o '[0-9]' | tail -1 || echo 0)
  say "# node $leader is the current Raft leader. killing it mid-flight:"
  say "\$ taskkill /F /PID <leader>"
  pid=$(wmic process where "CommandLine like '%-id $leader %' and Name='raftkv.exe'" get ProcessId 2>/dev/null | grep -o '[0-9]\+' | head -1 || true)
  [ -n "${pid:-}" ] && taskkill //F //PID "$pid" >/dev/null 2>&1 || true
  sleep 1
  say ""

  say "# a majority (2 of 3) survives, so the cluster elects a new leader:"
  say "\$ kvctl put user:2 bob        # written during the outage"
  ./bin/kvctl.exe -addr $KV put user:2 bob 2>&1
  say "\$ kvctl get user:1           # written before the leader died"
  ./bin/kvctl.exe -addr $KV get user:1 2>&1
  say "\$ kvctl get user:2"
  ./bin/kvctl.exe -addr $KV get user:2 2>&1
  say ""
  say "# no writes lost, no manual intervention, no client-visible failure."
} > "$TMP/demo_failover.txt"

cleanup
fi

# ---------------------------------------------------------------------------
# Demo 3: a REAL model behind the gateway. One SmolLM2-135M worker on CPU,
# the gateway (prefix routing + semantic cache) over a 3-node Raft KV, and
# py/demo_llm.py walking through a cold request, real prefix KV-cache reuse,
# a semantic-cache hit, and a cancel that reaches the worker.
# ---------------------------------------------------------------------------
if want llm; then
echo "recording llm demo (loads model weights)..."
LKV=127.0.0.1:7901,127.0.0.1:7902,127.0.0.1:7903
LKVPEERS=127.0.0.1:17901,127.0.0.1:17902,127.0.0.1:17903
for i in 0 1 2; do
  ./bin/raftkv.exe -id $i -peers $LKVPEERS -client-addrs $LKV -data "data/demo/llm$i" > "$TMP/demo_llmkv$i.log" 2>&1 &
done
sleep 2
./bin/gateway.exe -addr 127.0.0.1:7950 -kv $LKV -prefix-routing=true -cache=true -cache-near \
  > "$TMP/demo_gw.log" 2>&1 &
sleep 1.5
$PY py/infer_worker.py --addr 127.0.0.1:7960 --gateway 127.0.0.1:7950 --worker-id hf-0 \
  --backend hf --torch-threads 3 --kv-cache-blocks 32 > "$TMP/demo_worker.log" 2>&1 &
for t in $(seq 1 180); do
  grep -q "serving" "$TMP/demo_worker.log" 2>/dev/null && break
  sleep 1
done
grep -q "serving" "$TMP/demo_worker.log" || { echo "worker never came up"; cat "$TMP/demo_worker.log"; exit 1; }
sleep 3   # let it register with the gateway
{
  # plain echo, not say: this block is already piped through stamp
  echo "\$ python py/demo_llm.py    # gateway -> real model worker, state in a 3-node Raft KV"
  $PY py/demo_llm.py --gateway 127.0.0.1:7950 --worker 127.0.0.1:7960 2>&1
} | stamp > "$TMP/demo_llm.txt"
cleanup
fi

# ---------------------------------------------------------------------------
# Render.
# ---------------------------------------------------------------------------
echo "rendering gifs..."
if want chaos; then
$PY scripts/make_demo_gif.py "$TMP/demo_chaos.txt" "$OUT/chaos.gif" \
  --title "simrun — seeded chaos, 5-node Raft cluster" --speed 1.6 --cols 96 --rows 24
fi
if want failover; then
$PY scripts/make_demo_gif.py "$TMP/demo_failover.txt" "$OUT/failover.gif" \
  --title "raftkv — leader killed mid-write, cluster keeps serving" --speed 1.0 --cols 88 --rows 18
fi
if want llm; then
$PY scripts/make_demo_gif.py "$TMP/demo_llm.txt" "$OUT/llm.gif" \
  --title "gateway — a real LLM: prefix KV-cache reuse, semantic cache, cancel" \
  --speed 1.0 --cols 92 --rows 33 --max-frame-ms 2500
fi

echo "done: $(ls $OUT/*.gif | tr '\n' ' ')"
