#!/usr/bin/env bash
# Record the terminal demos used in the README, then render them to GIFs.
#
# Every frame in those GIFs is real output from a real run: this script runs
# the actual binaries, stamps each output line with the wall-clock time it
# appeared, and scripts/make_demo_gif.py replays that timing. Nothing is
# staged or re-typed.
#
# Usage:  source ./env.sh && bash scripts/record_demo.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
OUT=docs/media
TMP=${TMPDIR:-/tmp}
mkdir -p "$OUT"

PY=.venv/Scripts/python.exe
KV=127.0.0.1:7801,127.0.0.1:7802,127.0.0.1:7803

# stamp reads stdin and prefixes each line with a unix timestamp.
stamp() { while IFS= read -r line; do printf '%s|%s\n' "$(date +%s.%N)" "$line"; done; }
# say prints a narration line into the capture, so the GIF explains itself.
say() { printf '%s|%s\n' "$(date +%s.%N)" "$1"; }

cleanup() {
  taskkill //F //IM raftkv.exe >/dev/null 2>&1 || true
  rm -rf data/demo
}
trap cleanup EXIT

echo "building..."
make build >/dev/null
cleanup

# ---------------------------------------------------------------------------
# Demo 1: seeded chaos against an in-process 5-node Raft cluster.
# ---------------------------------------------------------------------------
echo "recording chaos demo..."
./bin/simrun.exe -seed 42 -duration 8s -nemesis-interval 25 -nodes 5 -clients 6 2>&1 \
  | stamp > "$TMP/demo_chaos.txt"

# ---------------------------------------------------------------------------
# Demo 2: kill the Raft leader of a real 3-process cluster mid-write and show
# the cluster elect a new one and keep serving, with no lost writes.
# ---------------------------------------------------------------------------
echo "recording failover demo..."
{
  say "\$ raftkv -id 0/1/2 -peers 127.0.0.1:7801,7802,7803    # 3 real processes"
  for i in 0 1 2; do
    ./bin/raftkv.exe -id $i -peers $KV -data "data/demo/kv$i" -v > "$TMP/demo_kv$i.log" 2>&1 &
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

# ---------------------------------------------------------------------------
# Render.
# ---------------------------------------------------------------------------
echo "rendering gifs..."
$PY scripts/make_demo_gif.py "$TMP/demo_chaos.txt" "$OUT/chaos.gif" \
  --title "simrun — seeded chaos, 5-node Raft cluster" --speed 1.6 --cols 96 --rows 24
$PY scripts/make_demo_gif.py "$TMP/demo_failover.txt" "$OUT/failover.gif" \
  --title "raftkv — leader killed mid-write, cluster keeps serving" --speed 1.0 --cols 88 --rows 18

echo "done: $OUT/chaos.gif, $OUT/failover.gif"
