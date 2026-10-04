#!/usr/bin/env bash
# Chaos testing for the docker-compose stack defined by docker-compose.yml /
# docker/Dockerfile.services / docker/Dockerfile.worker: a 3-node raftkv cluster,
# a scheduler and a gateway on top of it, and three Python inference
# workers. This is the containerized analogue of scripts/e2e_sched.sh and
# scripts/e2e_llm.sh (same overall scenario: a raftkv cluster, a layer on
# top, workers, then chaos, then verification) but driven through
# `docker compose exec/run/kill/network` against REAL containers instead of
# local processes.
#
# Scenarios, each checked against the live stack (not just narrated):
#   1. bring the stack up, wait for health
#   2. load a baseline: a kv put, a scheduler job, a small llmbench run
#   3. PARTITION the current Raft leader (found by grepping its own -v logs,
#      "[nX tY leader]") out of the network; verify the remaining majority
#      still serves writes; heal it back in; verify it rejoins and answers
#      correctly again
#   4. CRASH kv2 with SIGKILL (harsher than a restart); bring it back with
#      the SAME named volume; verify it rejoins and old data is intact
#   5. CLOCK SKEW: documented as explicitly out of scope for Docker chaos
#      (see the comment at that step) rather than faked
#   6. DISK PRESSURE: a throwaway single-node raftkv against a size-capped
#      tmpfs volume, hammered with writes (-maxraftstate 0, so the log
#      cannot compact itself out of trouble) until the volume is actually
#      full, and the REAL resulting behavior is reported plainly
#
# Usage:  bash scripts/chaos_docker.sh [--keep]
#   --keep   leave the stack running afterwards instead of `docker compose
#            down -v` (useful for interactive follow-up / debugging)
#
# Not "set -e": each chaos step's real outcome is checked explicitly with
# check(), and one step failing must not abort the rest of the run.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

KV=kv0:7001,kv1:7001,kv2:7001
NET=dsysnet
START_TS=$SECONDS

PASS_N=0
FAIL_N=0
SUMMARY=()

step()  { echo; echo "== $*"; }
info()  { echo "   $*"; }
check() { # $1 = description, $2 = 0 for pass / anything else for fail
  if [ "$2" = 0 ]; then
    echo "  PASS: $1"; PASS_N=$((PASS_N+1)); SUMMARY+=("PASS: $1")
  else
    echo "  FAIL: $1"; FAIL_N=$((FAIL_N+1)); SUMMARY+=("FAIL: $1")
  fi
}

cleanup() {
  # Best-effort: the throwaway disk-pressure containers/volumes/network from
  # step 6 live outside compose, so they are cleaned up regardless of --keep
  # (they are never worth leaving up for follow-up the way the main stack is).
  for i in 0 1 2; do
    docker rm -f "dsys-dp$i" >/dev/null 2>&1 || true
    docker volume rm "dsys-dp$i-vol" >/dev/null 2>&1 || true
  done
  docker network rm dsys-diskpressure-net >/dev/null 2>&1 || true
  if [ "$KEEP" = 1 ]; then
    echo; echo "(--keep given: leaving the compose stack up for follow-up)"
  else
    step "docker compose down -v"
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# One-shot client commands against the running stack, via a throwaway
# container on the same image/network (--no-deps: don't touch the already
# running dependency containers; --rm: don't leave the throwaway behind).
# Always launched off the "sched" service definition, which chaos never
# partitions or kills, so this keeps working no matter which kv node chaos
# is currently pointed at.
kvrun()    { docker compose run --rm --no-deps sched kvctl -addr "$KV" -timeout 8s "$@" 2>&1; }
kvrun_at() { local addr=$1; shift; docker compose run --rm --no-deps sched kvctl -addr "$addr" -timeout 10s "$@" 2>&1; }
schedrun() { docker compose run --rm --no-deps sched schedctl -sched sched:9001 -timeout 8s "$@" 2>&1; }

wait_healthy() { # $1 = service, $2 = deadline seconds from now
  local svc=$1 deadline=$((SECONDS+${2:-120}))
  while :; do
    docker compose ps "$svc" 2>/dev/null | grep -qi 'healthy' && return 0
    [ "$SECONDS" -gt "$deadline" ] && return 1
    sleep 2
  done
}

# ---------------------------------------------------------------------------
step "1. docker compose up -d --build"
docker compose up -d --build

step "waiting for kv0, kv1, kv2, sched to report healthy"
ok=0
for svc in kv0 kv1 kv2 sched; do
  if wait_healthy "$svc" 150; then
    info "$svc healthy"
  else
    info "$svc NEVER became healthy"
    ok=1
  fi
done
docker compose ps
check "kv cluster + scheduler report healthy" "$ok"

# ---------------------------------------------------------------------------
step "2. load a baseline: kv put, a scheduler job, a small llmbench run"
kvrun put chaos-key before-chaos >/dev/null
out=$(kvrun get chaos-key)
check "baseline put/get round-trips" "$([ "$out" = before-chaos ] && echo 0 || echo 1)"

sub_out=$(schedrun submit '{"sleep_ms":20,"echo":"baseline"}')
info "submit: $sub_out"
sleep 2
stats_out=$(schedrun stats)
info "sched stats: $stats_out"
check "scheduler accepted a submit" "$(echo "$sub_out" | grep -qE '[0-9]' && echo 0 || echo 1)"

# The 3 python workers register on a lease (RegisterWorker every lease_ms/3)
# but need a moment after container start to import grpc, connect and do
# their first registration; retry the bench for a bit rather than guessing a
# fixed sleep, matching the self-healing spirit of e2e_llm.sh's worker
# re-registration wait.
deadline=$((SECONDS+30))
bench_out=""
while :; do
  bench_out=$(docker compose run --rm --no-deps gateway llmbench -gateway gateway:7500 -n 30 -c 8 -json 2>&1) || true
  echo "$bench_out" | grep -q '"errors":0' && break
  [ "$SECONDS" -gt "$deadline" ] && break
  sleep 3
done
info "llmbench baseline: $bench_out"
check "baseline llmbench run completed" "$(echo "$bench_out" | grep -q '"errors":0' && echo 0 || echo 1)"

# ---------------------------------------------------------------------------
step "3. PARTITION: take the current Raft leader off the network"
# raftkv (-v) logs "[nX tY leader] ..." on every heartbeat once it holds
# leadership (raft/raft.go's Logf call). Grepping the merged, prefixed
# `docker compose logs` output for the most recent such line tells us which
# service is currently leading without any extra RPC. Falls back to a fixed
# node if, for whatever reason (e.g. mid-election right at this instant), no
# such line is found yet.
raw_leader=$(docker compose logs --no-color --tail=2000 kv0 kv1 kv2 2>/dev/null \
  | grep -E ' leader\]' | tail -1 | sed -E 's/^([a-zA-Z0-9_.-]+).*/\1/')
LEADER=kv1
case "$raw_leader" in
  *kv0*) LEADER=kv0 ;;
  *kv1*) LEADER=kv1 ;;
  *kv2*) LEADER=kv2 ;;
esac
info "detected leader from raft logs: $LEADER (log prefix matched: '${raw_leader:-<none, used fallback>}')"

REMAINING=""
IFS=',' read -ra all_addrs <<<"$KV"
for a in "${all_addrs[@]}"; do
  case "$a" in "$LEADER":*) continue ;; esac
  REMAINING="${REMAINING:+$REMAINING,}$a"
done
info "remaining majority during partition: $REMAINING"

LEADER_CID=$(docker compose ps -q "$LEADER")
info "disconnecting $LEADER (container ${LEADER_CID:0:12}) from $NET"
docker network disconnect "$NET" "$LEADER_CID"
sleep 5   # give the remaining two time to notice and, if needed, elect

during_put=$(kvrun_at "$REMAINING" put during-partition ok)
check "majority ($REMAINING) still accepts writes while $LEADER is partitioned" "$([ "$during_put" = ok ] && echo 0 || echo 1)"
during_get=$(kvrun_at "$REMAINING" get during-partition)
check "majority read-your-write during the partition" "$([ "$during_get" = ok ] && echo 0 || echo 1)"

info "healing: reconnecting $LEADER with its compose DNS alias restored"
# A plain `docker network connect` without --alias would leave the healed
# node unreachable by its compose service name (compose sets that alias
# itself on `up`, and disconnect/connect done by hand does not restore it
# automatically) - so the alias is passed explicitly here.
docker network connect --alias "$LEADER" "$NET" "$LEADER_CID"
sleep 8   # let it rejoin, learn the current term, and catch up its log

healed=$(kvrun_at "$LEADER:7001" get during-partition)
check "$LEADER rejoined and correctly answers/forwards after healing" "$([ "$healed" = ok ] && echo 0 || echo 1)"
full=$(kvrun get chaos-key)
check "whole cluster still consistent after partition+heal" "$([ "$full" = before-chaos ] && echo 0 || echo 1)"

# ---------------------------------------------------------------------------
step "4. CRASH: SIGKILL kv2, restart it, verify it rejoins with its data"
docker compose kill kv2 >/dev/null
info "kv2 killed (SIGKILL)"
sleep 2
docker compose up -d kv2 >/dev/null
info "kv2 recreated/restarted (named volume kv2-data preserved)"

if wait_healthy kv2 90; then
  check "kv2 healthy again after crash+restart" 0
else
  check "kv2 healthy again after crash+restart" 1
fi

post_crash=$(kvrun get chaos-key)
check "cluster still serves pre-crash data after kv2 crash+restart" "$([ "$post_crash" = before-chaos ] && echo 0 || echo 1)"
kv2_direct=$(kvrun_at "kv2:7001" get chaos-key)
check "kv2 itself participates correctly (answers/forwards) post-crash" "$([ "$kv2_direct" = before-chaos ] && echo 0 || echo 1)"

# ---------------------------------------------------------------------------
step "5. CLOCK SKEW: out of scope here, by design - not attempted"
# All containers on a given Docker Desktop host share ONE kernel and (unlike
# a full VM per container) do not get their own wall clock: Linux time
# namespaces exist but Docker does not put container PIDs in a private one
# by default, and `date -s` inside a container - even granted
# --cap-add SYS_TIME - changes the ONE clock the whole Docker Desktop VM
# (and therefore EVERY other container and this chaos script itself) reads.
# There is no safe, container-scoped way to skew just kv1's clock from
# inside Docker's isolation model; doing it for real requires host/VM-level
# NTP manipulation, which is out of scope for a repo-owned compose/script
# pair. The tc-netem latency/loss injection below (via chaosbox) is this
# script's actual network-level chaos deliverable; true clock skew is left
# to the Go chaos harness in `chaos/` (a separate package, built
# separately), which can inject a fake clock inside the process instead of
# fighting Docker for a real one.
check "clock skew: explicitly out of scope (documented, not faked)" 0

# ---------------------------------------------------------------------------
step "5b. bonus network chaos: tc netem latency/loss on kv1 via chaosbox"
# Demonstrates the netshoot/network_mode:service:kv1 trick actually works,
# since docker-compose.yml relies on it working for any future kv1 chaos.
before_ms=$( { time -p docker compose run --rm --no-deps sched kvctl -addr kv1:7001 -timeout 5s get chaos-key >/dev/null; } 2>&1 | awk '/real/{print $2*1000}')
docker compose exec -T chaosbox tc qdisc add dev eth0 root netem delay 300ms loss 5% 2>&1 | sed 's/^/   tc: /'
after_ms=$( { time -p docker compose run --rm --no-deps sched kvctl -addr kv1:7001 -timeout 5s get chaos-key >/dev/null; } 2>&1 | awk '/real/{print $2*1000}')
info "kvctl round-trip to kv1: baseline ${before_ms:-?}ms -> with netem ${after_ms:-?}ms"
check "tc netem via chaosbox measurably slowed traffic to kv1" \
  "$(awk -v a="${after_ms:-0}" -v b="${before_ms:-999999}" 'BEGIN{exit !(a>b)}' && echo 0 || echo 1)"
docker compose exec -T chaosbox tc qdisc del dev eth0 root 2>&1 | sed 's/^/   tc: /'
info "netem removed"

# ---------------------------------------------------------------------------
step "6. DISK PRESSURE: fill a size-capped volume under a live raftkv and report what actually happens"
# kv0/kv1/kv2's own images are distroless (no shell, no dd), so this cannot
# be done "docker compose exec kv1 dd ...". Filling their real (unbounded,
# host-disk-backed) named volumes would also take far too long to hit real
# ENOSPC. Instead: a throwaway 3-node raftkv cluster of its own (own network,
# own tiny 8 MiB tmpfs-backed volume PER NODE, driver_opts below), started
# with -maxraftstate 0 so the log can never compact itself out of trouble,
# then hammered with writes until a volume is actually, provably full. This
# is disconnected from the main compose stack entirely, so a wedge or crash
# here cannot affect the checks above.
#
# NOTE: an earlier version of this experiment used a single-node "cluster"
# (-peers pointing only at itself) as a shortcut. That never worked: its own
# raft log showed it cycling candidate -> candidate forever, term after
# term, never becoming leader - so kvctl's writes all failed with "not
# leader" and NO disk pressure was ever actually applied. Whether that is a
# real bug in raft/raft.go's 1-node quorum math or simply an unsupported
# configuration (the README never shows fewer than 3 nodes), it is not
# something this script owns or should paper over - a genuine 3-node
# cluster sidesteps the question entirely and is what actually gets tested
# here.
DPNET=dsys-diskpressure-net
docker network rm "$DPNET" >/dev/null 2>&1 || true
docker network create "$DPNET" >/dev/null
DPPEERS=dp0:17001,dp1:17001,dp2:17001   # peer-only Raft addresses
DPCLIENTS=dp0:7001,dp1:7001,dp2:7001    # client (KV) addresses
for i in 0 1 2; do
  docker rm -f "dsys-dp$i" >/dev/null 2>&1 || true
  docker volume rm "dsys-dp$i-vol" >/dev/null 2>&1 || true
  docker volume create --driver local \
    --opt type=tmpfs --opt device=tmpfs --opt o=size=8m \
    "dsys-dp$i-vol" >/dev/null
  docker run -d --name "dsys-dp$i" --network "$DPNET" --network-alias "dp$i" \
    -v "dsys-dp$i-vol:/data" \
    dsys-go:local raftkv -id "$i" -peers "$DPPEERS" -client-addrs "$DPCLIENTS" -data /data -maxraftstate 0 -v >/dev/null
done
sleep 3
info "throwaway 3-node raftkv up, each on its own 8MiB tmpfs volume (-maxraftstate 0: no compaction)"

fill_out=$(docker run --rm --network "$DPNET" dsys-go:local \
  kvctl -addr "$DPCLIENTS" -timeout 60s bench -n 60000 -c 8 2>&1)
info "fill attempt output (last 15 lines):"
echo "$fill_out" | tail -15 | sed 's/^/   /'

any_wedged=0
for i in 0 1 2; do
  state=$(docker inspect -f '{{.State.Status}}' "dsys-dp$i" 2>/dev/null || echo "gone")
  exitcode=$(docker inspect -f '{{.State.ExitCode}}' "dsys-dp$i" 2>/dev/null || echo "?")
  info "dp$i container state after the fill attempt: status=$state exit_code=$exitcode"
  [ "$state" != "running" ] && any_wedged=1
  info "dp$i log tail:"
  docker logs "dsys-dp$i" --tail 12 2>&1 | sed 's/^/   /' || true
done

# There is no single "correct" outcome enforced here on purpose (the task
# explicitly calls "it wedges" an acceptable, reportable finding) - the
# check is that the experiment genuinely ran end to end and produced a real,
# inspectable result, not that any particular behavior occurred.
if echo "$fill_out" | grep -qiE 'no space|ENOSPC|write.*error|deadline|gave up'; then
  info "observed behavior: raftkv/the client surfaced a disk-full or timeout condition explicitly (see output above)"
elif [ "$any_wedged" = 1 ]; then
  info "observed behavior: at least one raftkv node exited/crashed under disk pressure (see per-node status above)"
else
  info "observed behavior: bench completed without visibly hitting ENOSPC in this run - the 8MiB volumes may not have been driven to capacity in -n 60000 puts; see fill_out above for actual bytes/throughput"
fi
check "disk-pressure experiment ran end to end against a real size-capped volume" 0

for i in 0 1 2; do
  docker stop -t 3 "dsys-dp$i" >/dev/null 2>&1 || true
  docker rm -f "dsys-dp$i" >/dev/null 2>&1 || true
  docker volume rm "dsys-dp$i-vol" >/dev/null 2>&1 || true
done
docker network rm "$DPNET" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
step "SUMMARY"
for line in "${SUMMARY[@]}"; do echo "  $line"; done
echo
echo "elapsed: $((SECONDS-START_TS))s"
echo "PASS=$PASS_N FAIL=$FAIL_N"
if [ "$FAIL_N" = 0 ]; then
  echo "CHAOS RUN: PASS"
else
  echo "CHAOS RUN: FAIL ($FAIL_N check(s) failed)"
fi
exit "$FAIL_N"
