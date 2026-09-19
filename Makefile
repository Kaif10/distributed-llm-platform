# Run `. .\env.ps1` (PowerShell) or `source ./env.sh` (Git Bash) first.

.PHONY: all proto build test test-crash test-exercises test-raft test-lin test-shard test-sched e2e-sched test-llm e2e-llm test-chaos chaos-docker obs-up obs-down lint tidy clean

all: proto build test

proto:
	protoc -I proto \
		--go_out=. --go_opt=module=dsys \
		--go-grpc_out=. --go-grpc_opt=module=dsys \
		proto/kv/v1/kv.proto \
		proto/raft/v1/raft.proto \
		proto/shardctrl/v1/shardctrl.proto \
		proto/shardkv/v1/shardkv.proto \
		proto/sched/v1/sched.proto 		proto/infer/v1/infer.proto 		proto/gateway/v1/gateway.proto

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

# The Phase 1 exit criterion: many kill-mid-write iterations.
test-crash:
	go test -count=1 -run TestCrashRecovery -v ./kv/wal/ -crash-iterations=200

# Phase 1 exercise tests (CAS, idempotent retries, group commit).
test-exercises:
	go test -race -count=1 -v -run 'TestCAS|TestDuplicate|TestDedup|TestGroupCommit|TestClose|TestStale' ./kv/store/

# Phase 2: the Raft suite (3A election, 3B replication, 3C persistence, 3D snapshots).
test-raft:
	go test -race -count=1 -v ./raft/ 2>&1 | grep -E "^(--- |ok|FAIL|panic)"

# Phase 2 exit criterion: random clients + partitions + crashes, checked by Porcupine.
test-lin:
	go test -race -count=1 -run TestLinearizability -v ./kv/raftkv/

# Phase 3 exit criterion: rebalancing + migration under a Porcupine check.
test-shard:
	go test -race -count=1 -v ./shardctrl/... ./shardkv/...

# Phase 4 exit criterion: exactly-once completion under >=1000 random pauses/crashes,
# plus the queue over a real Raft KV with a leader outage.
test-sched:
	go test -race -count=1 -v ./sched/ -chaos-events=1000
	go test -race -count=1 ./sched/grpcserver/ ./sched/kvadapter/ ./sched/worker/ ./kv/client/

# Phase 4 end to end: real raftkv + sched + worker processes, a crashed worker,
# a zombie worker that must be fenced, every job DONE.
e2e-sched:
	bash scripts/e2e_sched.sh 200

# Phase 5: gateway, rate limiter, router, semantic cache, mock worker.
test-llm:
	go test -race -count=1 -v ./gateway/ ./ratelimit/ ./router/ ./semcache/ ./infer/...

# Phase 5 exit criterion: real raftkv + gateway + 4 Python inference workers;
# measures prefix routing and hedging against their baselines and CHECKS the
# result, plus rate limiting, semantic cache and cancellation.
e2e-llm:
	bash scripts/e2e_llm.sh 240

# Phase 6: the deterministic-ish chaos harness. -short runs the quick control
# only; the full run is 12 seeds x KV+scheduler(+gateway) chaos.
test-chaos:
	go test -race -count=1 -v ./chaos/...

# Phase 6: one seeded chaos run against the in-process harness, verbose.
simrun:
	go run ./cmd/simrun -seed 42 -duration 10s -nemesis-interval 30 -gateway

# Phase 6: chaos against REAL containers (Docker Desktop must be running):
# partitions, crashes, and disk pressure via docker network/kill/exec.
chaos-docker:
	bash scripts/chaos_docker.sh

# Phase 6: bring up Prometheus + Grafana + Jaeger for tracing/metrics.
# See docker-compose.observability.yml's header comment for standalone vs
# joined-with-docker-compose.yml usage.
obs-up:
	docker compose -f docker-compose.observability.yml up -d

obs-down:
	docker compose -f docker-compose.observability.yml down -v

lint:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -rf bin
