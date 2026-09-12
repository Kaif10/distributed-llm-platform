# Run `. .\env.ps1` (PowerShell) or `source ./env.sh` (Git Bash) first.

.PHONY: all proto build test test-crash test-exercises test-raft test-lin lint tidy clean

all: proto build test

proto:
	protoc -I proto \
		--go_out=. --go_opt=module=dsys \
		--go-grpc_out=. --go-grpc_opt=module=dsys \
		proto/kv/v1/kv.proto \
		proto/raft/v1/raft.proto

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

lint:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -rf bin
