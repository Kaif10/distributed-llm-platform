# Phase 0 — Setup checklist

## Install (Windows)
- Go 1.23+: https://go.dev/dl/  Verify with `go version`
- protoc: `winget install protobuf` (or the release zip from github.com/protocolbuffers/protobuf)
- Go protobuf plugins:
  - `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
  - `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`
- make: `winget install ezwinports.make`
- golangci-lint: `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`
- Make sure `%USERPROFILE%\go\bin` is on PATH, then restart the terminal.

## Read / watch
- [ ] DDIA ch. 5, 6, 7, 8, 9
- [ ] MIT 6.5840 lectures 1–3: https://pdos.csail.mit.edu/6.824/schedule.html
- [ ] Raft paper sections 1–5, read twice: https://raft.github.io/raft.pdf
- [ ] Tour of Go, concurrency section

## Exit criterion
- `make test` runs a Go gRPC hello-world server and client test.

## Notes (write in your own words as you go)
- What is the difference between a partition and a crash, from the perspective of the node that is still running?
- Why can a node never know whether a peer is slow or dead?
