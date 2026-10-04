// shardkv runs one replica of one shard-owning Raft group.
//
// It mirrors cmd/raftkv closely, with two additions: this replica belongs
// to a group identified by -gid (several such groups, each with their own
// -peers, together make up the sharded cluster), and it talks to a shard
// controller cluster (-ctrl) to learn and follow the current shard
// assignment.
//
// A two-group, three-replica-each cluster on one machine, with a shard
// controller already running at 127.0.0.1:6001-6003:
//
//	shardkv -gid 1 -id 0 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -ctrl 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003
//	shardkv -gid 1 -id 1 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -ctrl 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003
//	shardkv -gid 1 -id 2 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -ctrl 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003
//	shardkv -gid 2 -id 0 -peers 127.0.0.1:7011,127.0.0.1:7012,127.0.0.1:7013 -ctrl 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003
//	... (id 1, 2 for gid 2 likewise)
//
// Each process serves three gRPC services on one listener: this group's own
// Raft RPCs (peer traffic within the group), the client-facing KV API, and
// the ShardMigration service that lets OTHER groups pull a shard's data out
// of this one. Clients (kvctl -ctrl ...) discover which group owns a key's
// shard from the controller and talk to that group's -peers addresses
// directly; a follower answers "not leader" with a hint, same as raftkv.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	kvv1 "dsys/gen/kv/v1"
	"dsys/raft"
	raftgrpctransport "dsys/raft/grpctransport"
	"dsys/shardkv"
	shardkvgrpctransport "dsys/shardkv/grpctransport"
)

func main() {
	gid := flag.Int64("gid", 0, "this replica's group id (required)")
	id := flag.Int("id", 0, "this replica's index within its OWN group's -peers list")
	peersFlag := flag.String("peers", "", "comma-separated addresses of this replica's OWN group, in id order (required)")
	ctrlFlag := flag.String("ctrl", "", "comma-separated addresses of the shard controller cluster (required)")
	dataDir := flag.String("data", "", "data directory (default data/shard-g<gid>-r<id>)")
	maxRaftState := flag.Int("maxraftstate", 1<<20, "snapshot when Raft state exceeds this many bytes (0 = never)")
	heartbeat := flag.Duration("heartbeat", 100*time.Millisecond, "leader heartbeat interval")
	electMin := flag.Duration("election-min", 500*time.Millisecond, "minimum election timeout")
	electMax := flag.Duration("election-max", 1000*time.Millisecond, "maximum election timeout")
	pollInterval := flag.Duration("poll-interval", 100*time.Millisecond, "how often to poll the controller for a new config and retry pending shard pulls")
	verbose := flag.Bool("v", false, "log Raft state transitions")
	flag.Parse()

	if *gid <= 0 {
		log.Fatal("-gid is required and must be positive")
	}
	if *peersFlag == "" {
		log.Fatal("-peers is required")
	}
	if *ctrlFlag == "" {
		log.Fatal("-ctrl is required")
	}

	addrs := splitAddrs(*peersFlag)
	if *id < 0 || *id >= len(addrs) {
		log.Fatalf("-id %d out of range for %d peers", *id, len(addrs))
	}
	ctrlAddrs := splitAddrs(*ctrlFlag)

	if *dataDir == "" {
		*dataDir = "data/shard-g" + strconv.FormatInt(*gid, 10) + "-r" + strconv.Itoa(*id)
	}

	persister, err := raft.OpenFilePersister(*dataDir)
	if err != nil {
		log.Fatalf("open persister: %v", err)
	}

	peers := make([]raft.Peer, len(addrs))
	for i, a := range addrs {
		if i != *id {
			peers[i] = raftgrpctransport.NewPeer(a, raftgrpctransport.WithTimeout(*heartbeat*2))
		}
	}

	rcfg := raft.Config{HeartbeatInterval: *heartbeat, ElectionTimeoutMin: *electMin, ElectionTimeoutMax: *electMax}
	if *verbose {
		rcfg.Logf = raft.StdLogger
	}

	ctrl := shardkvgrpctransport.NewController(ctrlAddrs, shardkvgrpctransport.WithControllerTimeout(*pollInterval))
	defer ctrl.Close()
	fetcher := shardkvgrpctransport.NewFetcher(shardkvgrpctransport.WithTimeout(*pollInterval))
	defer fetcher.Close()

	srv := shardkv.New(peers, *id, persister, shardkv.Options{
		GID:          *gid,
		Addrs:        addrs,
		Ctrl:         ctrl,
		Fetcher:      fetcher,
		PollInterval: *pollInterval,
		MaxRaftState: *maxRaftState,
		Raft:         rcfg,
	})

	lis, err := net.Listen("tcp", addrs[*id])
	if err != nil {
		log.Fatalf("listen %s: %v", addrs[*id], err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(raftgrpctransport.MaxMessageSize))
	raftgrpctransport.Register(gs, srv.Raft()) // peer-facing (this group's own Raft)
	kvv1.RegisterKVServer(gs, srv)             // client-facing
	shardkvgrpctransport.Register(gs, srv)     // other-groups-facing (shard migration)

	// SIGTERM too: it is what `docker stop` (and systemd, k8s) sends, and
	// without it the graceful path below never runs in a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Print("shutting down")
		srv.Kill()
		gs.Stop()
	}()

	log.Printf("shardkv group %d replica %d listening on %s (data %s, persisted %d bytes, controller %v)",
		*gid, *id, lis.Addr(), *dataDir, persister.StateSize(), ctrlAddrs)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
