// raftkv runs one replica of the Raft-replicated KV store.
//
// A three-node cluster on one machine:
//
//	raftkv -id 0 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -data data/r0
//	raftkv -id 1 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -data data/r1
//	raftkv -id 2 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -data data/r2
//
// Each process serves both the Raft RPCs (peer traffic) and the KV API
// (client traffic) on its own address. Clients can talk to any node; a
// follower answers "not leader" with a hint and kvctl follows it.
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
	"time"

	"google.golang.org/grpc"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/raftkv"
	"dsys/raft"
	"dsys/raft/grpctransport"
)

func main() {
	id := flag.Int("id", 0, "this node's index into -peers")
	peersFlag := flag.String("peers", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated addresses of all nodes, in id order")
	dataDir := flag.String("data", "", "data directory (default data/r<id>)")
	maxRaftState := flag.Int("maxraftstate", 1<<20, "snapshot when Raft state exceeds this many bytes (0 = never)")
	heartbeat := flag.Duration("heartbeat", 100*time.Millisecond, "leader heartbeat interval")
	electMin := flag.Duration("election-min", 500*time.Millisecond, "minimum election timeout")
	electMax := flag.Duration("election-max", 1000*time.Millisecond, "maximum election timeout")
	verbose := flag.Bool("v", false, "log Raft state transitions")
	flag.Parse()

	addrs := strings.Split(*peersFlag, ",")
	if *id < 0 || *id >= len(addrs) {
		log.Fatalf("-id %d out of range for %d peers", *id, len(addrs))
	}
	if *dataDir == "" {
		*dataDir = "data/r" + itoa(*id)
	}

	persister, err := raft.OpenFilePersister(*dataDir)
	if err != nil {
		log.Fatalf("open persister: %v", err)
	}

	peers := make([]raft.Peer, len(addrs))
	for i, a := range addrs {
		if i != *id {
			peers[i] = grpctransport.NewPeer(a, grpctransport.WithTimeout(*heartbeat*2))
		}
	}

	rcfg := raft.Config{HeartbeatInterval: *heartbeat, ElectionTimeoutMin: *electMin, ElectionTimeoutMax: *electMax}
	if *verbose {
		rcfg.Logf = raft.StdLogger
	}
	srv := raftkv.New(peers, *id, persister, raftkv.Config{
		MaxRaftState: *maxRaftState,
		Addrs:        addrs,
		Raft:         rcfg,
	})

	lis, err := net.Listen("tcp", addrs[*id])
	if err != nil {
		log.Fatalf("listen %s: %v", addrs[*id], err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
	grpctransport.Register(gs, srv.Raft()) // peer-facing
	kvv1.RegisterKVServer(gs, srv)         // client-facing

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Print("shutting down")
		srv.Kill()
		gs.Stop()
	}()

	log.Printf("raftkv node %d listening on %s (data %s, persisted %d bytes)", *id, lis.Addr(), *dataDir, persister.StateSize())
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }
