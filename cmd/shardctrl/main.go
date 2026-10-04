// shardctrl runs one replica of the Raft-replicated shard controller.
//
// A three-node cluster on one machine:
//
//	shardctrl -id 0 -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 -data data/ctrl0
//	shardctrl -id 1 -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 -data data/ctrl1
//	shardctrl -id 2 -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 -data data/ctrl2
//
// Each process serves both the Raft RPCs (peer traffic) and the ShardCtrl
// API (operator/shardkv-poller traffic) on its own address. Clients can
// talk to any node; a follower answers "not leader" with a hint and
// shardctrl.Client follows it, exactly like kvctl follows raftkv's hint.
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

	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/raft"
	"dsys/raft/grpctransport"
	"dsys/shardctrl"
)

func main() {
	id := flag.Int("id", 0, "this node's index into -peers")
	peersFlag := flag.String("peers", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "comma-separated addresses of all nodes, in id order")
	dataDir := flag.String("data", "", "data directory (default data/ctrl<id>)")
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
		*dataDir = "data/ctrl" + strconv.Itoa(*id)
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
	srv := shardctrl.New(peers, *id, persister, shardctrl.Config{
		Addrs: addrs,
		Raft:  rcfg,
	})

	lis, err := net.Listen("tcp", addrs[*id])
	if err != nil {
		log.Fatalf("listen %s: %v", addrs[*id], err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
	grpctransport.Register(gs, srv.Raft())       // peer-facing
	shardctrlv1.RegisterShardCtrlServer(gs, srv) // client-facing

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

	log.Printf("shardctrl node %d listening on %s (data %s, persisted %d bytes)", *id, lis.Addr(), *dataDir, persister.StateSize())
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
