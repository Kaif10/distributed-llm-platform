// shardctrl runs one replica of the Raft-replicated shard controller.
//
// A three-node cluster on one machine:
//
//	shardctrl -id 0 -peers 127.0.0.1:18001,127.0.0.1:18002,127.0.0.1:18003 -client-addrs 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 -data data/ctrl0
//	shardctrl -id 1 -peers 127.0.0.1:18001,127.0.0.1:18002,127.0.0.1:18003 -client-addrs 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 -data data/ctrl1
//	shardctrl -id 2 -peers 127.0.0.1:18001,127.0.0.1:18002,127.0.0.1:18003 -client-addrs 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 -data data/ctrl2
//
// (those address lists are the defaults).
//
// Each process listens twice: Raft RPCs (peer traffic) on -peers[id], the
// ShardCtrl API and gRPC health (operator / shardkv-poller traffic) on
// -client-addrs[id]. The Raft service is not registered on the client port;
// keep the peer port reachable by the other controller replicas only.
// Everything that talks to the controller (shardctl -ctrl, kvctl -ctrl,
// shardkv -ctrl, sched/gateway -ctrl) uses the -client-addrs list; a
// follower answers "not leader" with a hint from that list and
// shardctrl.Client follows it, exactly like kvctl follows raftkv's hint.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dsys/cmd/internal/node"
	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/raft"
	"dsys/raft/grpctransport"
	"dsys/shardctrl"
)

func main() {
	id := flag.Int("id", 0, "this node's index into -peers and -client-addrs")
	peersFlag := flag.String("peers", "127.0.0.1:18001,127.0.0.1:18002,127.0.0.1:18003",
		"PEER-ONLY Raft addresses of all controller nodes, in id order; this node serves Raft on peers[id]. Unauthenticated: make it reachable by the other replicas only")
	clientsFlag := flag.String("client-addrs", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003",
		"client-facing addresses of all controller nodes, in id order; this node serves the ShardCtrl API and gRPC health on client-addrs[id], and leader hints point here")
	dataDir := flag.String("data", "", "data directory (default data/ctrl<id>)")
	heartbeat := flag.Duration("heartbeat", 100*time.Millisecond, "leader heartbeat interval")
	electMin := flag.Duration("election-min", 500*time.Millisecond, "minimum election timeout")
	electMax := flag.Duration("election-max", 1000*time.Millisecond, "maximum election timeout")
	verbose := flag.Bool("v", false, "log Raft state transitions")
	flag.Parse()

	addrs := node.SplitAddrs(*peersFlag)
	clientAddrs := node.SplitAddrs(*clientsFlag)
	if err := node.CheckLayout(*id, addrs, clientAddrs); err != nil {
		log.Fatal(err)
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
		Addrs: clientAddrs, // leader hints are for clients
		Raft:  rcfg,
	})

	peerLis, clientLis, err := node.Listeners(addrs[*id], clientAddrs[*id])
	if err != nil {
		log.Fatal(err)
	}
	peerGS := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
	grpctransport.Register(peerGS, srv.Raft())
	clientGS := grpc.NewServer()
	shardctrlv1.RegisterShardCtrlServer(clientGS, srv)
	// Per-node health: as leader, prove a Query commits through this node;
	// see node.Health for the rules.
	healthpb.RegisterHealthServer(clientGS, &node.Health{Raft: srv.Raft(), Me: *id, Probe: func(ctx context.Context) error {
		_, err := srv.Query(ctx, &shardctrlv1.QueryRequest{Num: -1})
		return err
	}})

	// SIGTERM too: it is what `docker stop` (and systemd, k8s) sends, and
	// without it the graceful path below never runs in a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Print("shutting down")
		srv.Kill()
		clientGS.Stop()
		peerGS.Stop()
	}()

	log.Printf("shardctrl node %d: peers on %s, clients on %s (data %s, persisted %d bytes)",
		*id, peerLis.Addr(), clientLis.Addr(), *dataDir, persister.StateSize())
	if err := node.Serve(peerGS, peerLis, clientGS, clientLis); err != nil {
		log.Fatal(err)
	}
}
