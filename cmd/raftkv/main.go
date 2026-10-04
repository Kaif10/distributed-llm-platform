// raftkv runs one replica of the Raft-replicated KV store.
//
// A three-node cluster on one machine:
//
//	raftkv -id 0 -peers 127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003 -client-addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
//	raftkv -id 1 -peers 127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003 -client-addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
//	raftkv -id 2 -peers 127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003 -client-addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
//
// (those are the defaults, so "raftkv -id N" alone does the same).
//
// Each process listens twice: Raft RPCs (peer traffic) on -peers[id], the KV
// API and gRPC health (client traffic) on -client-addrs[id]. The Raft
// service is not registered on the client port at all, so a client cannot
// send it AppendEntries/RequestVote; keep the peer port reachable by the
// other replicas only. Clients (kvctl -addr, sched -kv, gateway -kv) use the
// -client-addrs list; a follower answers "not leader" with a hint drawn
// from that list and kvctl follows it.
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

	"dsys/cmd/internal/node"
	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/raftkv"
	"dsys/raft"
	"dsys/raft/grpctransport"
)

func main() {
	id := flag.Int("id", 0, "this node's index into -peers and -client-addrs")
	peersFlag := flag.String("peers", "127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003",
		"PEER-ONLY Raft addresses of all nodes, in id order; this node serves Raft on peers[id]. Unauthenticated: reachable by the other replicas only")
	clientsFlag := flag.String("client-addrs", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003",
		"client-facing KV addresses of all nodes, in id order; this node serves the KV API and gRPC health on client-addrs[id], and leader hints point here")
	dataDir := flag.String("data", "", "data directory (default data/r<id>)")
	maxRaftState := flag.Int("maxraftstate", 1<<20, "snapshot when Raft state exceeds this many bytes (0 = never)")
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
		Addrs:        clientAddrs, // leader hints are for clients
		Raft:         rcfg,
	})

	peerLis, clientLis, err := node.Listeners(addrs[*id], clientAddrs[*id])
	if err != nil {
		log.Fatal(err)
	}
	peerGS := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
	grpctransport.Register(peerGS, srv.Raft())
	clientGS := grpc.NewServer(grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize))
	kvv1.RegisterKVServer(clientGS, srv)

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

	log.Printf("raftkv node %d: peers on %s, clients on %s (data %s, persisted %d bytes)",
		*id, peerLis.Addr(), clientLis.Addr(), *dataDir, persister.StateSize())
	if err := node.Serve(peerGS, peerLis, clientGS, clientLis); err != nil {
		log.Fatal(err)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }
