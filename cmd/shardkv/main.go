// shardkv runs one replica of one shard-owning Raft group.
//
// It mirrors cmd/raftkv closely, with two additions: this replica belongs
// to a group identified by -gid (several such groups, each with their own
// -peers/-client-addrs, together make up the sharded cluster), and it talks
// to a shard controller cluster (-ctrl, the controller's CLIENT addresses)
// to learn and follow the current shard assignment.
//
// A two-group, three-replica-each cluster on one machine, with a shard
// controller already running with its default client addresses
// 127.0.0.1:8001-8003:
//
//	G1P=127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003   G1C=127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103
//	G2P=127.0.0.1:19201,127.0.0.1:19202,127.0.0.1:19203   G2C=127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203
//	G2MAP=127.0.0.1:9201=127.0.0.1:19201,127.0.0.1:9202=127.0.0.1:19202,127.0.0.1:9203=127.0.0.1:19203
//	shardkv -gid 100 -id 0 -peers $G1P -client-addrs $G1C -peer-map $G2MAP -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
//	... (id 1, 2 for gid 100; id 0, 1, 2 for gid 200 with $G2P/$G2C and group 100's map)
//	shardctl -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 join 100=$G1C
//
// Each process listens twice. On -peers[id] (PEER traffic, unauthenticated,
// keep it reachable by other replicas only): this group's own Raft RPCs,
// and the ShardMigration service that lets OTHER groups pull a shard's data
// out of this one. On -client-addrs[id]: the client-facing KV API and gRPC
// health. Register the group with its CLIENT addresses (shardctl join
// gid=<client-addrs>): clients (kvctl -ctrl ...) discover which group owns
// a key's shard from the controller and talk to those addresses directly; a
// follower answers "not leader" with a hint from the same list.
//
// Because the controller config only holds client addresses, a replica
// pulling a shard from another group translates them to that group's peer
// addresses through -peer-map. This group's own pairs are added
// automatically; list every other group's replicas there.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dsys/cmd/internal/node"
	kvv1 "dsys/gen/kv/v1"
	"dsys/raft"
	raftgrpctransport "dsys/raft/grpctransport"
	"dsys/shard"
	"dsys/shardkv"
	shardkvgrpctransport "dsys/shardkv/grpctransport"
)

func main() {
	gid := flag.Int64("gid", 0, "this replica's group id (required)")
	id := flag.Int("id", 0, "this replica's index within its OWN group's -peers / -client-addrs lists")
	peersFlag := flag.String("peers", "",
		"PEER-ONLY addresses of this replica's OWN group, in id order (required); this replica serves Raft and shard migration on peers[id]. Unauthenticated: make it reachable by other replicas only")
	clientsFlag := flag.String("client-addrs", "",
		"client-facing addresses of this replica's OWN group, in id order (required); this replica serves the KV API and gRPC health on client-addrs[id]. Register these with shardctl join")
	peerMapFlag := flag.String("peer-map", "",
		"comma-separated client=peer address pairs for the replicas of every OTHER group, used to pull migrating shards from their peer ports (this group's own pairs are added automatically)")
	ctrlFlag := flag.String("ctrl", "", "comma-separated CLIENT addresses of the shard controller cluster (required)")
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
	if *peersFlag == "" || *clientsFlag == "" {
		log.Fatal("-peers and -client-addrs are both required")
	}
	if *ctrlFlag == "" {
		log.Fatal("-ctrl is required")
	}

	addrs := node.SplitAddrs(*peersFlag)
	clientAddrs := node.SplitAddrs(*clientsFlag)
	if err := node.CheckLayout(*id, addrs, clientAddrs); err != nil {
		log.Fatal(err)
	}
	ctrlAddrs := node.SplitAddrs(*ctrlFlag)
	peerMap, err := parsePeerMap(*peerMapFlag)
	if err != nil {
		log.Fatalf("-peer-map: %v", err)
	}
	for i := range addrs {
		peerMap[clientAddrs[i]] = addrs[i]
	}

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
	fetcher := shardkvgrpctransport.NewFetcher(
		shardkvgrpctransport.WithTimeout(*pollInterval),
		shardkvgrpctransport.WithPeerAddrs(peerMap),
	)
	defer fetcher.Close()

	srv := shardkv.New(peers, *id, persister, shardkv.Options{
		GID:          *gid,
		Addrs:        clientAddrs, // leader hints are for clients
		Ctrl:         ctrl,
		Fetcher:      fetcher,
		PollInterval: *pollInterval,
		MaxRaftState: *maxRaftState,
		Raft:         rcfg,
	})

	peerLis, clientLis, err := node.Listeners(addrs[*id], clientAddrs[*id])
	if err != nil {
		log.Fatal(err)
	}
	peerGS := grpc.NewServer(grpc.MaxRecvMsgSize(raftgrpctransport.MaxMessageSize))
	raftgrpctransport.Register(peerGS, srv.Raft()) // this group's own Raft
	shardkvgrpctransport.Register(peerGS, srv)     // other groups pulling shards
	clientGS := grpc.NewServer(grpc.MaxRecvMsgSize(raftgrpctransport.MaxMessageSize))
	kvv1.RegisterKVServer(clientGS, srv)
	healthpb.RegisterHealthServer(clientGS, &node.Health{Raft: srv.Raft(), Me: *id, Probe: func(ctx context.Context) error {
		return probeOwnedShard(ctx, srv)
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

	log.Printf("shardkv group %d replica %d: peers on %s, clients on %s (data %s, persisted %d bytes, controller %v)",
		*gid, *id, peerLis.Addr(), clientLis.Addr(), *dataDir, persister.StateSize(), ctrlAddrs)
	if err := node.Serve(peerGS, peerLis, clientGS, clientLis); err != nil {
		log.Fatal(err)
	}
}

// probeOwnedShard is the leader's health probe: a read of a key in a shard
// this group owns, which must commit through this node's log. shardkv
// rejects keys of shards it does not own (or has not finished migrating in)
// BEFORE touching the log, so the key has to be chosen per shard. A group
// that owns no shard yet (not joined) has nothing to read through; it is
// then judged on leadership alone.
func probeOwnedShard(ctx context.Context, srv *shardkv.Server) error {
	cfg := srv.CurrentConfig()
	var lastErr error
	for s, owner := range cfg.Shards {
		if owner != srv.GID() {
			continue
		}
		_, err := srv.Get(ctx, &kvv1.GetRequest{Key: probeKey(s)})
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// probeKey returns a fixed key that hashes to shard s.
func probeKey(s int) string {
	for i := 0; ; i++ {
		if k := "__health_probe_" + strconv.Itoa(i); shard.Key2Shard(k) == s {
			return k
		}
	}
}

// parsePeerMap parses "client=peer,client=peer,..." into a map.
func parsePeerMap(s string) (map[string]string, error) {
	m := make(map[string]string)
	for _, pair := range node.SplitAddrs(s) {
		client, peer, ok := strings.Cut(pair, "=")
		client, peer = strings.TrimSpace(client), strings.TrimSpace(peer)
		if !ok || client == "" || peer == "" {
			return nil, fmt.Errorf("%q is not client=peer", pair)
		}
		if client == peer {
			return nil, fmt.Errorf("%q maps an address to itself; peer and client ports must differ", pair)
		}
		m[client] = peer
	}
	return m, nil
}
