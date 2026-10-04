// Package node holds the listener plumbing shared by the Raft-backed server
// binaries (cmd/raftkv, cmd/shardkv, cmd/shardctrl).
//
// Every one of them serves two kinds of traffic that must not share a port:
//
//   - PEER traffic: the Raft RPCs (and, for shardkv, the shard-migration
//     pull). These have no authentication; anyone who can reach them can
//     send AppendEntries with a higher term and crafted entries, or depose
//     the leader with a RequestVote. They must only be reachable by the
//     other replicas.
//   - CLIENT traffic: the KV / ShardCtrl API that kvctl, sched, gateway and
//     the Python client speak, plus the gRPC health service.
//
// So each binary takes two address lists in id order, -peers and
// -client-addrs, and serves each kind on its own listener with its own
// grpc.Server; a service registered on one is Unimplemented on the other.
// Firewalling the peer port off from clients is then an ordinary network
// rule, which it could never be while both shared one port.
package node

import (
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc"
)

// SplitAddrs parses a comma-separated address list, trimming blanks.
func SplitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// CheckLayout validates a peers/clients pair: same length, id in range, and
// no address in both lists (that would put a client service and the Raft
// service back on one port, or simply fail to bind).
func CheckLayout(id int, peers, clients []string) error {
	if len(peers) == 0 {
		return fmt.Errorf("-peers is empty")
	}
	if len(clients) != len(peers) {
		return fmt.Errorf("-client-addrs has %d addresses but -peers has %d; both list every node, in id order", len(clients), len(peers))
	}
	if id < 0 || id >= len(peers) {
		return fmt.Errorf("-id %d out of range for %d nodes", id, len(peers))
	}
	seen := make(map[string]bool, len(peers))
	for _, p := range peers {
		seen[p] = true
	}
	for _, c := range clients {
		if seen[c] {
			return fmt.Errorf("%s is in both -peers and -client-addrs; peer and client traffic need separate ports", c)
		}
	}
	return nil
}

// Listeners binds both addresses up front, so a bad or taken address fails
// at startup rather than after one side is already serving.
func Listeners(peerAddr, clientAddr string) (peer, client net.Listener, err error) {
	peer, err = net.Listen("tcp", peerAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen peer %s: %w", peerAddr, err)
	}
	client, err = net.Listen("tcp", clientAddr)
	if err != nil {
		peer.Close()
		return nil, nil, fmt.Errorf("listen client %s: %w", clientAddr, err)
	}
	return peer, client, nil
}

// Serve runs both servers and returns when either stops: the first error,
// or nil after a clean Stop/GracefulStop. Callers stop BOTH servers on
// shutdown; a server that stops on its own takes the process down with it,
// since a replica serving only one side is not useful.
func Serve(peerSrv *grpc.Server, peerLis net.Listener, clientSrv *grpc.Server, clientLis net.Listener) error {
	errc := make(chan error, 2)
	go func() { errc <- wrap("peer", peerSrv.Serve(peerLis)) }()
	go func() { errc <- wrap("client", clientSrv.Serve(clientLis)) }()
	return <-errc
}

func wrap(side string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("serve %s: %w", side, err)
}
