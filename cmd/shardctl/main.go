// shardctl is the admin CLI for a shard controller cluster: it issues the
// Join/Leave/Move/Query operations that shape which replica group owns
// which shard. It has nothing to do with serving client traffic (that is
// kvctl's job); this is the tool an operator (or a deploy script) runs to
// register a new shardkv group, retire one, or manually rebalance.
//
//	shardctl -ctrl 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 \
//	    join 100=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
//	shardctl -ctrl ... move 3 200
//	shardctl -ctrl ... leave 100
//	shardctl -ctrl ... query
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"dsys/shardctrl"
)

func main() {
	ctrlFlag := flag.String("ctrl", "", "comma-separated shard controller addresses (required)")
	timeout := flag.Duration("timeout", 10*time.Second, "give up after this long")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `shardctl -ctrl <addrs> <command> [args]

Commands:
  join <gid>=<addr>,<addr>,... [<gid>=<addr>,...]   register one or more groups
  leave <gid> [<gid>...]                            retire one or more groups
  move <shard> <gid>                                force one shard to a group
  query [num]                                        print a configuration (default: latest)
`)
		flag.PrintDefaults()
	}
	flag.Parse()
	if *ctrlFlag == "" || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	c := shardctrl.NewClient(strings.Split(*ctrlFlag, ","))
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var err error
	switch cmd, args := flag.Arg(0), flag.Args()[1:]; cmd {
	case "join":
		groups := map[int64][]string{}
		for _, spec := range args {
			gid, addrs, perr := parseGroup(spec)
			if perr != nil {
				fatalf("join: %v", perr)
			}
			groups[gid] = addrs
		}
		if len(groups) == 0 {
			fatalf("join: at least one <gid>=<addr>,... argument required")
		}
		err = c.Join(ctx, groups)
	case "leave":
		var gids []int64
		for _, a := range args {
			gid, perr := strconv.ParseInt(a, 10, 64)
			if perr != nil {
				fatalf("leave: bad group id %q: %v", a, perr)
			}
			gids = append(gids, gid)
		}
		if len(gids) == 0 {
			fatalf("leave: at least one group id required")
		}
		err = c.Leave(ctx, gids)
	case "move":
		if len(args) != 2 {
			fatalf("move: usage: move <shard> <gid>")
		}
		sh, perr := strconv.ParseInt(args[0], 10, 64)
		if perr != nil {
			fatalf("move: bad shard %q: %v", args[0], perr)
		}
		gid, perr := strconv.ParseInt(args[1], 10, 64)
		if perr != nil {
			fatalf("move: bad group id %q: %v", args[1], perr)
		}
		err = c.Move(ctx, sh, gid)
	case "query":
		num := int64(-1)
		if len(args) == 1 {
			n, perr := strconv.ParseInt(args[0], 10, 64)
			if perr != nil {
				fatalf("query: bad config number %q: %v", args[0], perr)
			}
			num = n
		}
		var cfg any
		cfg, err = c.Query(ctx, num)
		if err == nil {
			printConfig(cfg)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatalf("%v", err)
	}
}

func parseGroup(spec string) (int64, []string, error) {
	eq := strings.IndexByte(spec, '=')
	if eq < 0 {
		return 0, nil, fmt.Errorf("expected <gid>=<addr>,<addr>,..., got %q", spec)
	}
	gid, err := strconv.ParseInt(spec[:eq], 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("bad group id in %q: %w", spec, err)
	}
	addrs := strings.Split(spec[eq+1:], ",")
	if len(addrs) == 0 || addrs[0] == "" {
		return 0, nil, fmt.Errorf("no addresses in %q", spec)
	}
	return gid, addrs, nil
}

func printConfig(v any) {
	// Printed via fmt.Sprintf("%+v") on the proto-generated Config: it has
	// public fields (Num, Shards, Groups) and this is an admin tool, not a
	// stable API, so a debug-formatted dump is the right amount of effort.
	fmt.Printf("%+v\n", v)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "shardctl: "+format+"\n", args...)
	os.Exit(1)
}
