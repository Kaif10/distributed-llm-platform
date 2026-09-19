package chaos

// The Porcupine linearizability model for the raw KV workload, the same
// shape used in kv/raftkv and shardkv's own tests (get/put/cas over a
// single-key sequential map). Duplicated rather than imported because it
// lived in an unexported _test.go file in both places; three copies of 40
// lines is a fine price for not making test-only code part of a package's
// public surface.

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

type kvInput struct {
	op       string // "put" | "get" | "cas"
	key      string
	value    string
	expected string
}

type kvOutput struct {
	value   string
	found   bool
	swapped bool
	current string
	failed  bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return "\x00absent" },
	Step: func(state, input, output any) (bool, any) {
		st := state.(string)
		in := input.(kvInput)
		out := output.(kvOutput)
		absent := st == "\x00absent"
		switch in.op {
		case "get":
			if out.failed {
				return true, st
			}
			if absent {
				return !out.found, st
			}
			return out.found && out.value == st, st
		case "put":
			return true, in.value
		case "cas":
			swapped := !absent && st == in.expected
			if out.failed {
				if swapped {
					return true, in.value
				}
				return true, st
			}
			if out.swapped != swapped {
				return false, st
			}
			if !absent && out.current != st {
				return false, st
			}
			if swapped {
				return true, in.value
			}
			return true, st
		}
		return false, st
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.op {
		case "get":
			return fmt.Sprintf("get(%s) -> %q,%v", in.key, out.value, out.found)
		case "put":
			return fmt.Sprintf("put(%s,%s)", in.key, in.value)
		default:
			return fmt.Sprintf("cas(%s,%s->%s) -> %v", in.key, in.expected, in.value, out.swapped)
		}
	},
}
