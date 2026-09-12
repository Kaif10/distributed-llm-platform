package shardkv

import (
	"bytes"
	"encoding/gob"
)

// commandKind tags what a command carries. Only the field(s) documented for
// that kind are populated.
type commandKind uint8

const (
	opKV commandKind = iota
	opConfig
	opMigration
)

// command is the payload of every raft.Entry in a shardkv group's log. It is
// encoded with gob, not protobuf: unlike raftkv's kvv1.LogEntry, this value
// never crosses a process boundary by itself (Raft's Command field is
// already an opaque []byte as far as any RPC is concerned), so there is no
// need for a schema-evolvable wire format, just something this one binary
// can decode back. The one exception is KVBytes: it stays as an already
// proto-marshaled kvv1.LogEntry rather than an embedded struct, because
// generated protobuf message types carry internal bookkeeping (a mutex-like
// MessageState) that reflection-based codecs like gob are not meant to walk;
// marshaling it ourselves keeps command a plain, fully gob-safe data type.
type command struct {
	Kind commandKind

	// Kind == opKV: a proto-marshaled kvv1.LogEntry.
	KVBytes []byte
	// Shard the key in KVBytes belongs to, computed by the proposer so apply
	// does not need to re-hash; apply still re-checks ownership (see
	// server.go) because the config may have changed since Start.
	Shard int

	// Kind == opConfig: the configuration to move to.
	Config Config

	// Kind == opMigration: shard state pulled from its previous owner.
	MigShard           int
	MigConfigNum       int64 // the config number this shard's state is valid AS OF
	MigMachineSnapshot []byte
}

func encodeCommand(c *command) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		// c is built entirely from in-memory values we control; a failure
		// here means a programming error, not bad input.
		panic("shardkv: encode command: " + err.Error())
	}
	return buf.Bytes()
}

func decodeCommand(b []byte) (*command, error) {
	var c command
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}
