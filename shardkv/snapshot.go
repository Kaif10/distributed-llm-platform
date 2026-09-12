package shardkv

import (
	"bytes"
	"encoding/gob"
)

// encodeSnapshot/decodeSnapshot serialize serverSnapshot with gob. Like
// command, this never leaves the process by itself (it becomes Raft's
// opaque snapshot []byte), so gob is the simplest fit: everything in
// serverSnapshot is plain data (a fixed array, maps of ints and byte
// slices), nothing protobuf-shaped, so there is no MessageState concern
// here the way there was for command.KVBytes.
func encodeSnapshot(ss *serverSnapshot) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(ss); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeSnapshot(b []byte) (*serverSnapshot, error) {
	var ss serverSnapshot
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&ss); err != nil {
		return nil, err
	}
	return &ss, nil
}
