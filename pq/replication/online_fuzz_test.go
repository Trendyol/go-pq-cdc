package replication

import (
	"encoding/binary"
	"testing"
)

func FuzzOnlineCommittedEnvelope(f *testing.F) {
	frame := func(messages ...[]byte) []byte {
		var stream []byte
		for _, message := range messages {
			stream = binary.BigEndian.AppendUint32(stream, uint32(len(message)))
			stream = append(stream, message...)
		}
		return stream
	}
	f.Add(frame(onlineBegin(99, 100), onlineRelation(), onlineInsert(), onlineCommit(100, 110)), uint8(200))
	f.Add(frame(onlineBegin(99, 100), onlineCommit(100, 110)), uint8(0))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}, uint8(10))
	f.Fuzz(func(t *testing.T, stream []byte, budget uint8) {
		if len(stream) > 65536 {
			return
		}
		limit := int64(47) + int64(budget)*16
		assembler := newOnlineAssembler(limit, 4096)
		for len(stream) > 0 {
			if len(stream) < 4 {
				return
			}
			n := uint64(binary.BigEndian.Uint32(stream))
			stream = stream[4:]
			if n > uint64(len(stream)) {
				return
			}
			message := stream[:int(n)]
			stream = stream[int(n):]
			env, err := assembler.accept(XLogData{WALData: message, WALStart: 50})
			if assembler.metadataBytes > 4096 || assembler.tx != nil && assembler.tx.EncodedBytes > limit {
				t.Fatal("budget overrun even on rejected input")
			}
			if err != nil {
				return
			}
			if env != nil {
				if env.EndLSN <= env.CommitLSN || env.XID == 0 || env.EncodedBytes > limit {
					t.Fatal("invalid committed boundary escaped")
				}
				for ordinal, change := range env.Changes {
					if change.Ordinal != uint32(ordinal) || change.EncodedBytes <= 0 {
						t.Fatal("unstable row ordinal")
					}
				}
			}
		}
	})
}
