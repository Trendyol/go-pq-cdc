package replication

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/require"
)

func TestOnlineDecoderPreservesRowWALStartAndServerClockNotCommitClock(t *testing.T) {
	assembler := newOnlineAssembler(4096, 4096)
	clock := time.Date(2026, 11, 13, 12, 13, 14, 123000, time.UTC)
	for i, wire := range [][]byte{onlineBegin(99, 100), onlineRelation(), onlineInsert(), onlineUpdate(), onlineDelete()} {
		env, err := assembler.accept(XLogData{WALData: wire, WALStart: pq.LSN(50 + i), ServerTime: clock})
		require.NoError(t, err)
		require.Nil(t, env)
	}
	env, err := assembler.accept(XLogData{WALData: onlineCommit(100, 110), WALStart: 100, ServerTime: clock.Add(time.Hour)})
	require.NoError(t, err)
	require.Equal(t, uint32(99), env.XID)
	require.Equal(t, pq.LSN(100), env.CommitLSN)
	require.Equal(t, pq.LSN(110), env.EndLSN)
	require.Len(t, env.Changes, 3)
	insert := env.Changes[0].Message.(*format.Insert)
	update := env.Changes[1].Message.(*format.Update)
	deleted := env.Changes[2].Message.(*format.Delete)
	require.Equal(t, pq.LSN(52), insert.LSN)
	require.Equal(t, pq.LSN(53), update.LSN)
	require.Equal(t, pq.LSN(54), deleted.LSN)
	require.Equal(t, clock, insert.MessageTime)
	require.Equal(t, clock, update.MessageTime)
	require.Equal(t, clock, deleted.MessageTime)
	require.Equal(t, int32(42), insert.Decoded["id"])
	require.Equal(t, int32(42), update.OldDecoded["id"])
	require.Equal(t, int32(43), update.NewDecoded["id"])
	require.Equal(t, int32(43), deleted.OldDecoded["id"])
	require.Positive(t, env.EncodedBytes, "old/new tuples are included in original transaction budget")
}
