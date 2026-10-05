package snapshot

import (
	"testing"
	"time"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseClaimedChunkWithPhysicalPartition(t *testing.T) {
	now := time.Now().UTC()
	row := [][]byte{
		[]byte("42"),
		[]byte("public"),
		[]byte("events"),
		[]byte("archive"),
		[]byte("events_2026_01"),
		[]byte("3"),
		[]byte("12"),
		[]byte("100"),
		nil,
		nil,
		[]byte("12"),
		nil,
		[]byte("true"),
		[]byte("ctid_block"),
		[]byte("0"),
	}

	chunk, err := (&Snapshotter{}).parseClaimedChunk(row, "snapshot", "worker", now)
	require.NoError(t, err)
	assert.Equal(t, int64(42), chunk.ID)
	assert.Equal(t, "public", chunk.TableSchema)
	assert.Equal(t, "events", chunk.TableName)
	assert.Equal(t, "archive", chunk.PhysicalTableSchema)
	assert.Equal(t, "events_2026_01", chunk.PhysicalTableName)
	assert.Equal(t, 3, chunk.ChunkIndex)
	assert.Equal(t, int64(12), chunk.ChunkStart)
	assert.Equal(t, int64(100), chunk.ChunkSize)
	assert.Equal(t, int64(12), *chunk.BlockStart)
	assert.Nil(t, chunk.BlockEnd)
	assert.True(t, chunk.IsLastChunk)
	assert.Equal(t, PartitionStrategyCTIDBlock, chunk.PartitionStrategy)
}

// The dropped-partition path relies on the SQLSTATE surviving the Wrap chain between
// the chunk query and executeChunkProcessing.
func TestIsUndefinedTableErrorSeesThroughWraps(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "42P01"}
	wrapped := errors.Wrap(errors.Wrap(errors.Wrap(pgErr, "execute chunk query"), "execute function"), "process chunk with transaction")

	assert.True(t, isUndefinedTableError(wrapped))
	assert.False(t, isUndefinedTableError(errors.Wrap(&pgconn.PgError{Code: "42703"}, "execute chunk query")))
}
