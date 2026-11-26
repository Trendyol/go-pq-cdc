package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBookRelation(oid uint32, cols ...tuple.RelationColumn) *format.Relation {
	if len(cols) == 0 {
		cols = []tuple.RelationColumn{
			{Flags: 1, Name: "id", DataType: pgtype.Int4OID},
			{Name: "name", DataType: pgtype.TextOID},
		}
	}
	return &format.Relation{
		OID:           oid,
		Namespace:     "public",
		Name:          "books",
		ReplicaID:     'f',
		ColumnNumbers: uint16(len(cols)),
		Columns:       cols,
	}
}

func gatherMetricValue(t *testing.T, reg metric.Registry, name string) (float64, bool) {
	t.Helper()
	families, err := reg.Prometheus().Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name || len(mf.GetMetric()) == 0 {
			continue
		}
		m := mf.GetMetric()[0]
		switch mf.GetType() {
		case dto.MetricType_GAUGE:
			return m.GetGauge().GetValue(), true
		case dto.MetricType_COUNTER:
			return m.GetCounter().GetValue(), true
		}
	}
	return 0, false
}

func TestStreamTxBufferLazyDecodingSkipsTupleDecodeOnAbort(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{}, "lazy_slot", nil)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}
	require.NoError(t, sb.startTx(10))

	// Valid Insert header (type 'I' + xid + oid), followed by intentionally corrupt tuple bytes.
	corrupt := make([]byte, 14)
	corrupt[0] = byte(message.InsertByte)
	binary.BigEndian.PutUint32(corrupt[1:5], 10)
	binary.BigEndian.PutUint32(corrupt[5:9], 1001)
	corrupt[9] = 'X' // invalid tuple data type; eager decoder would fail here.

	require.NoError(t, sb.appendRaw(corrupt, 100, time.Unix(1_700_000_000, 0).UTC(), rels))
	require.NoError(t, sb.stopTx())
	require.NoError(t, sb.abortTx(10, 10))
	assert.False(t, sb.hasOpenTxns())
}

func TestStreamTxBufferSpillThresholdAndCommitFidelity(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	spillDir := t.TempDir()
	m := metric.NewMetric("spill_commit")
	reg := metric.NewRegistry(m)

	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}
	// One row's relBind (9) + wireFrame (21 + ~28) is ~58 bytes; threshold 120 forces spill on row 3.
	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 120,
		SpillDir:       spillDir,
	}, "spill_commit", m)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	srvTime := time.Unix(1_700_000_123, 456_000).UTC()
	require.NoError(t, sb.startTx(42))
	for i := 0; i < 10; i++ {
		raw := encodeStreamedInsert(42, 1001, wireTextCol("1"), wireTextCol("book"))
		require.NoError(t, sb.appendRaw(raw, pq.LSN(1000+i*10), srvTime, rels))
	}
	require.NoError(t, sb.stopTx())

	tx := sb.txns[42]
	require.NotNil(t, tx)
	assert.True(t, tx.spilled)
	assert.Nil(t, tx.file, "STREAM STOP must close the open file descriptor")
	assert.Nil(t, tx.bw)
	assert.Zero(t, sb.totalMemBytes)
	assert.Positive(t, sb.totalDiskBytes)

	var delivered []*Message
	require.NoError(t, sb.flushTx(42, func(msg *Message) bool {
		delivered = append(delivered, msg)
		return true
	}, pq.LSN(5000), pq.LSN(5500)))

	require.Len(t, delivered, 10)
	for i, msg := range delivered {
		ins, ok := msg.message.(*format.Insert)
		require.True(t, ok)
		assert.Equal(t, uint32(42), msg.xid)
		assert.Equal(t, pq.LSN(5000), msg.commitLSN)
		assert.Equal(t, pq.LSN(1000+i*10), ins.LSN)
		assert.Equal(t, srvTime, ins.MessageTime)
		assert.Equal(t, int32(1), ins.Decoded["id"])
		assert.Equal(t, "book", ins.Decoded["name"])
		if i < len(delivered)-1 {
			assert.Equal(t, int64(1000+i*10), msg.walStart)
		} else {
			assert.Equal(t, int64(5500), msg.walStart)
		}
	}

	assert.False(t, sb.hasOpenTxns())
	assert.Zero(t, sb.totalMemBytes)
	assert.Zero(t, sb.totalDiskBytes)
	entries, err := os.ReadDir(sb.sessionDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "spill file must be removed after STREAM COMMIT")

	v, ok := gatherMetricValue(t, reg, "go_pq_cdc_streaming_spill_transactions_total")
	require.True(t, ok)
	assert.InDelta(t, 1, v, 0)
}

func TestStreamTxBufferLargestTxEvictionOrder(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 250,
		SpillDir:       t.TempDir(),
	}, "evict_order", nil)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	// Small TX 10: 1 row (~60 bytes)
	require.NoError(t, sb.startTx(10))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(10, 1001, wireTextCol("1"), wireTextCol("s")), 10, time.Time{}, rels))
	require.NoError(t, sb.stopTx())

	// Large TX 20: 3 rows (~150 bytes), still under 250 total
	require.NoError(t, sb.startTx(20))
	for i := 0; i < 3; i++ {
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(20, 1001, wireTextCol("2"), wireTextCol("large")), pq.LSN(20+i), time.Time{}, rels))
	}
	require.NoError(t, sb.stopTx())
	assert.False(t, sb.txns[10].spilled)
	assert.False(t, sb.txns[20].spilled)

	// Medium TX 30 pushes totalMemBytes over 250 -> largest (TX 20) must spill first, TX 10 stays in memory.
	require.NoError(t, sb.startTx(30))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(30, 1001, wireTextCol("3"), wireTextCol("med")), 30, time.Time{}, rels))
	require.NoError(t, sb.stopTx())

	assert.False(t, sb.txns[10].spilled, "small TX must remain in memory")
	assert.True(t, sb.txns[20].spilled, "largest TX must be evicted first")
	assert.False(t, sb.txns[30].spilled, "once below budget, remaining TXs stay in memory")

	// Tie-break test: equal size in-memory TXs -> lowest XID evicted first.
	sbTie, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 90,
		SpillDir:       t.TempDir(),
	}, "tie_break", nil)
	require.NoError(t, err)
	defer func() { _ = sbTie.close() }()

	require.NoError(t, sbTie.startTx(200))
	require.NoError(t, sbTie.appendRaw(encodeStreamedInsert(200, 1001, wireTextCol("1"), wireTextCol("aa")), 10, time.Time{}, rels))
	require.NoError(t, sbTie.stopTx())

	require.NoError(t, sbTie.startTx(100))
	require.NoError(t, sbTie.appendRaw(encodeStreamedInsert(100, 1001, wireTextCol("1"), wireTextCol("bb")), 20, time.Time{}, rels))
	require.NoError(t, sbTie.stopTx())

	assert.True(t, sbTie.txns[100].spilled, "tie-break must spill lowest XID first")
	assert.False(t, sbTie.txns[200].spilled)
}

func TestStreamTxBufferMultiChunkSingleFDInvariant(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1,
		SpillDir:       t.TempDir(),
	}, "multi_chunk", nil)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	// Chunk 1: spills immediately.
	require.NoError(t, sb.startTx(100))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(100, 1001, wireTextCol("1"), wireTextCol("c1")), 10, time.Time{}, rels))
	require.NotNil(t, sb.txns[100].file)
	require.NoError(t, sb.stopTx())
	assert.Nil(t, sb.txns[100].file, "FD must close on STREAM STOP")

	// Chunk 2 for TX 200 while TX 100 is stopped -> only TX 200 opens an FD.
	require.NoError(t, sb.startTx(200))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(200, 1001, wireTextCol("2"), wireTextCol("other")), 20, time.Time{}, rels))
	assert.Nil(t, sb.txns[100].file)
	require.NotNil(t, sb.txns[200].file)
	require.NoError(t, sb.stopTx())
	assert.Nil(t, sb.txns[200].file)

	// Chunk 3 resumes TX 100 -> lazily reopens spill file and appends.
	require.NoError(t, sb.startTx(100))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(100, 1001, wireTextCol("3"), wireTextCol("c2")), 30, time.Time{}, rels))
	require.NotNil(t, sb.txns[100].file)
	require.NoError(t, sb.stopTx())
	assert.Nil(t, sb.txns[100].file)

	var names []string
	require.NoError(t, sb.flushTx(100, func(m *Message) bool {
		names = append(names, m.message.(*format.Insert).Decoded["name"].(string))
		return true
	}, 40, 50))
	assert.Equal(t, []string{"c1", "c2"}, names)
}

func TestStreamTxBufferSubTransactionTransitions(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	t.Run("subxact recorded in memory then spilled then aborted on disk", func(t *testing.T) {
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
			Enabled:        true,
			MaxMemoryBytes: 150,
			SpillDir:       t.TempDir(),
		}, "sub_transition", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()

		require.NoError(t, sb.startTx(50))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(50, 1001, wireTextCol("1"), wireTextCol("keep")), 10, time.Time{}, rels))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(51, 1001, wireTextCol("2"), wireTextCol("drop1")), 20, time.Time{}, rels))
		assert.False(t, sb.txns[50].spilled, "must still be in memory when sub-TX 51 starts")

		// Push over 150 bytes inside sub-TX 51 so TX spills mid-sub-transaction.
		for i := 0; i < 4; i++ {
			require.NoError(t, sb.appendRaw(encodeStreamedInsert(51, 1001, wireTextCol("3"), wireTextCol("drop-spill")), pq.LSN(30+i), time.Time{}, rels))
		}
		require.True(t, sb.txns[50].spilled)
		require.NoError(t, sb.stopTx())

		// Sub-TX abort truncates on disk back to pre-spill offset.
		require.NoError(t, sb.abortTx(50, 51))
		assert.Nil(t, sb.txns[50].file)

		// Subsequent writes after disk truncation re-bind cleanly and commit.
		require.NoError(t, sb.startTx(50))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(50, 1001, wireTextCol("4"), wireTextCol("after")), 40, time.Time{}, rels))
		require.NoError(t, sb.stopTx())

		var got []string
		require.NoError(t, sb.flushTx(50, func(m *Message) bool {
			got = append(got, m.message.(*format.Insert).Decoded["name"].(string))
			return true
		}, 90, 99))
		assert.Equal(t, []string{"keep", "after"}, got)
	})

	t.Run("subxact abort dropping all remaining frames produces empty commit", func(t *testing.T) {
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
			Enabled:        true,
			MaxMemoryBytes: 1,
			SpillDir:       t.TempDir(),
		}, "sub_empty", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()

		require.NoError(t, sb.startTx(60))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(61, 1001, wireTextCol("1"), wireTextCol("only-sub")), 10, time.Time{}, rels))
		require.NoError(t, sb.stopTx())
		require.NoError(t, sb.abortTx(60, 61))
		assert.Zero(t, sb.totalDiskBytes)

		var count int
		require.NoError(t, sb.flushTx(60, func(_ *Message) bool {
			count++
			return true
		}, 20, 30))
		assert.Zero(t, count)
		assert.False(t, sb.hasOpenTxns())
	})
}

func TestStreamTxBufferWholeTxAbortCleansUpAndResetsMetrics(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	m := metric.NewMetric("abort_metrics")
	reg := metric.NewRegistry(m)
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1,
		SpillDir:       t.TempDir(),
	}, "abort_metrics", m)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	require.NoError(t, sb.startTx(77))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(77, 1001, wireTextCol("1"), wireTextCol("x")), 10, time.Time{}, rels))
	require.NoError(t, sb.stopTx())

	require.NoError(t, sb.abortTx(77, 77))

	assert.False(t, sb.hasOpenTxns())
	entries, err := os.ReadDir(sb.sessionDir)
	require.NoError(t, err)
	assert.Empty(t, entries)

	active, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_active_transactions")
	bufBytes, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_buffer_bytes")
	spilledBytes, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_spilled_bytes")
	aborted, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_aborted_transactions_total")
	assert.Zero(t, active)
	assert.Zero(t, bufBytes)
	assert.Zero(t, spilledBytes)
	assert.InDelta(t, 1, aborted, 0)
}

func TestStreamTxBufferRelationSchemaEvolution(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	colsV1 := []tuple.RelationColumn{
		{Flags: 1, Name: "id", DataType: pgtype.Int4OID},
		{Name: "name", DataType: pgtype.TextOID},
	}
	colsV2 := []tuple.RelationColumn{
		{Flags: 1, Name: "id", DataType: pgtype.Int4OID},
		{Name: "name", DataType: pgtype.TextOID},
		{Name: "author", DataType: pgtype.TextOID},
	}

	t.Run("TX-B ALTER TABLE while TX-A is spilled does not corrupt TX-A replay", func(t *testing.T) {
		s := &stream{mu: &sync.RWMutex{}, relation: make(map[uint32]*format.Relation)}
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{Enabled: true, MaxMemoryBytes: 1, SpillDir: t.TempDir()}, "ddl_interleave", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()
		out := make(chan *Message, 10)
		buf := &messageBuffer{outCh: out}

		// Non-streamed 'R' v1 before TX-A, plus streamed 'R' for a second table OID 1002.
		require.NoError(t, s.handleXLogData(encodeXLogData(1, 1, time.Time{}, encodeRelation(false, 0, 1001, "public", "books", 'f', colsV1)), buf, sb))

		// TX-A starts, inserts v1 row, spills, stops.
		require.NoError(t, s.handleXLogData(encodeXLogData(2, 2, time.Time{}, encodeStreamStart(10, 1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(3, 3, time.Time{}, encodeStreamedInsert(10, 1001, wireTextCol("1"), wireTextCol("v1-book"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(4, 4, time.Time{}, encodeStreamStop()), buf, sb))

		// TX-B alters table -> emits 'R' v2 while TX-A is spilled, plus an insert on OID 1002.
		require.NoError(t, s.handleXLogData(encodeXLogData(5, 5, time.Time{}, encodeStreamStart(20, 1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(6, 6, time.Time{}, encodeRelation(true, 20, 1001, "public", "books", 'f', colsV2)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(7, 7, time.Time{}, encodeRelation(true, 20, 1002, "inventory", "authors", 'd', colsV1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(8, 8, time.Time{}, encodeStreamedInsert(20, 1001, wireTextCol("2"), wireTextCol("v2-book"), wireTextCol("alice"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(9, 9, time.Time{}, encodeStreamedInsert(20, 1002, wireTextCol("3"), wireTextCol("author-row"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(10, 10, time.Time{}, encodeStreamStop()), buf, sb))

		// TX-A commits -> must still decode using v1 schema (2 columns, no index panic).
		require.NoError(t, s.handleXLogData(encodeXLogData(11, 11, time.Time{}, encodeStreamCommit(10, 11, 11)), buf, sb))
		msgA := <-out
		insA := msgA.message.(*format.Insert)
		assert.Equal(t, "v1-book", insA.Decoded["name"])
		assert.Len(t, insA.Decoded, 2)

		// TX-B commits -> delivers 'R' v2, 'R' authors, v2 Insert, and authors Insert.
		require.NoError(t, s.handleXLogData(encodeXLogData(12, 12, time.Time{}, encodeStreamCommit(20, 12, 12)), buf, sb))
		assert.IsType(t, &format.Relation{}, (<-out).message)
		assert.IsType(t, &format.Relation{}, (<-out).message)
		msgB := <-out
		insB := msgB.message.(*format.Insert)
		assert.Equal(t, "alice", insB.Decoded["author"])
		msgAuthor := <-out
		assert.Equal(t, "author-row", msgAuthor.message.(*format.Insert).Decoded["name"])
	})

	t.Run("mid-TX relation upgrade inside same spilled TX", func(t *testing.T) {
		s := &stream{mu: &sync.RWMutex{}, relation: make(map[uint32]*format.Relation)}
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{Enabled: true, MaxMemoryBytes: 1, SpillDir: t.TempDir()}, "ddl_same_tx", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()
		out := make(chan *Message, 10)
		buf := &messageBuffer{outCh: out}

		require.NoError(t, s.handleXLogData(encodeXLogData(1, 1, time.Time{}, encodeStreamStart(10, 1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(2, 2, time.Time{}, encodeRelation(true, 10, 1001, "public", "books", 'f', colsV1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(3, 3, time.Time{}, encodeStreamedInsert(10, 1001, wireTextCol("1"), wireTextCol("old"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(4, 4, time.Time{}, encodeStreamStop()), buf, sb))

		require.NoError(t, s.handleXLogData(encodeXLogData(5, 5, time.Time{}, encodeStreamStart(10, 0)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(6, 6, time.Time{}, encodeRelation(true, 10, 1001, "public", "books", 'f', colsV2)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(7, 7, time.Time{}, encodeStreamedInsert(10, 1001, wireTextCol("2"), wireTextCol("new"), wireTextCol("bob"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(8, 8, time.Time{}, encodeStreamStop()), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(9, 9, time.Time{}, encodeStreamCommit(10, 9, 9)), buf, sb))

		assert.IsType(t, &format.Relation{}, (<-out).message)
		assert.Equal(t, "old", (<-out).message.(*format.Insert).Decoded["name"])
		assert.IsType(t, &format.Relation{}, (<-out).message)
		assert.Equal(t, "bob", (<-out).message.(*format.Insert).Decoded["author"])
	})

	t.Run("relation inside aborted TX-A stays available for TX-B", func(t *testing.T) {
		s := &stream{mu: &sync.RWMutex{}, relation: make(map[uint32]*format.Relation)}
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{Enabled: true, MaxMemoryBytes: 1, SpillDir: t.TempDir()}, "ddl_abort", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()
		out := make(chan *Message, 10)
		buf := &messageBuffer{outCh: out}

		require.NoError(t, s.handleXLogData(encodeXLogData(1, 1, time.Time{}, encodeStreamStart(10, 1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(2, 2, time.Time{}, encodeRelation(true, 10, 1001, "public", "books", 'f', colsV1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(3, 3, time.Time{}, encodeStreamStop()), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(4, 4, time.Time{}, encodeStreamAbort(10, 10)), buf, sb))

		// TX-B inserts without another 'R' (PG's schema_sent=true behavior).
		require.NoError(t, s.handleXLogData(encodeXLogData(5, 5, time.Time{}, encodeStreamStart(20, 1)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(6, 6, time.Time{}, encodeStreamedInsert(20, 1001, wireTextCol("7"), wireTextCol("survived"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(7, 7, time.Time{}, encodeStreamStop()), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(8, 8, time.Time{}, encodeStreamCommit(20, 8, 8)), buf, sb))

		require.Len(t, out, 1)
		assert.Equal(t, "survived", (<-out).message.(*format.Insert).Decoded["name"])
	})

	t.Run("sub-TX rollback after bind frame re-binds cleanly", func(t *testing.T) {
		s := &stream{mu: &sync.RWMutex{}, relation: map[uint32]*format.Relation{1001: testBookRelation(1001, colsV1...)}}
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{Enabled: true, MaxMemoryBytes: 1, SpillDir: t.TempDir()}, "ddl_subabort", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()
		out := make(chan *Message, 10)
		buf := &messageBuffer{outCh: out}

		require.NoError(t, s.handleXLogData(encodeXLogData(1, 1, time.Time{}, encodeStreamStart(10, 1)), buf, sb))
		// First DML in TX belongs to sub-XID 11, so frameKindRelBind is emitted inside sub-XID 11's range!
		require.NoError(t, s.handleXLogData(encodeXLogData(2, 2, time.Time{}, encodeStreamedInsert(11, 1001, wireTextCol("1"), wireTextCol("dropped"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(3, 3, time.Time{}, encodeStreamStop()), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(4, 4, time.Time{}, encodeStreamAbort(10, 11)), buf, sb))

		// Subsequent top-level insert must emit a fresh frameKindRelBind because clear(tx.boundRel) ran.
		require.NoError(t, s.handleXLogData(encodeXLogData(5, 5, time.Time{}, encodeStreamStart(10, 0)), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(6, 6, time.Time{}, encodeStreamedInsert(10, 1001, wireTextCol("2"), wireTextCol("kept"))), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(7, 7, time.Time{}, encodeStreamStop()), buf, sb))
		require.NoError(t, s.handleXLogData(encodeXLogData(8, 8, time.Time{}, encodeStreamCommit(10, 8, 8)), buf, sb))

		require.Len(t, out, 1)
		assert.Equal(t, "kept", (<-out).message.(*format.Insert).Decoded["name"])
	})
}

func TestStreamTxBufferSpilledUpdateToastTruncateAndLogicalMsg(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	cols := []tuple.RelationColumn{
		{Flags: 1, Name: "id", DataType: pgtype.Int4OID},
		{Name: "payload", DataType: pgtype.TextOID},
	}
	rels := map[uint32]*format.Relation{
		1001: testBookRelation(1001, cols...),
		1002: testBookRelation(1002, cols...),
	}
	rels[1002].Name = "authors"

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1,
		SpillDir:       t.TempDir(),
	}, "toast_trunc", nil)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	require.NoError(t, sb.startTx(88))
	// Update with 'O' (REPLICA IDENTITY FULL) restoring unchanged TOAST 'u'
	require.NoError(t, sb.appendRaw(encodeStreamedUpdate(88, 1001, format.UpdateTupleTypeOld,
		[]wireTupleCol{wireTextCol("1"), wireTextCol("big-toast")},
		[]wireTupleCol{wireTextCol("1"), wireToastCol()},
	), 10, time.Time{}, rels))
	// Update with 'K' (REPLICA IDENTITY DEFAULT) keeping 'u' in UnchangedToastColumns
	require.NoError(t, sb.appendRaw(encodeStreamedUpdate(88, 1001, format.UpdateTupleTypeKey,
		[]wireTupleCol{wireTextCol("2"), wireNullCol()},
		[]wireTupleCol{wireTextCol("2"), wireToastCol()},
	), 20, time.Time{}, rels))
	// Delete
	require.NoError(t, sb.appendRaw(encodeStreamedDelete(88, 1001, format.UpdateTupleTypeKey,
		[]wireTupleCol{wireTextCol("3"), wireNullCol()},
	), 30, time.Time{}, rels))
	// Multi-OID Truncate
	require.NoError(t, sb.appendRaw(encodeStreamedTruncate(88, format.TruncateOptionCascade, 1001, 1002), 40, time.Time{}, rels))
	// Transactional LogicalDecodingMessage
	require.NoError(t, sb.appendRaw(encodeStreamedLogicalMsg(88, 1, 45, "audit", []byte("ok")), 50, time.Time{}, rels))
	require.NoError(t, sb.stopTx())

	var got []*Message
	require.NoError(t, sb.flushTx(88, func(m *Message) bool {
		got = append(got, m)
		return true
	}, 60, 70))

	require.Len(t, got, 5)

	updFull := got[0].message.(*format.Update)
	assert.Equal(t, "big-toast", updFull.NewDecoded["payload"])
	assert.Empty(t, updFull.UnchangedToastColumns)

	updKey := got[1].message.(*format.Update)
	assert.Equal(t, []string{"payload"}, updKey.UnchangedToastColumns)

	del := got[2].message.(*format.Delete)
	assert.Equal(t, int32(3), del.OldDecoded["id"])

	trunc := got[3].message.(*format.Truncate)
	assert.True(t, trunc.Cascade)
	require.Len(t, trunc.Relations, 2)
	assert.Equal(t, "books", trunc.Relations[0].Name)
	assert.Equal(t, "authors", trunc.Relations[1].Name)

	lm := got[4].message.(*format.LogicalDecodingMessage)
	assert.Equal(t, "audit", lm.Prefix)
	assert.Equal(t, []byte("ok"), lm.Content)
	assert.Equal(t, int64(70), got[4].walStart)
}

func TestStreamTxBufferContextCancelMidFlushKeepsTxOpenAndCloseCleansUp(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1,
		SpillDir:       t.TempDir(),
	}, "cancel_flush", nil)
	require.NoError(t, err)

	require.NoError(t, sb.startTx(99))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(99, 1001, wireTextCol("1"), wireTextCol("a")), 10, time.Time{}, rels))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(99, 1001, wireTextCol("2"), wireTextCol("b")), 20, time.Time{}, rels))
	require.NoError(t, sb.stopTx())

	sessionDir := sb.sessionDir
	sent := 0
	require.NoError(t, sb.flushTx(99, func(_ *Message) bool {
		sent++
		return false // simulate context cancellation on first message send
	}, 30, 40))

	assert.Equal(t, 1, sent)
	assert.True(t, sb.hasOpenTxns(), "cancelled replay must keep tx open so keepalive cannot advance")

	require.NoError(t, sb.close())
	_, statErr := os.Stat(sessionDir)
	assert.ErrorIs(t, statErr, os.ErrNotExist, "close must remove session dir and spill files")
}

type scriptedReceiveConn struct {
	standbyCaptureConn
	msgs []pgproto3.BackendMessage
	idx  int
}

func (c *scriptedReceiveConn) ReceiveMessage(_ context.Context) (pgproto3.BackendMessage, error) {
	if c.idx >= len(c.msgs) {
		return nil, context.Canceled
	}
	m := c.msgs[c.idx]
	c.idx++
	return m, nil
}

func TestStreamTxBufferDiskLimitFailsClosedAndOpenChecksSpillDir(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	t.Run("MaxDiskBytes triggers sinkLoop corrupted=true without advancing confirmedXLogPos", func(t *testing.T) {
		spillDir := t.TempDir()
		s := NewStream("", config.Config{}, metric.NewMetric("disk_limit"), func(*ListenerContext) {}).(*stream)
		s.relation[1001] = testBookRelation(1001)

		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
			Enabled:        true,
			MaxMemoryBytes: 1,
			MaxDiskBytes:   50,
			SpillDir:       spillDir,
		}, "disk_limit", s.metric)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()

		wrapCopy := func(lsn uint64, walData []byte) *pgproto3.CopyData {
			raw := encodeXLogData(pq.LSN(lsn), pq.LSN(lsn), time.Time{}, walData)
			return &pgproto3.CopyData{Data: append([]byte{message.XLogDataByteID}, raw...)}
		}
		s.conn = &scriptedReceiveConn{
			msgs: []pgproto3.BackendMessage{
				wrapCopy(10, encodeStreamStart(11, 1)),
				wrapCopy(20, encodeStreamedInsert(11, 1001, wireTextCol("1"), wireTextCol("exceeds-50-bytes"))),
			},
		}

		corrupted := s.sinkLoop(context.Background(), &messageBuffer{outCh: make(chan *Message, 4)}, sb)
		assert.True(t, corrupted, "spill disk limit exceeded must fail closed and restart stream")
		assert.Zero(t, s.LoadConfirmedXLogPos())
	})

	t.Run("unwritable spillDir fails Open immediately", func(t *testing.T) {
		fileAsDir := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(fileAsDir, []byte("x"), 0o600))

		cfg := config.Config{
			Slot: slot.Config{
				Name:         "bad_dir_slot",
				ProtoVersion: 2,
				Spill: slot.StreamSpillConfig{
					Enabled:        true,
					MaxMemoryBytes: 1024,
					SpillDir:       fileAsDir,
				},
			},
		}
		s := NewStream("", cfg, nil, func(*ListenerContext) {}).(*stream)
		s.conn = newFakeOpenConn()

		err := s.Open(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "stream spill buffer")
	})
}

// fakeOpenConn satisfies pq.Connection for Open() startup tests where
// replication.StartWithOptions / Test succeed immediately.
type fakeOpenConn struct {
	standbyCaptureConn
	frontend *pgproto3.Frontend
}

func newFakeOpenConn() *fakeOpenConn {
	var rw bytes.Buffer
	return &fakeOpenConn{
		frontend: pgproto3.NewFrontend(&rw, &rw),
	}
}

func (c *fakeOpenConn) Frontend() *pgproto3.Frontend {
	return c.frontend
}

func (c *fakeOpenConn) ReceiveMessage(_ context.Context) (pgproto3.BackendMessage, error) {
	return &pgproto3.CopyBothResponse{}, nil
}

func (c *fakeOpenConn) Exec(_ context.Context, _ string) *pgconn.MultiResultReader {
	return nil
}

func TestStreamTxBufferPermissionsAndDisabledZeroFS(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	t.Run("disabled spill performs zero filesystem access", func(t *testing.T) {
		badPath := filepath.Join(t.TempDir(), "nonexistent", "dir")
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
			Enabled:  false,
			SpillDir: badPath,
		}, "disabled_slot", nil)
		require.NoError(t, err)
		assert.Empty(t, sb.sessionDir)
		require.NoError(t, sb.close())
		_, statErr := os.Stat(badPath)
		assert.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("session dir is 0700 and spill file is 0600", func(t *testing.T) {
		rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
			Enabled:        true,
			MaxMemoryBytes: 1,
			SpillDir:       t.TempDir(),
		}, "perm/slot", nil)
		require.NoError(t, err)
		sessionDir := sb.sessionDir

		dirInfo, err := os.Stat(sessionDir)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())

		require.NoError(t, sb.startTx(123))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(123, 1001, wireTextCol("1"), wireTextCol("perm")), 10, time.Time{}, rels))
		require.NoError(t, sb.stopTx())

		fileInfo, err := os.Stat(sb.txns[123].filePath)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm())

		require.NoError(t, sb.close())
		_, statErr := os.Stat(sessionDir)
		assert.ErrorIs(t, statErr, os.ErrNotExist)
	})
}

func TestStreamTxBufferPrometheusMetricsLifecycle(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	m := metric.NewMetric("metrics_lifecycle")
	reg := metric.NewRegistry(m)
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 120,
		SpillDir:       t.TempDir(),
	}, "metrics_lifecycle", m)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	// TX 1 stays in memory.
	require.NoError(t, sb.startTx(1))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(1, 1001, wireTextCol("1"), wireTextCol("in-mem")), 10, time.Time{}, rels))
	require.NoError(t, sb.stopTx())

	active, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_active_transactions")
	bufBytes, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_buffer_bytes")
	spilledBytes, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_spilled_bytes")
	assert.InDelta(t, 1, active, 0)
	assert.Positive(t, bufBytes)
	assert.Zero(t, spilledBytes)

	// TX 2 pushes totalMemBytes over 120 -> largest spills.
	require.NoError(t, sb.startTx(2))
	for i := 0; i < 3; i++ {
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(2, 1001, wireTextCol("2"), wireTextCol("spill-me")), pq.LSN(20+i), time.Time{}, rels))
	}
	require.NoError(t, sb.stopTx())

	active, _ = gatherMetricValue(t, reg, "go_pq_cdc_streaming_active_transactions")
	spilledBytes, _ = gatherMetricValue(t, reg, "go_pq_cdc_streaming_spilled_bytes")
	spillBytesTotal, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_spill_bytes_total")
	spillTxTotal, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_spill_transactions_total")
	assert.InDelta(t, 2, active, 0)
	assert.Positive(t, spilledBytes)
	assert.InDelta(t, spilledBytes, spillBytesTotal, 0)
	assert.InDelta(t, 1, spillTxTotal, 0)

	// Abort TX 2 (spilled), commit TX 1 (in memory) -> gauges return to 0.
	require.NoError(t, sb.abortTx(2, 2))
	require.NoError(t, sb.flushTx(1, func(*Message) bool { return true }, 30, 40))

	active, _ = gatherMetricValue(t, reg, "go_pq_cdc_streaming_active_transactions")
	bufBytes, _ = gatherMetricValue(t, reg, "go_pq_cdc_streaming_buffer_bytes")
	spilledBytes, _ = gatherMetricValue(t, reg, "go_pq_cdc_streaming_spilled_bytes")
	abortedTotal, _ := gatherMetricValue(t, reg, "go_pq_cdc_streaming_aborted_transactions_total")
	assert.Zero(t, active)
	assert.Zero(t, bufBytes)
	assert.Zero(t, spilledBytes)
	assert.InDelta(t, 1, abortedTotal, 0)
}

func TestStreamTxBufferRejectsTruncatedAndCorruptFrames(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	t.Run("external spill file truncation at frame boundary fails closed", func(t *testing.T) {
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{
			Enabled:        true,
			MaxMemoryBytes: 1,
			SpillDir:       t.TempDir(),
		}, "trunc_boundary", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()

		require.NoError(t, sb.startTx(11))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(11, 1001, wireTextCol("1"), wireTextCol("row1")), 10, time.Time{}, rels))
		firstRowSize := sb.txns[11].size
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(11, 1001, wireTextCol("2"), wireTextCol("row2")), 20, time.Time{}, rels))
		require.NoError(t, sb.stopTx())

		// Truncate on exact frame boundary back to firstRowSize so frame reading hits io.EOF cleanly if unchecked.
		require.NoError(t, os.Truncate(sb.txns[11].filePath, firstRowSize))

		var delivered int
		err = sb.flushTx(11, func(*Message) bool {
			delivered++
			return true
		}, 30, 40)
		require.ErrorContains(t, err, "truncated stream spill file")
		assert.Zero(t, delivered, "must fail before emitting partial rows")
		assert.True(t, sb.hasOpenTxns(), "failed replay must keep tx open")
	})

	t.Run("in-memory size mismatch at EOF fails closed", func(t *testing.T) {
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{}, "trunc_eof", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()

		require.NoError(t, sb.startTx(12))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(12, 1001, wireTextCol("1"), wireTextCol("row1")), 10, time.Time{}, rels))
		require.NoError(t, sb.stopTx())

		sb.txns[12].size += 10 // simulate missing tail bytes
		err = sb.flushTx(12, func(*Message) bool { return true }, 30, 40)
		require.ErrorContains(t, err, "truncated stream spill data")
	})

	t.Run("bogus dataLen exceeding remaining bytes fails closed without OOM allocation", func(t *testing.T) {
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{}, "bogus_datalen", nil)
		require.NoError(t, err)
		defer func() { _ = sb.close() }()

		require.NoError(t, sb.startTx(13))
		require.NoError(t, sb.appendRaw(encodeStreamedInsert(13, 1001, wireTextCol("1"), wireTextCol("row1")), 10, time.Time{}, rels))
		require.NoError(t, sb.stopTx())

		tx := sb.txns[13]
		// Overwrite the wire frame's 4-byte length field (after 9-byte relBind + 1+8+8 header) with 2 GiB.
		lenOffset := relBindFrameSize + 1 + 8 + 8
		binary.BigEndian.PutUint32(tx.memBuf[lenOffset:lenOffset+4], 0x7fffffff)

		err = sb.flushTx(13, func(*Message) bool { return true }, 30, 40)
		require.ErrorContains(t, err, "invalid wire frame payload length")
	})
}

func TestStreamTxBufferUnknownMessageByteDoesNotPolluteSubXacts(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{}, "unknown_byte", nil)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	require.NoError(t, sb.startTx(100))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(100, 1001, wireTextCol("1"), wireTextCol("keep")), 10, time.Time{}, rels))

	// Unknown message byte 'Z' whose bytes [1:5] match future sub-XID 200.
	unknown := make([]byte, 8)
	unknown[0] = 'Z'
	binary.BigEndian.PutUint32(unknown[1:5], 200)
	sizeBefore := sb.totalMemBytes
	require.NoError(t, sb.appendRaw(unknown, 15, time.Time{}, rels))
	assert.Equal(t, sizeBefore, sb.totalMemBytes, "unknown message must not consume buffer budget")
	assert.Empty(t, sb.txns[100].subXacts, "unknown message must not record a subXact")

	// Now real sub-XID 200 inserts and rolls back: "keep" must survive and "drop" must be truncated.
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(200, 1001, wireTextCol("2"), wireTextCol("drop")), 20, time.Time{}, rels))
	require.NoError(t, sb.stopTx())
	require.NoError(t, sb.abortTx(100, 200))

	var got []string
	require.NoError(t, sb.flushTx(100, func(m *Message) bool {
		got = append(got, m.message.(*format.Insert).Decoded["name"].(string))
		return true
	}, 30, 40))
	assert.Equal(t, []string{"keep"}, got)
}

func TestStreamTxBufferInMemorySubTxRollbackShrinksBackingArray(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	rels := map[uint32]*format.Relation{1001: testBookRelation(1001)}

	sb, err := newStreamTxBuffer(slot.StreamSpillConfig{}, "shrink_membuf", nil)
	require.NoError(t, err)
	defer func() { _ = sb.close() }()

	require.NoError(t, sb.startTx(100))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(100, 1001, wireTextCol("1"), wireTextCol("keep")), 10, time.Time{}, rels))

	bigPayload := string(bytes.Repeat([]byte("a"), 200*1024))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(200, 1001, wireTextCol("2"), wireTextCol(bigPayload)), 20, time.Time{}, rels))
	require.Greater(t, cap(sb.txns[100].memBuf), 200*1024)

	require.NoError(t, sb.abortTx(100, 200))
	assert.Less(t, cap(sb.txns[100].memBuf), 4096, "backing array capacity should shrink after large sub-TX rollback")

	// Truncating back to offset 0 releases memBuf completely.
	require.NoError(t, sb.startTx(101))
	require.NoError(t, sb.appendRaw(encodeStreamedInsert(201, 1001, wireTextCol("3"), wireTextCol(bigPayload)), 30, time.Time{}, rels))
	require.NoError(t, sb.abortTx(101, 201))
	assert.Nil(t, sb.txns[101].memBuf)
	assert.Zero(t, cap(sb.txns[101].memBuf))
}

func TestStreamTxBufferOpenCleansSessionDirOnVisibilityGuardErrorAndCloseIsIdempotent(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	spillDir := t.TempDir()

	cfg := config.Config{
		Host: "127.0.0.1",
		Port: 1, // closed port so openVisibilityGuard fails deterministically
		Slot: slot.Config{
			Name:         "vis_fail_slot",
			ProtoVersion: 2,
			Spill: slot.StreamSpillConfig{
				Enabled:        true,
				MaxMemoryBytes: 1024,
				SpillDir:       spillDir,
			},
		},
		VisibilityGuard: config.VisibilityGuardConfig{
			Enabled:      true,
			FailMode:     config.VisibilityFailClosed,
			Timeout:      time.Second,
			PollInterval: time.Millisecond,
		},
	}
	s := NewStream("", cfg, nil, func(*ListenerContext) {}).(*stream)
	s.conn = newFakeOpenConn()
	s.system = &pq.IdentifySystemResult{Timeline: 1}

	err := s.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "visibility guard")

	entries, readErr := os.ReadDir(spillDir)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "session dir created in Open must be removed if visibility guard setup fails")

	// Repeat close() on streamTxBuffer is a safe no-op.
	sb, sbErr := newStreamTxBuffer(slot.StreamSpillConfig{Enabled: true, MaxMemoryBytes: 1024, SpillDir: spillDir}, "idemp", nil)
	require.NoError(t, sbErr)
	require.NoError(t, sb.close())
	require.NoError(t, sb.close())
}
