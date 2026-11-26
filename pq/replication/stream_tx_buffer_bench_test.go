package replication

import (
	"fmt"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgtype"
)

func buildBenchStreamRows(n int) ([][]byte, map[uint32]*format.Relation) {
	rels := map[uint32]*format.Relation{
		1001: {
			OID:       1001,
			Namespace: "public",
			Name:      "books",
			Columns: []tuple.RelationColumn{
				{Flags: 1, Name: "id", DataType: pgtype.Int4OID},
				{Name: "name", DataType: pgtype.TextOID},
			},
		},
	}
	rows := make([][]byte, n)
	for i := range rows {
		rows[i] = encodeStreamedInsert(100, 1001, wireTextCol(fmt.Sprintf("%d", i)), wireTextCol("benchmark-book-title"))
	}
	return rows, rels
}

func BenchmarkStreamTxBufferCommit(b *testing.B) {
	const rowCount = 10_000
	rows, rels := buildBenchStreamRows(rowCount)
	srvTime := time.Unix(1_700_000_000, 0).UTC()

	run := func(b *testing.B, cfg slot.StreamSpillConfig) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			sb, err := newStreamTxBuffer(cfg, "bench_commit", nil)
			if err != nil {
				b.Fatal(err)
			}
			if err := sb.startTx(100); err != nil {
				b.Fatal(err)
			}
			for i, raw := range rows {
				if err := sb.appendRaw(raw, pq.LSN(100+i), srvTime, rels); err != nil {
					b.Fatal(err)
				}
			}
			if err := sb.stopTx(); err != nil {
				b.Fatal(err)
			}
			if err := sb.flushTx(100, func(*Message) bool { return true }, 20000, 20010); err != nil {
				b.Fatal(err)
			}
			if err := sb.close(); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("in_memory_raw", func(b *testing.B) {
		run(b, slot.StreamSpillConfig{})
	})

	b.Run("spilled", func(b *testing.B) {
		run(b, slot.StreamSpillConfig{
			Enabled:        true,
			MaxMemoryBytes: 64 * 1024,
			SpillDir:       b.TempDir(),
		})
	})
}

func BenchmarkStreamTxBufferAbort(b *testing.B) {
	const rowCount = 10_000
	rows, rels := buildBenchStreamRows(rowCount)
	srvTime := time.Unix(1_700_000_000, 0).UTC()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sb, err := newStreamTxBuffer(slot.StreamSpillConfig{}, "bench_abort", nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := sb.startTx(100); err != nil {
			b.Fatal(err)
		}
		for i, raw := range rows {
			if err := sb.appendRaw(raw, pq.LSN(100+i), srvTime, rels); err != nil {
				b.Fatal(err)
			}
		}
		if err := sb.stopTx(); err != nil {
			b.Fatal(err)
		}
		if err := sb.abortTx(100, 100); err != nil {
			b.Fatal(err)
		}
		if err := sb.close(); err != nil {
			b.Fatal(err)
		}
	}
}
