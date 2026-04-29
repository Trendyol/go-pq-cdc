package snapshot

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
)

func ptrInt64(v int64) *int64 { return &v }

func TestAndCondition(t *testing.T) {
	t.Run("both empty returns empty", func(t *testing.T) {
		assert.Equal(t, "", andCondition("", ""))
	})
	t.Run("only existing returns existing", func(t *testing.T) {
		assert.Equal(t, "a = 1", andCondition("a = 1", ""))
	})
	t.Run("only extra returns parenthesized", func(t *testing.T) {
		assert.Equal(t, "(b = 2)", andCondition("", "b = 2"))
	})
	t.Run("combines with AND and parenthesizes extra for OR precedence", func(t *testing.T) {
		assert.Equal(t, "a = 1 AND (b = 2)", andCondition("a = 1", "b = 2"))
	})
}

func TestGetQueryCondition(t *testing.T) {
	newSnapshotter := func(cfg config.SnapshotConfig, tables publication.Tables) *Snapshotter {
		return &Snapshotter{config: cfg, tables: tables}
	}

	t.Run("returns empty when no conditions defined", func(t *testing.T) {
		s := newSnapshotter(config.SnapshotConfig{}, publication.Tables{
			{Name: "users", Schema: "public"},
		})
		assert.Equal(t, "", s.getQueryCondition("public", "users"))
	})

	t.Run("returns global condition when no per-table override", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{{Name: "users", Schema: "public"}},
		)
		assert.Equal(t, "is_active = true", s.getQueryCondition("public", "users"))
	})

	t.Run("per-table condition overrides global", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{
				{Name: "users", Schema: "public", QueryCondition: "deleted_at IS NULL"},
			},
		)
		assert.Equal(t, "deleted_at IS NULL", s.getQueryCondition("public", "users"))
	})

	t.Run("empty per-table falls back to global", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{
				{Name: "users", Schema: "public", QueryCondition: ""},
				{Name: "orders", Schema: "public", QueryCondition: "status <> 'cancelled'"},
			},
		)
		assert.Equal(t, "is_active = true", s.getQueryCondition("public", "users"))
		assert.Equal(t, "status <> 'cancelled'", s.getQueryCondition("public", "orders"))
	})

	t.Run("empty schema is normalized to public", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{},
			publication.Tables{
				{Name: "users", Schema: "", QueryCondition: "active"},
			},
		)
		assert.Equal(t, "active", s.getQueryCondition("", "users"))
		assert.Equal(t, "active", s.getQueryCondition("public", "users"))
	})

	t.Run("different schema does not match", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "global_cond"},
			publication.Tables{
				{Name: "users", Schema: "tenant_a", QueryCondition: "table_cond"},
			},
		)
		assert.Equal(t, "global_cond", s.getQueryCondition("public", "users"))
		assert.Equal(t, "table_cond", s.getQueryCondition("tenant_a", "users"))
	})
}

func TestBuildChunkQueryWithCondition(t *testing.T) {
	s := &Snapshotter{}

	t.Run("integer range injects condition into WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(1000),
			ChunkSize:         500,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "is_active = true")
		assert.Contains(t, q, "WHERE id >= 1 AND id <= 1000 AND (is_active = true)")
		assert.Contains(t, q, "ORDER BY id LIMIT 500")
	})

	t.Run("integer range without condition is unchanged from legacy shape", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(10),
			ChunkSize:         5,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "")
		assert.Equal(t, "SELECT * FROM public.users WHERE id >= 1 AND id <= 10 ORDER BY id LIMIT 5", q)
	})

	t.Run("offset strategy injects condition into WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "orders",
			PartitionStrategy: PartitionStrategyOffset,
			ChunkSize:         100,
			ChunkStart:        200,
		}
		q := s.buildChunkQuery(chunk, "id", nil, "status = 'active'")
		assert.Contains(t, q, "WHERE (status = 'active')")
		assert.Contains(t, q, "ORDER BY id LIMIT 100 OFFSET 200")
	})

	t.Run("OR in condition is parenthesized with integer range", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(1000),
			ChunkSize:         500,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "status = 'a' OR status = 'b'")
		assert.Contains(t, q, "AND (status = 'a' OR status = 'b')")
	})

	t.Run("offset strategy without condition omits WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "orders",
			PartitionStrategy: PartitionStrategyOffset,
			ChunkSize:         100,
			ChunkStart:        0,
		}
		q := s.buildChunkQuery(chunk, "id", nil, "")
		assert.NotContains(t, q, "WHERE")
	})

	t.Run("ctid block with bounds injects condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
			BlockStart:        ptrInt64(0),
			BlockEnd:          ptrInt64(100),
		}
		q := s.buildChunkQuery(chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, q, "WHERE ctid >= '(0,0)'::tid AND ctid < '(100,0)'::tid AND (tenant_id = 7)")
	})

	t.Run("ctid last chunk (nil BlockEnd) injects condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
			BlockStart:        ptrInt64(50),
			BlockEnd:          nil,
			IsLastChunk:       true,
		}
		q := s.buildChunkQuery(chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, q, "WHERE ctid >= '(50,0)'::tid AND (tenant_id = 7)")
	})

	t.Run("ctid empty table with condition uses WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
		}
		q := s.buildChunkQuery(chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, q, "FROM public.events WHERE (tenant_id = 7)")
	})

	t.Run("ctid empty table without condition has no WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
		}
		q := s.buildChunkQuery(chunk, "", nil, "")
		assert.NotContains(t, q, "WHERE")
	})

	t.Run("integer range without bounds falls back to offset with condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			ChunkSize:         10,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "is_active = true")
		assert.Contains(t, q, "WHERE (is_active = true)")
		assert.Contains(t, q, "ORDER BY id LIMIT 10")
	})
}
