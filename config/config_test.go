package config

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSnapshotTables(t *testing.T) {
	t.Run("should preserve SnapshotPartitionStrategy from snapshot.tables when publication exists", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Enabled: true,
				Mode:    SnapshotModeInitial,
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
					},
				},
			},
		}
		pubInfo := &publication.Config{
			Tables: publication.Tables{
				{Name: "events", Schema: "public", SnapshotPartitionStrategy: ""},
			},
		}

		tables, err := cfg.GetSnapshotTables(pubInfo)

		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, publication.SnapshotPartitionStrategyCTIDBlock, tables[0].SnapshotPartitionStrategy)
	})

	t.Run("should preserve SnapshotPartitionStrategy from publication.tables when snapshot.tables is empty", func(t *testing.T) {
		cfg := Config{
			Publication: publication.Config{
				Tables: publication.Tables{
					{
						Name:                      "orders",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyIntegerRange,
					},
				},
			},
			Snapshot: SnapshotConfig{
				Enabled: true,
				Mode:    SnapshotModeInitial,
				Tables:  nil,
			},
		}
		pubInfo := &publication.Config{
			Tables: publication.Tables{
				{Name: "orders", Schema: "public", SnapshotPartitionStrategy: ""},
			},
		}

		tables, err := cfg.GetSnapshotTables(pubInfo)

		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, publication.SnapshotPartitionStrategyIntegerRange, tables[0].SnapshotPartitionStrategy)
	})

	t.Run("should prioritize snapshot.tables strategy over publication.tables strategy", func(t *testing.T) {
		cfg := Config{
			Publication: publication.Config{
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyIntegerRange,
					},
				},
			},
			Snapshot: SnapshotConfig{
				Enabled: true,
				Mode:    SnapshotModeInitial,
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
					},
				},
			},
		}
		pubInfo := &publication.Config{
			Tables: publication.Tables{
				{Name: "events", Schema: "public", SnapshotPartitionStrategy: ""},
			},
		}

		tables, err := cfg.GetSnapshotTables(pubInfo)

		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, publication.SnapshotPartitionStrategyCTIDBlock, tables[0].SnapshotPartitionStrategy)
	})

	t.Run("should use auto-detect when no strategy specified in config", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Enabled: true,
				Mode:    SnapshotModeInitial,
				Tables:  nil,
			},
		}
		pubInfo := &publication.Config{
			Tables: publication.Tables{
				{Name: "users", Schema: "public", SnapshotPartitionStrategy: ""},
			},
		}

		tables, err := cfg.GetSnapshotTables(pubInfo)

		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, publication.SnapshotPartitionStrategyAuto, tables[0].SnapshotPartitionStrategy)
	})

	t.Run("should return error when snapshot.tables contains table not in publication", func(t *testing.T) {
		cfg := Config{
			Publication: publication.Config{Name: "test_pub"},
			Snapshot: SnapshotConfig{
				Enabled: true,
				Mode:    SnapshotModeInitial,
				Tables: publication.Tables{
					{Name: "non_existent", Schema: "public"},
				},
			},
		}
		pubInfo := &publication.Config{
			Tables: publication.Tables{
				{Name: "events", Schema: "public"},
			},
		}

		_, err := cfg.GetSnapshotTables(pubInfo)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "snapshot table 'public.non_existent' not found in publication")
	})

	t.Run("should return snapshot.tables directly for snapshot_only mode", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Enabled: true,
				Mode:    SnapshotModeSnapshotOnly,
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
					},
				},
			},
		}

		tables, err := cfg.GetSnapshotTables(nil)

		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, "events", tables[0].Name)
		assert.Equal(t, publication.SnapshotPartitionStrategyCTIDBlock, tables[0].SnapshotPartitionStrategy)
	})

	t.Run("should return error for snapshot_only mode when tables not specified", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Enabled: true,
				Mode:    SnapshotModeSnapshotOnly,
				Tables:  nil,
			},
		}

		_, err := cfg.GetSnapshotTables(nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "snapshot.tables must be specified for snapshot_only mode")
	})
}

func TestMergePublicationTableConfig(t *testing.T) {
	t.Run("should merge strategy from publication config", func(t *testing.T) {
		cfg := Config{
			Publication: publication.Config{
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
					},
					{
						Name:                      "orders",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyOffset,
					},
				},
			},
		}
		pubInfoTables := publication.Tables{
			{Name: "events", Schema: "public", SnapshotPartitionStrategy: ""},
			{Name: "orders", Schema: "public", SnapshotPartitionStrategy: ""},
			{Name: "users", Schema: "public", SnapshotPartitionStrategy: ""},
		}

		result := cfg.mergePublicationTableConfig(pubInfoTables)

		require.Len(t, result, 3)
		assert.Equal(t, publication.SnapshotPartitionStrategyCTIDBlock, result[0].SnapshotPartitionStrategy)
		assert.Equal(t, publication.SnapshotPartitionStrategyOffset, result[1].SnapshotPartitionStrategy)
		assert.Equal(t, publication.SnapshotPartitionStrategyAuto, result[2].SnapshotPartitionStrategy)
	})

	t.Run("should return original tables when publication config is empty", func(t *testing.T) {
		cfg := Config{
			Publication: publication.Config{Tables: nil},
		}
		pubInfoTables := publication.Tables{
			{Name: "events", Schema: "public"},
		}

		result := cfg.mergePublicationTableConfig(pubInfoTables)

		assert.Equal(t, pubInfoTables, result)
	})

	t.Run("should preserve other table properties while merging strategy", func(t *testing.T) {
		cfg := Config{
			Publication: publication.Config{
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
					},
				},
			},
		}
		pubInfoTables := publication.Tables{
			{Name: "events", Schema: "public", ReplicaIdentity: "full", SnapshotPartitionStrategy: ""},
		}

		result := cfg.mergePublicationTableConfig(pubInfoTables)

		require.Len(t, result, 1)
		assert.Equal(t, "events", result[0].Name)
		assert.Equal(t, "public", result[0].Schema)
		assert.Equal(t, "full", result[0].ReplicaIdentity)
		assert.Equal(t, publication.SnapshotPartitionStrategyCTIDBlock, result[0].SnapshotPartitionStrategy)
	})
}

func TestValidateSnapshotSubset(t *testing.T) {
	t.Run("should preserve SnapshotPartitionStrategy from snapshot config", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
					},
				},
			},
		}
		pubTables := publication.Tables{
			{Name: "events", Schema: "public", ReplicaIdentity: "full", SnapshotPartitionStrategy: ""},
		}

		result, err := cfg.validateSnapshotSubset(pubTables)

		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, "events", result[0].Name)
		assert.Equal(t, "full", result[0].ReplicaIdentity)
		assert.Equal(t, publication.SnapshotPartitionStrategyCTIDBlock, result[0].SnapshotPartitionStrategy)
	})

	t.Run("should return error when publication tables are empty", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Tables: publication.Tables{
					{Name: "events", Schema: "public"},
				},
			},
		}

		_, err := cfg.validateSnapshotSubset(publication.Tables{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "publication has no tables defined")
	})

	t.Run("should use auto-detect when snapshot table has no strategy specified", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: "",
					},
				},
			},
		}
		pubTables := publication.Tables{
			{Name: "events", Schema: "public", SnapshotPartitionStrategy: ""},
		}

		result, err := cfg.validateSnapshotSubset(pubTables)

		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, publication.SnapshotPartitionStrategyAuto, result[0].SnapshotPartitionStrategy)
	})

	t.Run("should validate multiple tables with different strategies", func(t *testing.T) {
		cfg := Config{
			Snapshot: SnapshotConfig{
				Tables: publication.Tables{
					{
						Name:                      "events",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
					},
					{
						Name:                      "orders",
						Schema:                    "public",
						SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyIntegerRange,
					},
				},
			},
		}
		pubTables := publication.Tables{
			{Name: "events", Schema: "public"},
			{Name: "orders", Schema: "public"},
			{Name: "users", Schema: "public"},
		}

		result, err := cfg.validateSnapshotSubset(pubTables)

		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, publication.SnapshotPartitionStrategyCTIDBlock, result[0].SnapshotPartitionStrategy)
		assert.Equal(t, publication.SnapshotPartitionStrategyIntegerRange, result[1].SnapshotPartitionStrategy)
	})
}
