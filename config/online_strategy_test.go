package config

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/stretchr/testify/require"
)

func onlineTestConfig() Config {
	c := Config{Host: "localhost", Username: "u", Password: "p", Database: "db",
		Publication: publication.Config{Name: "pub"}, Slot: slot.Config{Name: "slot", ProtoVersion: 1},
		Snapshot: SnapshotConfig{Enabled: true, Mode: SnapshotModeInitial, Strategy: SnapshotStrategyOnline}}
	c.SetDefault()
	return c
}

func TestOnlineStrategyValidation(t *testing.T) {
	t.Run("legacy defaults", func(t *testing.T) {
		c := Config{Snapshot: SnapshotConfig{Enabled: true}}
		c.SetDefault()
		require.Equal(t, SnapshotStrategyConsistent, c.Snapshot.Strategy)
		require.Equal(t, 2, c.Slot.ProtoVersion)
		require.Equal(t, SnapshotModeNever, c.Snapshot.Mode)
		require.EqualValues(t, 8000, c.Snapshot.ChunkSize)
		require.Zero(t, c.Snapshot.Online.QueueBytes)
	})
	t.Run("online defaults and explicit protocol", func(t *testing.T) {
		c := onlineTestConfig()
		require.NoError(t, c.Validate())
		require.Equal(t, 5*time.Second, c.Snapshot.Online.QueryTimeout)
		require.Equal(t, 30*time.Second, c.Snapshot.Online.DeliveryTimeout)
		require.EqualValues(t, 4<<20, c.Snapshot.Online.ChunkBytes)
		require.EqualValues(t, 16<<20, c.Snapshot.Online.TransactionBytes)
		require.EqualValues(t, 32<<20, c.Snapshot.Online.QueueBytes)
		require.Equal(t, time.Second, c.Snapshot.Online.CheckpointInterval)
		require.Equal(t, 5*time.Second, c.Snapshot.Online.MarkerInterval)
		c.Slot.ProtoVersion = 0
		c.SetDefault()
		require.Equal(t, 2, c.Slot.ProtoVersion)
		require.ErrorContains(t, c.Validate(), "protocol 1")
	})
	cases := []struct {
		name, want string
		mutate     func(*Config)
	}{
		{"disabled", "enabled", func(c *Config) { c.Snapshot.Enabled = false }},
		{"never", "initial", func(c *Config) { c.Snapshot.Mode = SnapshotModeNever }},
		{"only", "initial", func(c *Config) { c.Snapshot.Mode = SnapshotModeSnapshotOnly }},
		{"unknown", "strategy", func(c *Config) { c.Snapshot.Strategy = "typo" }},
		{"protocol2", "protocol 1", func(c *Config) { c.Slot.ProtoVersion = 2 }},
		{"resnapshot", "resnapshot", func(c *Config) { c.Snapshot.Resnapshot = true }},
		{"broad publication", "explicit table", func(c *Config) { c.Publication.AllTables = true }},
		{"operations", "INSERT, UPDATE, DELETE, TRUNCATE", func(c *Config) {
			c.Publication.CreateIfNotExists = true
			c.Publication.Tables = publication.Tables{{Name: "t", ReplicaIdentity: publication.ReplicaIdentityFull}}
			c.Publication.Operations = publication.Operations{"insert"}
		}},
		{"global condition", "queryCondition", func(c *Config) { c.Snapshot.QueryCondition = "id > 1" }},
		{"publication condition", "queryCondition", func(c *Config) { c.Publication.Tables = publication.Tables{{Name: "t", QueryCondition: "id > 1"}} }},
		{"snapshot condition", "queryCondition", func(c *Config) { c.Snapshot.Tables = publication.Tables{{Name: "t", QueryCondition: "id > 1"}} }},
		{"column list", "column", func(c *Config) { c.Publication.Tables = publication.Tables{{Name: "t", Columns: []string{"id"}}} }},
		{"partition", "partition", func(c *Config) { c.Publication.Tables = publication.Tables{{Name: "t", Partitioned: true}} }},
		{"partition strategy", "partition strategy", func(c *Config) {
			c.Snapshot.Tables = publication.Tables{{Name: "t", SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyOffset}}
		}},
		{"extension", "TimescaleDB", func(c *Config) { c.ExtensionSupport.EnableTimeScaleDB = true }},
		{"failover", "failover", func(c *Config) { c.Slot.Failover = true }},
		{"replicas", "visibilityGuard.replicas", func(c *Config) { c.VisibilityGuard.Replicas = []string{"standby:5432"} }},
		{"incomplete identity", "replicaIdentity", func(c *Config) {
			c.Publication.Tables = publication.Tables{{Name: "t", ReplicaIdentity: publication.ReplicaIdentityDefault}}
		}},
		{"unknown operation", "undefined operation", func(c *Config) {
			c.Publication.Operations = append(append(publication.Operations(nil), publication.OperationOptions...), "UNKNOWN")
		}},
		{"snapshot column list", "column", func(c *Config) { c.Snapshot.Tables = publication.Tables{{Name: "t", Columns: []string{"id"}}} }},
		{"snapshot partition", "partition", func(c *Config) { c.Snapshot.Tables = publication.Tables{{Name: "t", Partitioned: true}} }},
		{"fail open", "fail closed", func(c *Config) {
			c.VisibilityGuard.Enabled = true
			c.VisibilityGuard.FailMode = VisibilityFailOpen
			c.SetDefault()
		}},
		{"query timeout", "queryTimeout", func(c *Config) { c.Snapshot.Online.QueryTimeout = -1 }},
		{"delivery timeout", "deliveryTimeout", func(c *Config) { c.Snapshot.Online.DeliveryTimeout = -1 }},
		{"chunk bytes", "chunkBytes", func(c *Config) { c.Snapshot.Online.ChunkBytes = -1 }},
		{"transaction bytes", "transactionBytes", func(c *Config) { c.Snapshot.Online.TransactionBytes = -1 }},
		{"queue bytes", "queueBytes", func(c *Config) { c.Snapshot.Online.QueueBytes = -1 }},
		{"queue capacity", "queueBytes", func(c *Config) { c.Snapshot.Online.QueueBytes = c.Snapshot.Online.TransactionBytes - 1 }},
		{"checkpoint interval", "checkpointInterval", func(c *Config) { c.Snapshot.Online.CheckpointInterval = -1 }},
		{"marker interval", "markerInterval", func(c *Config) { c.Snapshot.Online.MarkerInterval = -1 }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := onlineTestConfig()
			tt.mutate(&c)
			require.ErrorContains(t, c.Validate(), tt.want)
		})
	}
	t.Run("positive limits required without defaulting", func(t *testing.T) {
		c := onlineTestConfig()
		c.Snapshot.Online = OnlineSnapshotConfig{}
		require.Error(t, c.Validate())
	})
	t.Run("consistent restrictions unchanged", func(t *testing.T) {
		c := onlineTestConfig()
		c.Snapshot.Strategy = SnapshotStrategyConsistent
		c.Snapshot.Resnapshot = true
		c.Slot.ProtoVersion = 2
		c.Snapshot.QueryCondition = "id > 1"
		require.NoError(t, c.Validate())
	})
}
