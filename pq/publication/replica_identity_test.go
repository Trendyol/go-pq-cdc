package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentityOnlyTables(t *testing.T) {
	t.Run("should ignore columns and partitioned so Diff matches on identity only", func(t *testing.T) {
		configured := Tables{
			{
				Name:            "books",
				Schema:          "public",
				ReplicaIdentity: ReplicaIdentityFull,
				Columns:         []string{"id", "name"},
				Partitioned:     true,
			},
		}
		actual := Tables{
			{
				Name:            "books",
				Schema:          "public",
				ReplicaIdentity: ReplicaIdentityFull,
			},
		}

		require.NotEmpty(t, configured.Diff(actual), "Diff includes Columns/Partitioned by design")

		diff := identityOnlyTables(configured).Diff(actual)
		assert.Empty(t, diff)
	})

	t.Run("should still detect replica identity mismatches", func(t *testing.T) {
		configured := Tables{
			{
				Name:            "books",
				Schema:          "public",
				ReplicaIdentity: ReplicaIdentityFull,
				Columns:         []string{"id"},
			},
		}
		actual := Tables{
			{
				Name:            "books",
				Schema:          "public",
				ReplicaIdentity: ReplicaIdentityDefault,
			},
		}

		diff := identityOnlyTables(configured).Diff(actual)
		require.Len(t, diff, 1)
		assert.Equal(t, ReplicaIdentityFull, diff[0].ReplicaIdentity)
		assert.Empty(t, diff[0].Columns)
	})
}

func TestMapReplicaIdentity(t *testing.T) {
	t.Run("should map postgres relreplident codes", func(t *testing.T) {
		assert.Equal(t, ReplicaIdentityDefault, mapReplicaIdentity(int32('d')))
		assert.Equal(t, ReplicaIdentityFull, mapReplicaIdentity(int32('f')))
		assert.Equal(t, ReplicaIdentityNothing, mapReplicaIdentity(int32('n')))
		assert.Equal(t, ReplicaIdentityUsingIndex, mapReplicaIdentity(int32('i')))
	})

	t.Run("should handle string values", func(t *testing.T) {
		assert.Equal(t, ReplicaIdentityDefault, mapReplicaIdentity("d"))
		assert.Equal(t, ReplicaIdentityUsingIndex, mapReplicaIdentity("i"))
	})

	t.Run("should return raw code for unknown values", func(t *testing.T) {
		assert.Equal(t, "x", mapReplicaIdentity("x"))
		assert.Equal(t, "x", mapReplicaIdentity(int32('x')))
	})
}

func TestQualifiedTableName(t *testing.T) {
	t.Run("should use explicit schema", func(t *testing.T) {
		assert.Equal(t, "custom.books", qualifiedTableName(Table{Schema: "custom", Name: "books"}))
	})

	t.Run("should use schema from dotted table name", func(t *testing.T) {
		assert.Equal(t, "custom.books", qualifiedTableName(Table{Name: "custom.books"}))
	})

	t.Run("should default to public schema", func(t *testing.T) {
		assert.Equal(t, "public.books", qualifiedTableName(Table{Name: "books"}))
	})
}
