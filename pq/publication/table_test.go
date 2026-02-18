package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableValidateReplicaIdentityIndex(t *testing.T) {
	t.Run("should allow using index with index name", func(t *testing.T) {
		table := Table{
			Name:                 "books",
			Schema:               "public",
			ReplicaIdentity:      ReplicaIdentityUsingIndex,
			ReplicaIdentityIndex: "books_name_unique_idx",
		}

		require.NoError(t, table.Validate())
	})

	t.Run("should reject using index without index name", func(t *testing.T) {
		table := Table{
			Name:            "books",
			Schema:          "public",
			ReplicaIdentity: ReplicaIdentityUsingIndex,
		}

		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "replicaIdentityIndex cannot be empty")
	})

	t.Run("should reject index name when identity is not using index", func(t *testing.T) {
		table := Table{
			Name:                 "books",
			Schema:               "public",
			ReplicaIdentity:      ReplicaIdentityDefault,
			ReplicaIdentityIndex: "books_name_unique_idx",
		}

		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "replicaIdentityIndex can only be set")
	})
}

func TestTablesDiffReplicaIdentityIndex(t *testing.T) {
	current := Tables{
		{
			Name:                 "books",
			Schema:               "public",
			ReplicaIdentity:      ReplicaIdentityUsingIndex,
			ReplicaIdentityIndex: "books_name_unique_idx",
		},
	}

	t.Run("should include table when replica identity index changes", func(t *testing.T) {
		desired := Tables{
			{
				Name:                 "books",
				Schema:               "public",
				ReplicaIdentity:      ReplicaIdentityUsingIndex,
				ReplicaIdentityIndex: "books_name_other_idx",
			},
		}

		diff := desired.Diff(current)
		require.Len(t, diff, 1)
		assert.Equal(t, "books_name_other_idx", diff[0].ReplicaIdentityIndex)
	})

	t.Run("should not include table when identity and index are equal", func(t *testing.T) {
		desired := Tables{
			{
				Name:                 "books",
				Schema:               "public",
				ReplicaIdentity:      ReplicaIdentityUsingIndex,
				ReplicaIdentityIndex: "books_name_unique_idx",
			},
		}

		diff := desired.Diff(current)
		require.Empty(t, diff)
	})

	t.Run("should include table when switching from using index to default", func(t *testing.T) {
		desired := Tables{
			{
				Name:            "books",
				Schema:          "public",
				ReplicaIdentity: ReplicaIdentityDefault,
			},
		}

		diff := desired.Diff(current)
		require.Len(t, diff, 1)
		assert.Equal(t, ReplicaIdentityDefault, diff[0].ReplicaIdentity)
		assert.Empty(t, diff[0].ReplicaIdentityIndex)
	})
}
