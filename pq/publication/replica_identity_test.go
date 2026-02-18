package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestQuoteLiteral(t *testing.T) {
	assert.Equal(t, "'simple'", quoteLiteral("simple"))
	assert.Equal(t, "'o''reilly'", quoteLiteral("o'reilly"))
}
