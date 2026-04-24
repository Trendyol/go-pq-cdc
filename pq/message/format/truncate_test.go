package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTruncate_New(t *testing.T) {
	now := time.Now()
	relations := map[uint32]*Relation{
		42: {OID: 42, Namespace: "public", Name: "authors"},
		43: {OID: 43, Namespace: "public", Name: "books"},
	}

	t.Run("decodes truncate message", func(t *testing.T) {
		data := []byte{
			'T',
			0, 0, 0, 2, // relation count
			3, // cascade + restart identity
			0, 0, 0, 42,
			0, 0, 0, 43,
		}

		msg, err := NewTruncate(data, false, relations, now)

		require.NoError(t, err)
		assert.Equal(t, now, msg.MessageTime)
		assert.Equal(t, []uint32{42, 43}, msg.RelationOIDs)
		assert.Equal(t, []*Relation{relations[42], relations[43]}, msg.Relations)
		assert.Equal(t, uint8(3), msg.Options)
		assert.True(t, msg.Cascade)
		assert.True(t, msg.RestartIdentity)
		assert.Zero(t, msg.XID)
	})

	t.Run("decodes streamed truncate message", func(t *testing.T) {
		data := []byte{
			'T',
			0, 0, 0, 99, // xid
			0, 0, 0, 1, // relation count
			1, // cascade
			0, 0, 0, 42,
		}

		msg, err := NewTruncate(data, true, relations, now)

		require.NoError(t, err)
		assert.Equal(t, uint32(99), msg.XID)
		assert.Equal(t, []uint32{42}, msg.RelationOIDs)
		assert.Equal(t, []*Relation{relations[42]}, msg.Relations)
		assert.True(t, msg.Cascade)
		assert.False(t, msg.RestartIdentity)
	})

	t.Run("returns error when relation is missing", func(t *testing.T) {
		data := []byte{
			'T',
			0, 0, 0, 1, // relation count
			0, // options
			0, 0, 0, 44,
		}

		msg, err := NewTruncate(data, false, relations, now)

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "relation not found")
	})

	t.Run("returns error when data is too short", func(t *testing.T) {
		msg, err := NewTruncate([]byte{'T', 0, 0}, false, relations, now)

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "truncate message length")
	})

	t.Run("returns error when relation IDs are incomplete", func(t *testing.T) {
		data := []byte{
			'T',
			0, 0, 0, 2, // relation count
			0, // options
			0, 0, 0, 42,
		}

		msg, err := NewTruncate(data, false, relations, now)

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "truncate message relation IDs length")
	})
}
