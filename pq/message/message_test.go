package message

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDecodesTruncate(t *testing.T) {
	now := time.Now()
	relations := map[uint32]*format.Relation{
		42: {OID: 42, Namespace: "public", Name: "books"},
	}
	data := []byte{
		'T',
		0, 0, 0, 1, // relation count
		2, // restart identity
		0, 0, 0, 42,
	}

	msg, err := New(data, false, now, relations)

	require.NoError(t, err)
	truncate, ok := msg.(*format.Truncate)
	require.True(t, ok)
	assert.Equal(t, now, truncate.MessageTime)
	assert.Equal(t, []uint32{42}, truncate.RelationOIDs)
	assert.Equal(t, []*format.Relation{relations[42]}, truncate.Relations)
	assert.False(t, truncate.Cascade)
	assert.True(t, truncate.RestartIdentity)
}

// A Relation message inside a streamed transaction chunk carries a 4-byte XID
// after the type byte. It must be decoded with the caller-provided flag — with
// the wrong flag the offsets misalign and the decode reads past the buffer.
func TestNewDecodesStreamedRelation(t *testing.T) {
	relations := map[uint32]*format.Relation{}
	data := []byte{
		'R',
		0, 0, 4, 210, // XID 1234
		0, 0, 64, 6, // OID 16390
		'p', 'u', 'b', 'l', 'i', 'c', 0,
		't', 0,
		'd', // replica identity
		0, 1, // column count
		1, 'i', 'd', 0, 0, 0, 0, 23, 255, 255, 255, 255,
	}

	msg, err := New(data, true, time.Now(), relations)

	require.NoError(t, err)
	rel, ok := msg.(*format.Relation)
	require.True(t, ok)
	assert.Equal(t, uint32(1234), rel.XID)
	assert.Equal(t, uint32(16390), rel.OID)
	assert.Equal(t, "public", rel.Namespace)
	assert.Equal(t, "t", rel.Name)
	require.Len(t, rel.Columns, 1)
	assert.Equal(t, "id", rel.Columns[0].Name)
	assert.Equal(t, rel, relations[16390])
}
