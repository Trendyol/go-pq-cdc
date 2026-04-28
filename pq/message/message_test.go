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

	msg, err := New(data, now, relations)

	require.NoError(t, err)
	truncate, ok := msg.(*format.Truncate)
	require.True(t, ok)
	assert.Equal(t, now, truncate.MessageTime)
	assert.Equal(t, []uint32{42}, truncate.RelationOIDs)
	assert.Equal(t, []*format.Relation{relations[42]}, truncate.Relations)
	assert.False(t, truncate.Cascade)
	assert.True(t, truncate.RestartIdentity)
}
