package format

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Trendyol/go-pq-cdc/pq"
)

func TestNewPrimaryKeepaliveMessage(t *testing.T) {
	t.Run("should decode keepalive message successfully", func(t *testing.T) {
		// Given
		walEnd := uint64(65538)
		microSinceY2K := int64(1_000_000) // 1 second after Y2K epoch
		replyRequested := byte(1)

		data := make([]byte, 17)
		binary.BigEndian.PutUint64(data[0:], walEnd)
		binary.BigEndian.PutUint64(data[8:], uint64(microSinceY2K))
		data[16] = replyRequested

		// When
		msg, err := NewPrimaryKeepaliveMessage(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, msg)
		assert.Equal(t, pq.LSN(walEnd), msg.ServerWALEnd)
		expectedTime := time.Unix(946684800+(microSinceY2K/1_000_000), (microSinceY2K%1_000_000)*1_000).UTC()
		assert.Equal(t, expectedTime, msg.ServerTime)
		assert.True(t, msg.ReplyRequested)
	})

	t.Run("should return error when data length is not 17 bytes", func(t *testing.T) {
		// Given
		data := make([]byte, 10) // invalid length

		// When
		msg, err := NewPrimaryKeepaliveMessage(data)

		// Then
		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "primary keepalive message length must be 17 byte")
	})
}
