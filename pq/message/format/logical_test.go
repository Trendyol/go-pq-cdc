package format

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogicalDecodingMessage(t *testing.T) {
	now := time.Now().UTC()

	t.Run("transactional message carries lsn, prefix, and content", func(t *testing.T) {
		data := []byte{
			'M',
			1,                        // transactional
			0, 0, 0, 0, 0, 0, 0, 100, // LSN
			'd', 's', '-', 'c', 'o', 'm', 'm', 'a', 'n', 'd', 0,
			0, 0, 0, 2,
			'{', '}',
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.NoError(t, err)
		assert.Equal(t, now, msg.MessageTime)
		assert.Equal(t, uint8(1), msg.Flags)
		assert.Equal(t, pq.LSN(100), msg.LSN)
		assert.Equal(t, "ds-command", msg.Prefix)
		assert.Equal(t, []byte("{}"), msg.Content)
		assert.Equal(t, uint32(0), msg.XID)
	})

	t.Run("non-transactional message has no lsn on the wire", func(t *testing.T) {
		data := []byte{
			'M',
			0,
			'n', 'o', 't', 'e', 0,
			0, 0, 0, 1,
			'x',
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.NoError(t, err)
		assert.Equal(t, uint8(0), msg.Flags)
		assert.Equal(t, pq.LSN(0), msg.LSN)
		assert.Equal(t, "note", msg.Prefix)
		assert.Equal(t, []byte("x"), msg.Content)
	})

	t.Run("streamed message is prefixed with the transaction id", func(t *testing.T) {
		data := []byte{
			'M',
			0, 0, 0, 7, // XID
			1,
			0, 0, 0, 0, 0, 0, 1, 0, // LSN 256
			'p', 0,
			0, 0, 0, 0, // empty content
		}

		msg, err := NewLogicalDecodingMessage(data, true, now)

		require.NoError(t, err)
		assert.Equal(t, uint32(7), msg.XID)
		assert.Equal(t, pq.LSN(256), msg.LSN)
		assert.Equal(t, "p", msg.Prefix)
		assert.Empty(t, msg.Content)
	})

	t.Run("returns an error when the prefix is not terminated", func(t *testing.T) {
		data := []byte{'M', 0, 'p'}

		_, err := NewLogicalDecodingMessage(data, false, now)

		require.Error(t, err)
	})
}
