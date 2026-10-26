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
			0, 0, 0, 0, 0, 0, 0, 100, // LSN: the transaction's commit record
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

	// The LSN field is always on the wire; the flag only changes its meaning.
	t.Run("non-transactional message carries lsn, prefix, and content", func(t *testing.T) {
		data := []byte{
			'M',
			0,                       // non-transactional
			0, 0, 0, 0, 0, 0, 0, 42, // LSN: the message's own WAL record
			'n', 'o', 't', 'e', 0,
			0, 0, 0, 1,
			'x',
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.NoError(t, err)
		assert.Equal(t, now, msg.MessageTime)
		assert.Equal(t, uint8(0), msg.Flags)
		assert.Equal(t, pq.LSN(42), msg.LSN)
		assert.Equal(t, "note", msg.Prefix)
		assert.Equal(t, []byte("x"), msg.Content)
		assert.Equal(t, uint32(0), msg.XID)
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
		assert.Equal(t, now, msg.MessageTime)
		assert.Equal(t, uint32(7), msg.XID)
		assert.Equal(t, pq.LSN(256), msg.LSN)
		assert.Equal(t, "p", msg.Prefix)
		assert.Empty(t, msg.Content)
	})

	// The decoder must not retain the caller's buffer: pgx reuses the slice it
	// hands over, so an aliased Content would show the next frame's bytes.
	t.Run("content does not alias the input buffer", func(t *testing.T) {
		data := []byte{
			'M',
			0,
			0, 0, 0, 0, 0, 0, 0, 42, // LSN
			'n', 'o', 't', 'e', 0,
			0, 0, 0, 1,
			'x',
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)
		require.NoError(t, err)

		// The connection refills the same slice for the next wire message.
		data[len(data)-1] = 'Y'

		assert.Equal(t, []byte("x"), msg.Content)
	})

	t.Run("returns an error when the frame ends before the flags", func(t *testing.T) {
		data := []byte{'M'}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.ErrorContains(t, err, "length must be at least")
		assert.Nil(t, msg)
	})

	t.Run("returns an error when the lsn is truncated", func(t *testing.T) {
		data := []byte{'M', 0, 0, 0}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.ErrorContains(t, err, "missing its LSN")
		assert.Nil(t, msg)
	})

	t.Run("returns an error when the prefix is not terminated", func(t *testing.T) {
		data := []byte{
			'M',
			0,
			0, 0, 0, 0, 0, 0, 0, 1, // LSN
			'p', // no NUL terminator
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.ErrorContains(t, err, "not null terminated")
		assert.Nil(t, msg)
	})

	t.Run("returns an error when the content length exceeds the frame", func(t *testing.T) {
		data := []byte{
			'M',
			0,
			0, 0, 0, 0, 0, 0, 0, 1, // LSN
			'p', 0,
			0, 0, 0, 9, // content length 9, but only 2 bytes follow
			'x', 'y',
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.ErrorContains(t, err, "content length out of bounds")
		assert.Nil(t, msg)
	})
}
