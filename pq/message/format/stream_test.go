package format

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStreamCommit(t *testing.T) {
	t.Run("should decode stream commit message successfully", func(t *testing.T) {
		// Given
		// StreamCommit format:
		// Byte1('c') + Int32(Xid) + Int8(Flags) + Int64(CommitLSN) + Int64(TransactionEndLSN) + Int64(CommitTime)
		data := []byte{
			'c',         // Message type 'c' (StreamCommit)
			0, 0, 0, 42, // Xid: 42
			0,                           // Flags: 0
			0, 0, 0, 0, 1, 150, 157, 24, // CommitLSN: 26647832
			0, 0, 0, 0, 1, 150, 157, 72, // TransactionEndLSN: 26647880
			0, 0, 0, 0, 0, 0, 0, 123, // CommitTime: 123
		}

		// When
		sc, err := NewStreamCommit(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, sc)
		assert.Equal(t, uint32(42), sc.Xid)
		assert.Equal(t, uint8(0), sc.Flags)
		assert.Equal(t, pq.LSN(26647832), sc.CommitLSN)
		assert.Equal(t, pq.LSN(26647880), sc.TransactionEndLSN)
		assert.Equal(t, time.Unix(123, 0), sc.CommitTime)
	})

	t.Run("should return error when data is too short", func(t *testing.T) {
		// Given
		data := []byte{
			'c',         // Message type 'c'
			0, 0, 0, 42, // Xid: 42
			0,          // Flags
			0, 0, 0, 0, // Incomplete data
		}

		// When
		sc, err := NewStreamCommit(data)

		// Then
		require.Error(t, err)
		assert.Nil(t, sc)
		assert.Contains(t, err.Error(), "stream commit message length must be at least 30 bytes")
	})

	t.Run("should decode stream commit message with minimum valid length", func(t *testing.T) {
		// Given - exactly 30 bytes
		data := []byte{
			'c',        // Message type 'c'
			0, 0, 0, 1, // Xid: 1
			0,                      // Flags
			0, 0, 0, 0, 0, 0, 0, 1, // CommitLSN: 1
			0, 0, 0, 0, 0, 0, 0, 2, // TransactionEndLSN: 2
			0, 0, 0, 0, 0, 0, 0, 0, // CommitTime: 0
		}

		// When
		sc, err := NewStreamCommit(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, sc)
		assert.Equal(t, uint32(1), sc.Xid)
		assert.Equal(t, uint8(0), sc.Flags)
		assert.Equal(t, pq.LSN(1), sc.CommitLSN)
		assert.Equal(t, pq.LSN(2), sc.TransactionEndLSN)
		assert.Equal(t, time.Unix(0, 0), sc.CommitTime)
	})

	t.Run("should return error for empty data", func(t *testing.T) {
		// Given
		data := []byte{}

		// When
		sc, err := NewStreamCommit(data)

		// Then
		require.Error(t, err)
		assert.Nil(t, sc)
	})

	t.Run("should decode with flags and large xid", func(t *testing.T) {
		// Given
		data := []byte{
			'c',                    // Message type 'c'
			0xFF, 0xFF, 0xFF, 0xFF, // Xid: max uint32
			1,                        // Flags: 1
			0, 0, 0, 0, 0, 0, 0, 100, // CommitLSN: 100
			0, 0, 0, 0, 0, 0, 0, 200, // TransactionEndLSN: 200
			0, 0, 0, 0, 0, 0, 0, 50, // CommitTime: 50
		}

		// When
		sc, err := NewStreamCommit(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, sc)
		assert.Equal(t, uint32(0xFFFFFFFF), sc.Xid)
		assert.Equal(t, uint8(1), sc.Flags)
		assert.Equal(t, pq.LSN(100), sc.CommitLSN)
		assert.Equal(t, pq.LSN(200), sc.TransactionEndLSN)
		assert.Equal(t, time.Unix(50, 0), sc.CommitTime)
	})
}
