package format

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStreamStart(t *testing.T) {
	t.Run("should decode stream start message successfully", func(t *testing.T) {
		// Given
		// StreamStart format:
		// Byte1('S') + Int32(Xid) + Int8(FirstSegment)
		data := []byte{
			'S',         // Message type 'S' (StreamStart)
			0, 0, 0, 42, // Xid: 42
			1, // FirstSegment: true
		}

		// When
		ss, err := NewStreamStart(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, ss)
		assert.Equal(t, uint32(42), ss.Xid)
		assert.True(t, ss.FirstSegment)
	})

	t.Run("should decode stream start with first_segment false", func(t *testing.T) {
		// Given
		data := []byte{
			'S',         // Message type 'S'
			0, 0, 0, 10, // Xid: 10
			0, // FirstSegment: false
		}

		// When
		ss, err := NewStreamStart(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, ss)
		assert.Equal(t, uint32(10), ss.Xid)
		assert.False(t, ss.FirstSegment)
	})

	t.Run("should return error when data is too short", func(t *testing.T) {
		// Given
		data := []byte{
			'S',     // Message type 'S'
			0, 0, 0, // Incomplete data
		}

		// When
		ss, err := NewStreamStart(data)

		// Then
		require.Error(t, err)
		assert.Nil(t, ss)
		assert.Contains(t, err.Error(), "stream start message length must be at least 6 bytes")
	})

	t.Run("should return error for empty data", func(t *testing.T) {
		// Given
		data := []byte{}

		// When
		ss, err := NewStreamStart(data)

		// Then
		require.Error(t, err)
		assert.Nil(t, ss)
	})

	t.Run("should decode with large xid", func(t *testing.T) {
		// Given
		data := []byte{
			'S',                    // Message type 'S'
			0xFF, 0xFF, 0xFF, 0xFF, // Xid: max uint32
			1, // FirstSegment: true
		}

		// When
		ss, err := NewStreamStart(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, ss)
		assert.Equal(t, uint32(0xFFFFFFFF), ss.Xid)
		assert.True(t, ss.FirstSegment)
	})

	t.Run("should decode stream start message with minimum valid length", func(t *testing.T) {
		// Given - exactly 6 bytes
		data := []byte{
			'S',        // Message type 'S'
			0, 0, 0, 1, // Xid: 1
			0, // FirstSegment: false
		}

		// When
		ss, err := NewStreamStart(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, ss)
		assert.Equal(t, uint32(1), ss.Xid)
		assert.False(t, ss.FirstSegment)
	})
}

func TestNewStreamAbort(t *testing.T) {
	t.Run("should decode stream abort message successfully", func(t *testing.T) {
		// Given
		// StreamAbort format:
		// Byte1('A') + Int32(Xid) + Int32(SubXid)
		data := []byte{
			'A',         // Message type 'A' (StreamAbort)
			0, 0, 0, 42, // Xid: 42
			0, 0, 0, 7, // SubXid: 7
		}

		// When
		sa, err := NewStreamAbort(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, sa)
		assert.Equal(t, uint32(42), sa.Xid)
		assert.Equal(t, uint32(7), sa.SubXid)
	})

	t.Run("should return error when data is too short", func(t *testing.T) {
		// Given
		data := []byte{
			'A',         // Message type 'A'
			0, 0, 0, 42, // Xid: 42
			0, 0, // Incomplete SubXid
		}

		// When
		sa, err := NewStreamAbort(data)

		// Then
		require.Error(t, err)
		assert.Nil(t, sa)
		assert.Contains(t, err.Error(), "stream abort message length must be at least 9 bytes")
	})

	t.Run("should return error for empty data", func(t *testing.T) {
		// Given
		data := []byte{}

		// When
		sa, err := NewStreamAbort(data)

		// Then
		require.Error(t, err)
		assert.Nil(t, sa)
	})

	t.Run("should decode with large xid and subxid", func(t *testing.T) {
		// Given
		data := []byte{
			'A',                    // Message type 'A'
			0xFF, 0xFF, 0xFF, 0xFF, // Xid: max uint32
			0xFF, 0xFF, 0xFF, 0xFE, // SubXid: max uint32 - 1
		}

		// When
		sa, err := NewStreamAbort(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, sa)
		assert.Equal(t, uint32(0xFFFFFFFF), sa.Xid)
		assert.Equal(t, uint32(0xFFFFFFFE), sa.SubXid)
	})

	t.Run("should decode stream abort message with minimum valid length", func(t *testing.T) {
		// Given - exactly 9 bytes
		data := []byte{
			'A',        // Message type 'A'
			0, 0, 0, 1, // Xid: 1
			0, 0, 0, 2, // SubXid: 2
		}

		// When
		sa, err := NewStreamAbort(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, sa)
		assert.Equal(t, uint32(1), sa.Xid)
		assert.Equal(t, uint32(2), sa.SubXid)
	})
}

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
