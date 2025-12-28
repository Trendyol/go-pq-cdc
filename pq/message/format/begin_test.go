package format

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
)

func TestNewBegin(t *testing.T) {
	t.Run("should decode begin message successfully", func(t *testing.T) {
		// Given
		data := []byte{
			66,                          // Message type 'B'
			0, 0, 0, 0, 1, 150, 157, 24, // FinalLSN: 0x1969D18 = 26647832
			0, 2, 234, 4, 120, 77, 196, 132, // CommitTime: 0x2EA04784DC484 = 820254872552580 microseconds from 2000-01-01 (as Unix seconds in decode)
			0, 0, 2, 249, // Xid: 0x2F9 = 761
		}

		// When
		begin, err := NewBegin(data)

		// Then
		assert.NoError(t, err)
		assert.NotNil(t, begin)
		assert.Equal(t, pq.LSN(26647832), begin.FinalLSN)
		// The decode function treats the 8 bytes as Unix seconds directly
		assert.Equal(t, time.Unix(820254872552580, 0), begin.CommitTime)
		assert.Equal(t, uint32(761), begin.Xid)
	})

	t.Run("should return error when data is too short", func(t *testing.T) {
		// Given
		data := []byte{
			66,                          // Message type 'B'
			0, 0, 0, 0, 1, 150, 157, 24, // FinalLSN: 26779928
			0, 2, 234, // Incomplete data
		}

		// When
		begin, err := NewBegin(data)

		// Then
		assert.Error(t, err)
		assert.Nil(t, begin)
		assert.Contains(t, err.Error(), "begin message length must be at least 20 byte")
	})

	t.Run("should decode begin message with minimum valid length", func(t *testing.T) {
		// Given
		data := []byte{
			66,                     // Message type 'B'
			0, 0, 0, 0, 0, 0, 0, 1, // FinalLSN: 1
			0, 0, 0, 0, 0, 0, 0, 0, // CommitTime: 0 (Unix epoch)
			0, 0, 0, 1, // Xid: 1
		}

		// When
		begin, err := NewBegin(data)

		// Then
		assert.NoError(t, err)
		assert.NotNil(t, begin)
		assert.Equal(t, pq.LSN(1), begin.FinalLSN)
		assert.Equal(t, time.Unix(0, 0), begin.CommitTime)
		assert.Equal(t, uint32(1), begin.Xid)
	})

	t.Run("should decode begin message with maximum values", func(t *testing.T) {
		// Given
		data := []byte{
			66,                                             // Message type 'B'
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // FinalLSN: max uint64
			0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // CommitTime: max int64
			0xFF, 0xFF, 0xFF, 0xFF, // Xid: max uint32
		}

		// When
		begin, err := NewBegin(data)

		// Then
		assert.NoError(t, err)
		assert.NotNil(t, begin)
		assert.Equal(t, pq.LSN(0xFFFFFFFFFFFFFFFF), begin.FinalLSN)
		assert.Equal(t, uint32(0xFFFFFFFF), begin.Xid)
	})
}

func TestBegin_decode(t *testing.T) {
	t.Run("should decode valid begin data", func(t *testing.T) {
		// Given
		data := []byte{
			66,                          // Message type 'B'
			0, 0, 0, 0, 1, 150, 157, 24, // FinalLSN: 26647832
			0, 2, 234, 4, 120, 77, 196, 132, // CommitTime: 820254872552580 as Unix seconds
			0, 0, 2, 249, // Xid: 761
		}
		begin := &Begin{}

		// When
		err := begin.decode(data)

		// Then
		assert.NoError(t, err)
		assert.Equal(t, pq.LSN(26647832), begin.FinalLSN)
		assert.Equal(t, time.Unix(820254872552580, 0), begin.CommitTime)
		assert.Equal(t, uint32(761), begin.Xid)
	})

	t.Run("should return error for empty data", func(t *testing.T) {
		// Given
		data := []byte{}
		begin := &Begin{}

		// When
		err := begin.decode(data)

		// Then
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "begin message length must be at least 20 byte")
	})

	t.Run("should return error when data length is exactly 19 bytes", func(t *testing.T) {
		// Given
		data := make([]byte, 19)
		begin := &Begin{}

		// When
		err := begin.decode(data)

		// Then
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "begin message length must be at least 20 byte, but got 19")
	})

	t.Run("should decode when data length is exactly 21 bytes", func(t *testing.T) {
		// Given - need 21 bytes: 1 (type) + 8 (LSN) + 8 (time) + 4 (Xid) = 21
		data := make([]byte, 21)
		data[0] = 66 // Message type
		begin := &Begin{}

		// When
		err := begin.decode(data)

		// Then
		assert.NoError(t, err)
		assert.Equal(t, pq.LSN(0), begin.FinalLSN)
		assert.Equal(t, time.Unix(0, 0), begin.CommitTime)
		assert.Equal(t, uint32(0), begin.Xid)
	})
}
