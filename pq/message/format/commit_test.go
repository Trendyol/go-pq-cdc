package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
)

func TestNewCommit(t *testing.T) {
	t.Run("should decode commit message successfully", func(t *testing.T) {
		// Given
		data := []byte{
			67,                          // Message type 'C'
			0,                           // Flags
			0, 0, 0, 0, 1, 150, 157, 24, // CommitLSN: 0x1969D18 = 26647832
			0, 0, 0, 0, 1, 150, 157, 72, // TransactionEndLSN: 0x1969D48 = 26647880
			0, 2, 234, 4, 120, 77, 196, 132, // CommitTime: 0x2EA04784DC484 = 820254872552580 as Unix seconds
		}

		// When
		commit, err := NewCommit(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, commit)
		assert.Equal(t, uint8(0), commit.Flags)
		assert.Equal(t, pq.LSN(26647832), commit.CommitLSN)
		assert.Equal(t, pq.LSN(26647880), commit.TransactionEndLSN)
		assert.Equal(t, time.Unix(820254872552580, 0), commit.CommitTime)
	})

	t.Run("should return error when data is too short", func(t *testing.T) {
		// Given
		data := []byte{
			67,                          // Message type 'C'
			0,                           // Flags
			0, 0, 0, 0, 1, 150, 157, 24, // CommitLSN: 26779928
			0, 0, 0, 0, // Incomplete data
		}

		// When
		commit, err := NewCommit(data)

		// Then
		assert.Error(t, err)
		assert.Nil(t, commit)
		assert.Contains(t, err.Error(), "commit message length must be at least 25 byte")
	})

	t.Run("should decode commit message with minimum valid length", func(t *testing.T) {
		// Given
		data := []byte{
			67,                     // Message type 'C'
			0,                      // Flags
			0, 0, 0, 0, 0, 0, 0, 1, // CommitLSN: 1
			0, 0, 0, 0, 0, 0, 0, 2, // TransactionEndLSN: 2
			0, 0, 0, 0, 0, 0, 0, 0, // CommitTime: 0 (Unix epoch)
		}

		// When
		commit, err := NewCommit(data)

		// Then
		require.NoError(t, err)
		assert.NotNil(t, commit)
		assert.Equal(t, uint8(0), commit.Flags)
		assert.Equal(t, pq.LSN(1), commit.CommitLSN)
		assert.Equal(t, pq.LSN(2), commit.TransactionEndLSN)
		assert.Equal(t, time.Unix(0, 0), commit.CommitTime)
	})

	t.Run("should decode commit message with flags set", func(t *testing.T) {
		// Given
		data := []byte{
			67,                       // Message type 'C'
			1,                        // Flags: 1
			0, 0, 0, 0, 0, 0, 0, 100, // CommitLSN: 100
			0, 0, 0, 0, 0, 0, 0, 200, // TransactionEndLSN: 200
			0, 0, 0, 0, 0, 0, 0, 123, // CommitTime: 123
		}

		// When
		commit, err := NewCommit(data)

		// Then
		assert.NoError(t, err)
		assert.NotNil(t, commit)
		assert.Equal(t, uint8(1), commit.Flags)
		assert.Equal(t, pq.LSN(100), commit.CommitLSN)
		assert.Equal(t, pq.LSN(200), commit.TransactionEndLSN)
		assert.Equal(t, time.Unix(123, 0), commit.CommitTime)
	})

	t.Run("should decode commit message with maximum values", func(t *testing.T) {
		// Given
		data := []byte{
			67,                                             // Message type 'C'
			0xFF,                                           // Flags: max uint8
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // CommitLSN: max uint64
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, // TransactionEndLSN: max uint64 - 1
			0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // CommitTime: max int64
		}

		// When
		commit, err := NewCommit(data)

		// Then
		assert.NoError(t, err)
		assert.NotNil(t, commit)
		assert.Equal(t, uint8(0xFF), commit.Flags)
		assert.Equal(t, pq.LSN(0xFFFFFFFFFFFFFFFF), commit.CommitLSN)
		assert.Equal(t, pq.LSN(0xFFFFFFFFFFFFFFFE), commit.TransactionEndLSN)
	})
}

func TestCommit_decode(t *testing.T) {
	t.Run("should decode valid commit data", func(t *testing.T) {
		// Given
		data := []byte{
			67,                          // Message type 'C'
			0,                           // Flags
			0, 0, 0, 0, 1, 150, 157, 24, // CommitLSN: 26647832
			0, 0, 0, 0, 1, 150, 157, 72, // TransactionEndLSN: 26647880
			0, 2, 234, 4, 120, 77, 196, 132, // CommitTime: 820254872552580 as Unix seconds
		}
		commit := &Commit{}

		// When
		err := commit.decode(data)

		// Then
		assert.NoError(t, err)
		assert.Equal(t, uint8(0), commit.Flags)
		assert.Equal(t, pq.LSN(26647832), commit.CommitLSN)
		assert.Equal(t, pq.LSN(26647880), commit.TransactionEndLSN)
		assert.Equal(t, time.Unix(820254872552580, 0), commit.CommitTime)
	})

	t.Run("should return error for empty data", func(t *testing.T) {
		// Given
		data := []byte{}
		commit := &Commit{}

		// When
		err := commit.decode(data)

		// Then
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit message length must be at least 25 byte")
	})

	t.Run("should return error when data length is exactly 24 bytes", func(t *testing.T) {
		// Given
		data := make([]byte, 24)
		commit := &Commit{}

		// When
		err := commit.decode(data)

		// Then
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "commit message length must be at least 25 byte, but got 24")
	})

	t.Run("should decode when data length is exactly 26 bytes", func(t *testing.T) {
		// Given - need 26 bytes: 1 (type) + 1 (flags) + 8 (CommitLSN) + 8 (TransactionEndLSN) + 8 (CommitTime) = 26
		data := make([]byte, 26)
		data[0] = 67 // Message type
		commit := &Commit{}

		// When
		err := commit.decode(data)

		// Then
		assert.NoError(t, err)
		assert.Equal(t, uint8(0), commit.Flags)
		assert.Equal(t, pq.LSN(0), commit.CommitLSN)
		assert.Equal(t, pq.LSN(0), commit.TransactionEndLSN)
		assert.Equal(t, time.Unix(0, 0), commit.CommitTime)
	})

	t.Run("should decode with non-zero flags", func(t *testing.T) {
		// Given
		data := []byte{
			67,                     // Message type 'C'
			255,                    // Flags: 255
			0, 0, 0, 0, 0, 0, 1, 0, // CommitLSN: 256
			0, 0, 0, 0, 0, 0, 2, 0, // TransactionEndLSN: 512
			0, 0, 0, 0, 100, 0, 0, 0, // CommitTime: 1677721600
		}
		commit := &Commit{}

		// When
		err := commit.decode(data)

		// Then
		assert.NoError(t, err)
		assert.Equal(t, uint8(255), commit.Flags)
		assert.Equal(t, pq.LSN(256), commit.CommitLSN)
		assert.Equal(t, pq.LSN(512), commit.TransactionEndLSN)
		assert.Equal(t, time.Unix(1677721600, 0), commit.CommitTime)
	})
}
