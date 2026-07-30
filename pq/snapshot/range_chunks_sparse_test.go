package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntegerRangeChunkCount_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		minValue  int64
		maxValue  int64
		chunkSize int64
		want      int64
	}{
		{
			name:      "min equals max → 1 chunk",
			minValue:  42,
			maxValue:  42,
			chunkSize: 1000,
			want:      1,
		},
		{
			name:      "chunkSize greater than totalRange → 1 chunk",
			minValue:  1,
			maxValue:  128,
			chunkSize: 1000,
			want:      1,
		},
		{
			name:      "exact multiple of chunkSize",
			minValue:  1,
			maxValue:  2000,
			chunkSize: 1000,
			want:      2, // (2000-1+1)/1000 = 2
		},
		{
			name:      "dense 128 rows in 1..128",
			minValue:  1,
			maxValue:  128,
			chunkSize: 1000,
			want:      1,
		},
		{
			name:      "sparse 2 rows spanning 1..1_000_000",
			minValue:  1,
			maxValue:  1_000_000,
			chunkSize: 1000,
			want:      1000,
		},
		{
			name:      "contentblacklist-scale sparse 1..10M",
			minValue:  1,
			maxValue:  10_000_000,
			chunkSize: 1000,
			want:      10_000,
		},
		{
			name:      "integration sparse fixture 1..20000 chunkSize 1000",
			minValue:  1,
			maxValue:  20_000,
			chunkSize: 1000,
			want:      20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := integerRangeChunkCount(tt.minValue, tt.maxValue, tt.chunkSize)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Same conceptual row count, different PK span → chunk counts diverge hard.
func TestIntegerRangeChunkCount_SameRowCountDifferentSpan(t *testing.T) {
	const (
		rowCount  int64 = 128
		chunkSize int64 = 1000
	)

	denseChunks := integerRangeChunkCount(1, 128, chunkSize)         // sequential IDs
	sparseChunks := integerRangeChunkCount(1, 10_000_000, chunkSize) // sparse IDs

	assert.Equal(t, int64(1), denseChunks)
	assert.Equal(t, int64(10_000), sparseChunks)
	assert.Greater(t, sparseChunks, denseChunks*100)
	assert.Greater(t, sparseChunks, rowCount*10)
}

func TestIntegerRangeIsSparse(t *testing.T) {
	const chunkSize int64 = 1000

	t.Run("dense sequential keeps integer_range", func(t *testing.T) {
		numChunks := integerRangeChunkCount(1, 20, chunkSize)
		assert.False(t, integerRangeIsSparse(numChunks, 20, chunkSize))
	})

	t.Run("integration sparse fixture falls back", func(t *testing.T) {
		numChunks := integerRangeChunkCount(1, 20_000, chunkSize) // 20
		assert.True(t, integerRangeIsSparse(numChunks, 3, chunkSize))
	})

	t.Run("mild gaps under 2x ideal keep integer_range", func(t *testing.T) {
		// 9000 rows in 1..10000 → 10 chunks; ideal=9; threshold=18
		numChunks := integerRangeChunkCount(1, 10_000, chunkSize)
		assert.False(t, integerRangeIsSparse(numChunks, 9000, chunkSize))
	})

	t.Run("zero rowCount never sparse", func(t *testing.T) {
		assert.False(t, integerRangeIsSparse(20, 0, chunkSize))
	})
}
