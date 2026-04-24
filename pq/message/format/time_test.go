package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPgTimeToTime(t *testing.T) {
	assert.Equal(t, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), pgTimeToTime(0))
	assert.Equal(t, time.Date(2000, 1, 1, 0, 0, 1, 123456000, time.UTC), pgTimeToTime(1_123_456))
}

func pgTimeForTest(microSecSinceY2K int64) time.Time {
	return time.UnixMicro(int64(946684800)*1_000_000 + microSecSinceY2K).UTC()
}
