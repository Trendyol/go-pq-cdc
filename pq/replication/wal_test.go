package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPgTimeConversions(t *testing.T) {
	ts := time.Date(2026, 4, 24, 12, 30, 15, 123456000, time.UTC)
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	pgTime := timeToPgTime(ts)

	assert.Equal(t, uint64(ts.Sub(pgEpoch)/time.Microsecond), pgTime)
	assert.Equal(t, ts, pgTimeToTime(int64(pgTime)))
}
