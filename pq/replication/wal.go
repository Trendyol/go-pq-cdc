package replication

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"

	"github.com/vskurikhin/go-pq-cdc/pq"
)

// The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
// microSecFromUnixEpochToY2K is unix timestamp of 2000-01-01.
var microSecFromUnixEpochToY2K = int64(946684800)

type XLogData struct {
	ServerTime   time.Time
	WALData      []byte
	WALStart     pq.LSN
	ServerWALEnd pq.LSN
}

func ParseXLogData(buf []byte) (XLogData, error) {
	var xld XLogData
	if len(buf) < 24 {
		return xld, errors.Newf("XLogData must be at least 24 bytes, got %d", len(buf))
	}

	xld.WALStart = pq.LSN(binary.BigEndian.Uint64(buf))
	xld.ServerWALEnd = pq.LSN(binary.BigEndian.Uint64(buf[8:]))
	xld.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(buf[16:])))
	xld.WALData = buf[24:]

	return xld, nil
}

func pgTimeToTime(microSecSinceY2K int64) time.Time {
	return time.Unix(microSecFromUnixEpochToY2K+(microSecSinceY2K/1_000_000), (microSecSinceY2K%1_000_000)*1_000).UTC()
}
