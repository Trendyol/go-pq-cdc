package format

import "time"

const microSecFromUnixEpochToY2K = int64(946684800) * 1_000_000

// pgTimeToTime converts microseconds since 2000-01-01 (Y2K) to time.Time (UTC).
func pgTimeToTime(microSecSinceY2K int64) time.Time {
	return time.UnixMicro(microSecFromUnixEpochToY2K + microSecSinceY2K).UTC()
}
