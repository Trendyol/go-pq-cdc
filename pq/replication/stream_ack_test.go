package replication

import (
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
)

func TestHandleXLogDataUpdatesReceivedPosition(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	walEnd := pq.LSN(0x16B6D90)
	data := make([]byte, 25)
	binary.BigEndian.PutUint64(data[0:], uint64(0x16B6D80)) // WAL start
	binary.BigEndian.PutUint64(data[8:], uint64(walEnd))    // server WAL end
	binary.BigEndian.PutUint64(data[16:], 0)                // server time
	data[24] = 'Z'                                          // unsupported logical message, enough to decode XLogData

	stream.handleXLogData(data, &messageBuffer{outCh: make(chan *Message, 1)}, &streamTxBuffer{})

	assert.Equal(t, walEnd, stream.LoadXLogPos())
}
