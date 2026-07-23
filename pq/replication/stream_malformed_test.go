package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedConn replays a fixed sequence of backend messages and then fails the
// read, letting a test drive the real sink loop with crafted frames.
type scriptedConn struct {
	frames []pgproto3.BackendMessage
}

func (c *scriptedConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	if len(c.frames) == 0 {
		return nil, errors.New("script exhausted")
	}
	next := c.frames[0]
	c.frames = c.frames[1:]
	return next, nil
}

func (c *scriptedConn) Connect(context.Context) error                          { return nil }
func (c *scriptedConn) IsClosed() bool                                         { return false }
func (c *scriptedConn) Close(context.Context) error                            { return nil }
func (c *scriptedConn) Frontend() *pgproto3.Frontend                           { return nil }
func (c *scriptedConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }

// panicMetric panics when the sink records CDC latency, which happens for every
// XLogData frame before decoding. It gives the test a deterministic panic inside
// the dispatch path so the recover backstop is exercised without depending on
// any particular decoder bug.
type panicMetric struct{ metric.Metric }

func (panicMetric) SetCDCLatency(int64) { panic("injected decode panic") }

func keepaliveFrame(walEnd uint64) *pgproto3.CopyData {
	data := make([]byte, 18)
	data[0] = message.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(data[1:], walEnd)
	// reply requested stays zero so the sink does not try to write
	return &pgproto3.CopyData{Data: data}
}

func xLogFrame(walData ...byte) *pgproto3.CopyData {
	frame := make([]byte, 0, 25+len(walData))
	frame = append(frame, message.XLogDataByteID)
	frame = append(frame, make([]byte, 24)...) // zero WAL header
	frame = append(frame, walData...)
	return &pgproto3.CopyData{Data: frame}
}

func newSinkTestStream(t *testing.T, conn pq.Connection) *stream {
	t.Helper()
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.conn = conn
	// The terminal read error from the exhausted script must read as a normal
	// stop, not a corrupted connection.
	s.closed.Store(true)
	return s
}

// An empty CopyData frame must be skipped without touching Data[0].
func TestDispatchCopyDataSkipsEmptyFrame(t *testing.T) {
	s := newSinkTestStream(t, &scriptedConn{})

	fatal := s.dispatchCopyData(
		context.Background(),
		&pgproto3.CopyData{Data: []byte{}},
		&messageBuffer{outCh: make(chan *Message, 1)},
		&streamTxBuffer{},
	)

	require.False(t, fatal)
}

// A panic raised while dispatching one frame must be swallowed by the recover
// backstop, and the sink loop must go on to process the frames that follow.
// Without the recover in dispatchCopyData this test itself dies with the
// injected panic.
func TestSinkLoopRecoversFromPanicAndKeepsProcessing(t *testing.T) {
	conn := &scriptedConn{frames: []pgproto3.BackendMessage{
		&pgproto3.CopyData{Data: []byte{}}, // empty frame, skipped by the guard
		xLogFrame('B'),                     // reaches the metric, panics, recovered
		keepaliveFrame(42),                 // must still be processed afterwards
	}}
	s := newSinkTestStream(t, conn)
	s.metric = panicMetric{s.metric}

	corrupted := s.sinkLoop(context.Background(), &messageBuffer{outCh: make(chan *Message, 16)}, &streamTxBuffer{})

	require.False(t, corrupted)
	assert.Equal(t, pq.LSN(42), s.LoadXLogPos(), "keepalive after the panic frame was not processed")
}

// Malformed replication frames of the kinds issue 152 describes must be logged
// and skipped by the error paths alone, no recover needed, and must never end
// the sink loop as a corrupted connection.
func TestSinkLoopSkipsMalformedFramesWithoutPanic(t *testing.T) {
	conn := &scriptedConn{frames: []pgproto3.BackendMessage{
		&pgproto3.CopyData{Data: []byte{message.XLogDataByteID, 1, 2, 3}}, // truncated WAL header
		xLogFrame('B', 0, 0, 0),                                          // truncated begin body
		xLogFrame('R', 0, 0, 0, 1, 'a', 0, 'b', 0),                       // relation cut before replica identity
		xLogFrame('U', 0, 0, 64, 6, 'N', 0, 2, 't', 0, 0, 0, 99),         // update column length past the end
		keepaliveFrame(42),                                               // must still be processed afterwards
	}}
	s := newSinkTestStream(t, conn)

	corrupted := s.sinkLoop(context.Background(), &messageBuffer{outCh: make(chan *Message, 16)}, &streamTxBuffer{})

	require.False(t, corrupted)
	assert.Equal(t, pq.LSN(42), s.LoadXLogPos(), "keepalive after the malformed frames was not processed")
}

// The recovered panic log must stay bounded: a small frame is encoded whole, a
// large frame is truncated and annotated with the omitted byte count.
func TestHexPreviewTruncatesLargeFrames(t *testing.T) {
	assert.Equal(t, "010203", hexPreview([]byte{1, 2, 3}))

	preview := hexPreview(make([]byte, maxFramePreview+10))
	assert.Contains(t, preview, "more bytes")
	assert.LessOrEqual(t, len(preview), 2*maxFramePreview+32)
}
