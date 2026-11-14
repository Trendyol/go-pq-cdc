package replication

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stallTestInterval is the stall warning interval used by these tests. The
// tests wait on the warning itself, not on this duration.
const stallTestInterval = 5 * time.Millisecond

// recordingLogger keeps the sink's own log calls so a test can assert on them.
// logger.InitLogger keeps only the first logger it receives in the test binary,
// so this one is installed from init, before any test runs. Every call is
// forwarded to the logger the other tests in this package already use.
type recordingLogger struct {
	delegate logger.Logger
	mu       sync.Mutex
	calls    []loggedCall
}

type loggedCall struct {
	level string
	msg   string
	args  []any
}

var stallLog = &recordingLogger{delegate: logger.NewSlog(slog.LevelError)}

// Package-level initialization runs before any test.
var _ = installStallLog()

func installStallLog() struct{} {
	logger.InitLogger(stallLog)
	return struct{}{}
}

func (r *recordingLogger) record(level, msg string, args []any) {
	if !strings.HasPrefix(msg, "postgres message sink") {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, loggedCall{level: level, msg: msg, args: args})
}

func (r *recordingLogger) Debug(msg string, args ...any) {
	r.record("debug", msg, args)
	r.delegate.Debug(msg, args...)
}

func (r *recordingLogger) Info(msg string, args ...any) {
	r.record("info", msg, args)
	r.delegate.Info(msg, args...)
}

func (r *recordingLogger) Warn(msg string, args ...any) {
	r.record("warn", msg, args)
	r.delegate.Warn(msg, args...)
}

func (r *recordingLogger) Error(msg string, args ...any) {
	r.record("error", msg, args)
	r.delegate.Error(msg, args...)
}

func (r *recordingLogger) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// find returns the recorded calls with the given message.
func (r *recordingLogger) find(msg string) []loggedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []loggedCall
	for _, c := range r.calls {
		if c.msg == msg {
			out = append(out, c)
		}
	}
	return out
}

// logArg returns the value of a key/value pair in a logger call.
func logArg(t *testing.T, args []any, key string) any {
	t.Helper()
	for i := 0; i+1 < len(args); i += 2 {
		if k, ok := args[i].(string); ok && k == key {
			return args[i+1]
		}
	}
	require.FailNow(t, "log argument missing", key)
	return nil
}

// A send into a full channel with no consumer warns once the stall interval
// passes. Draining one slot lets the send complete, which is then counted once
// and logged as resumed.
func TestMessageBufferSendWarnsWhenStalledThenResumes(t *testing.T) {
	stallLog.reset()
	ch := make(chan *Message, 2)
	ch <- &Message{}
	ch <- &Message{}
	buf := &messageBuffer{
		outCh:             ch,
		ctx:               context.Background(),
		stallWarnInterval: stallTestInterval,
		progress: func() (uint64, pq.LSN) {
			return 7, pq.LSN(42)
		},
	}

	done := make(chan bool, 1)
	go func() { done <- buf.send(&Message{}) }()

	// Wait for two warnings, not one, so a sink that warns only once fails here.
	require.Eventually(t, func() bool {
		return len(stallLog.find(messageSinkStallMsg)) >= 2
	}, 5*time.Second, time.Millisecond, "stall warning was not repeated while the channel stays full")

	warn := stallLog.find(messageSinkStallMsg)[0]
	assert.Equal(t, "warn", warn.level)
	blocked, ok := logArg(t, warn.args, "blocked").(time.Duration)
	require.True(t, ok, "blocked is a duration")
	assert.GreaterOrEqual(t, blocked, stallTestInterval)
	assert.Equal(t, 2, logArg(t, warn.args, "buffered"), "buffered count")
	assert.Equal(t, 2, logArg(t, warn.args, "capacity"), "channel capacity")
	assert.Equal(t, uint64(0), logArg(t, warn.args, "sent"), "sent before this send")
	assert.Equal(t, uint64(7), logArg(t, warn.args, "acked"), "acked from progress")
	assert.Equal(t, pq.LSN(42).String(), logArg(t, warn.args, "confirmedLSN"), "confirmed LSN from progress")

	<-ch // drain one slot, the blocked send can now complete

	require.True(t, <-done, "send returns true once the channel has room")
	assert.Equal(t, uint64(1), buf.sent, "sent is incremented exactly once")
	assert.Len(t, ch, 2, "the drained slot holds the sent message")

	resumed := stallLog.find(messageSinkResumedMsg)
	require.Len(t, resumed, 1, "sink resumed is logged once")
	assert.Equal(t, "info", resumed[0].level)
	_, ok = logArg(t, resumed[0].args, "blocked").(time.Duration)
	assert.True(t, ok, "resume log carries the total blocked duration")
}

// A send cancelled while blocked returns false and is not counted as sent. It
// warned first, which shows it reached the blocked path.
func TestMessageBufferSendReturnsFalseWhenCancelledWhileBlocked(t *testing.T) {
	stallLog.reset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan *Message, 1)
	ch <- &Message{}
	buf := &messageBuffer{outCh: ch, ctx: ctx, stallWarnInterval: stallTestInterval}

	done := make(chan bool, 1)
	go func() { done <- buf.send(&Message{}) }()

	require.Eventually(t, func() bool {
		return len(stallLog.find(messageSinkStallMsg)) > 0
	}, 5*time.Second, time.Millisecond, "send did not reach the blocked path")

	cancel()

	assert.False(t, <-done, "send returns false on cancellation")
	assert.Equal(t, uint64(0), buf.sent, "cancelled send is not counted")
	assert.Len(t, ch, 1, "channel is unchanged")
	assert.Empty(t, stallLog.find(messageSinkResumedMsg), "a cancelled send does not log resumed")
}

// The fast path sends into a channel with room without warning.
func TestMessageBufferSendFastPathDoesNotWarn(t *testing.T) {
	stallLog.reset()
	ch := make(chan *Message, 1)
	buf := &messageBuffer{outCh: ch, stallWarnInterval: stallTestInterval}

	require.True(t, buf.send(&Message{}))
	assert.Equal(t, uint64(1), buf.sent)
	assert.Empty(t, stallLog.find(messageSinkStallMsg))
}

// The fast path into a channel with room must not allocate. The message is
// built outside the measured closure, and each run drains the slot it filled.
func TestMessageBufferSendFastPathAllocatesNothing(t *testing.T) {
	ch := make(chan *Message, 1)
	buf := &messageBuffer{outCh: ch, stallWarnInterval: stallTestInterval}
	msg := &Message{}
	failed := false
	allocs := testing.AllocsPerRun(100, func() {
		if !buf.send(msg) {
			failed = true
		}
		<-ch
	})
	assert.False(t, failed, "every send into a channel with room succeeds")
	assert.Zero(t, allocs, "fast path allocations per send")
}
