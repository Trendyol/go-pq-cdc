package replication

import (
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/Trendyol/go-pq-cdc/logger"
)

// logs records what the package logs at Info and above, so a test can assert
// the level of an operator-facing line. Output still goes to stdout at Error
// level, as every test here configured it before.
var logs = &captureLogger{Logger: logger.NewSlog(slog.LevelError)}

func TestMain(m *testing.M) {
	logger.InitLogger(logs)
	os.Exit(m.Run())
}

type logRecord struct {
	msg   string
	args  []any
	level slog.Level
}

type captureLogger struct {
	logger.Logger
	records []logRecord
	mu      sync.Mutex
}

func (l *captureLogger) record(level slog.Level, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, logRecord{level: level, msg: msg, args: args})
}

func (l *captureLogger) Info(msg string, args ...any) {
	l.record(slog.LevelInfo, msg, args)
	l.Logger.Info(msg, args...)
}

func (l *captureLogger) Warn(msg string, args ...any) {
	l.record(slog.LevelWarn, msg, args)
	l.Logger.Warn(msg, args...)
}

func (l *captureLogger) Error(msg string, args ...any) {
	l.record(slog.LevelError, msg, args)
	l.Logger.Error(msg, args...)
}

func (l *captureLogger) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = nil
}

// find returns the first record with msg, whatever its level.
func (l *captureLogger) find(msg string) (logRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.records {
		if r.msg == msg {
			return r, true
		}
	}
	return logRecord{}, false
}
