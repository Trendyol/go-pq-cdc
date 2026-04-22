// Package logger provides a pluggable logging interface for the CDC library.
package logger

import (
	"log/slog"
	"os"
	"sync"
)

var _default Logger

// Logger defines the interface for structured logging at multiple levels.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

var once sync.Once

// InitLogger sets the global logger instance; it can only be called once.
func InitLogger(l Logger) {
	once.Do(func() {
		_default = l
	})
}

// Debug logs a message at debug level using the global logger.
func Debug(msg string, args ...any) {
	_default.Debug(msg, args...)
}

// Info logs a message at info level using the global logger.
func Info(msg string, args ...any) {
	_default.Info(msg, args...)
}

// Warn logs a message at warn level using the global logger.
func Warn(msg string, args ...any) {
	_default.Warn(msg, args...)
}

// Error logs a message at error level using the global logger.
func Error(msg string, args ...any) {
	_default.Error(msg, args...)
}

// NewSlog creates a new Logger backed by slog with JSON output at the given level.
func NewSlog(logLevel slog.Level) Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
}
