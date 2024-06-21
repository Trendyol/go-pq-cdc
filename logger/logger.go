package logger

import (
	"log/slog"
	"os"
	"sync"
)

var _default Logger

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

var once sync.Once

func InitLogger(l Logger) {
	once.Do(func() {
		_default = l
	})
}

func Debug(msg string, args ...any) {
	_default.Debug(msg, args...)
}

func Info(msg string, args ...any) {
	_default.Info(msg, args...)
}

func Warn(msg string, args ...any) {
	_default.Warn(msg, args...)
}

func Error(msg string, args ...any) {
	_default.Error(msg, args...)
}

func NewSlog(logLevel slog.Level) Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
}
