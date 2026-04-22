// Package retry provides configurable retry logic for CDC operations.
package retry

import (
	"time"

	"github.com/avast/retry-go/v4"
)

// DefaultOptions defines the default retry behavior with a fixed one-second delay.
var DefaultOptions = []retry.Option{
	retry.LastErrorOnly(true),
	retry.Delay(time.Second),
	retry.DelayType(retry.FixedDelay),
}

// Config holds retry options and an optional error filter for retryable operations.
type Config[T any] struct {
	If      func(err error) bool
	Options []retry.Option
}

// Do executes the given function with the configured retry options.
func (rc Config[T]) Do(f retry.RetryableFuncWithData[T]) (T, error) {
	return retry.DoWithData(f, rc.Options...)
}

// OnErrorConfig creates a retry Config that retries up to attemptCount times when check returns true.
func OnErrorConfig[T any](attemptCount uint, check func(error) bool) Config[T] {
	cfg := Config[T]{
		If:      check,
		Options: []retry.Option{retry.Attempts(attemptCount)},
	}
	cfg.Options = append(cfg.Options, DefaultOptions...)
	return cfg
}
