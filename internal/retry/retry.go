package retry

import (
	"github.com/avast/retry-go/v4"
	"time"
)

var DefaultOptions = []retry.Option{
	retry.LastErrorOnly(true),
	retry.Delay(time.Second),
	retry.DelayType(retry.FixedDelay),
}

type Config[T any] struct {
	If      func(err error) bool
	Options []retry.Option
}

func (rc Config[T]) Do(f retry.RetryableFuncWithData[T]) (T, error) {
	return retry.DoWithData(f, rc.Options...)
}

func OnErrorConfig[T any](AttemptCount uint, check func(error) bool) Config[T] {
	cfg := Config[T]{
		If:      check,
		Options: []retry.Option{retry.Attempts(AttemptCount)},
	}
	cfg.Options = append(cfg.Options, DefaultOptions...)
	return cfg
}