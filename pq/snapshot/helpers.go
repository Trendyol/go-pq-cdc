package snapshot

import (
	"context"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
)

// execSQL executes a SQL statement without returning results (DDL, DML)
func (s *Snapshotter) execSQL(ctx context.Context, conn pq.Connection, sql string) error {
	resultReader := conn.Exec(ctx, sql)
	_, err := resultReader.ReadAll()
	if err != nil {
		return err
	}
	return resultReader.Close()
}

// execQuery executes a SQL query and returns results
// Query should be pre-formatted (use fmt.Sprintf before calling)
func (s *Snapshotter) execQuery(ctx context.Context, conn pq.Connection, query string) ([]*pgconn.Result, error) {
	resultReader := conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, err
	}
	if err = resultReader.Close(); err != nil {
		return nil, err
	}
	return results, nil
}

// retryDBOperation retries a database operation on transient errors
func (s *Snapshotter) retryDBOperation(ctx context.Context, operation func() error) error {
	maxRetries := 3
	retryDelay := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := retryDelay * time.Duration(1<<uint(attempt-1))
			logger.Debug("[retry] database operation", "attempt", attempt, "delay", delay)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err := operation()
		if err == nil {
			return nil
		}

		// Check if error is transient (network, connection, timeout)
		if isTransientError(err) {
			lastErr = err
			continue
		}

		// Non-transient error, fail immediately
		return err
	}

	return errors.Wrap(lastErr, "database operation failed after retries")
}

// isTransientError checks if an error is transient and should be retried
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	transientPatterns := []string{
		"connection reset",
		"connection refused",
		"i/o timeout",
		"deadline exceeded",
		"broken pipe",
		"connection closed",
		"connection lost",
		"temporary failure",
	}

	for _, pattern := range transientPatterns {
		if strings.Contains(strings.ToLower(errStr), pattern) {
			return true
		}
	}

	return false
}
