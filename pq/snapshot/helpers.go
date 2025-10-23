package snapshot

import (
	"context"
	goerrors "errors"
	"net"
	"strings"
	"syscall"
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

	// 1. Check for context errors (deadline exceeded, canceled)
	if goerrors.Is(err, context.DeadlineExceeded) || goerrors.Is(err, context.Canceled) {
		return true
	}

	// 2. Check for pgconn connection errors
	// These occur during initial connection or when connection is lost
	var connectErr *pgconn.ConnectError
	if goerrors.As(err, &connectErr) {
		return true
	}

	// 3. Check for PostgreSQL-specific transient errors
	// Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
	var pgErr *pgconn.PgError
	if goerrors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001": // serialization_failure
			return true
		case "40P01": // deadlock_detected
			return true
		case "55006": // object_in_use
			return true
		case "55P03": // lock_not_available
			return true
		case "57P03": // cannot_connect_now
			return true
		case "58000": // system_error
			return true
		case "58030": // io_error
			return true
		}
	}

	// 4. Check for Go's standard network timeout errors
	var timeoutErr interface{ Timeout() bool }
	if goerrors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}

	// 5. Check for Go's standard temporary network errors
	var tempErr interface{ Temporary() bool }
	if goerrors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}

	// 6. Check for specific syscall errors (low-level network errors)
	var netErr *net.OpError
	if goerrors.As(err, &netErr) {
		// ECONNREFUSED, ECONNRESET, EPIPE are transient
		if goerrors.Is(netErr.Err, syscall.ECONNREFUSED) ||
			goerrors.Is(netErr.Err, syscall.ECONNRESET) ||
			goerrors.Is(netErr.Err, syscall.EPIPE) {
			return true
		}
	}

	// 7. Fallback to string matching for edge cases not covered above
	// This is kept minimal as a safety net
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "connection lost") {
		return true
	}

	return false
}
