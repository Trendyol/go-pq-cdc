package pq

import (
	goerrors "errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsRecoverableConnectionError reports whether err likely indicates a broken
// connection that can be restored by opening a new one.
func IsRecoverableConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if goerrors.Is(err, io.EOF) ||
		goerrors.Is(err, io.ErrUnexpectedEOF) ||
		goerrors.Is(err, net.ErrClosed) ||
		goerrors.Is(err, syscall.ECONNRESET) ||
		goerrors.Is(err, syscall.EPIPE) {
		return true
	}

	var connectErr *pgconn.ConnectError
	if goerrors.As(err, &connectErr) {
		return true
	}

	var pgErr *pgconn.PgError
	if goerrors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P01", "57P02", "57P03", "58030":
			return true
		}
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "conn closed") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "connection lost")
}
