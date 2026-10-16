package pq

import (
	"fmt"
	"io"
	"math"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// OnlineBodyLimit includes a bounded 64KiB allowance for PostgreSQL row/relation
// framing, errors and startup metadata. This is a wire allocation limit, not RSS.
func OnlineBodyLimit(payload int64) (int, error) {
	const allowance = int64(64 << 10)
	if payload <= 0 || payload > int64(math.MaxInt)-allowance {
		return 0, fmt.Errorf("online wire budget overflows body limit")
	}
	return int(payload + allowance), nil
}
func LimitOnlineFrontend(config *pgconn.Config, payload int64) error {
	limit, err := OnlineBodyLimit(payload)
	if err != nil {
		return err
	}
	config.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		f := pgproto3.NewFrontend(r, w)
		f.SetMaxBodyLen(limit)
		return f
	}
	return nil
}
