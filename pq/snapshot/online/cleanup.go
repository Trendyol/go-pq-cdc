package online

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"time"
)

// RollbackBounded never lets a lost rollback response hold an owner/delivery
// indefinitely. A failed cleanup discards its non-reconnecting connection.
func RollbackBounded(tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	if err != nil {
		closeCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		return errors.Join(err, tx.Conn().Close(closeCtx))
	}
	return nil
}
