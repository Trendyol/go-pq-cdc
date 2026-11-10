package online

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// serverVersion selects compatible catalog fields, not a supported-major policy.
func serverVersion(ctx context.Context, conn *pgx.Conn) (int, error) {
	var version int
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}
