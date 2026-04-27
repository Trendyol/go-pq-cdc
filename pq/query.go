package pq

import (
	"context"
	"fmt"
)

// ExecSQL executes a SQL statement without returning results.
func ExecSQL(ctx context.Context, conn Connection, sql string) error {
	resultReader := conn.Exec(ctx, sql)
	_, err := resultReader.ReadAll()
	if err != nil {
		return err
	}
	return resultReader.Close()
}

// ExecExistsQuery executes a SELECT EXISTS query and returns the boolean result.
func ExecExistsQuery(ctx context.Context, conn Connection, query string) (bool, error) {
	resultReader := conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return false, err
	}
	if err := resultReader.Close(); err != nil {
		return false, err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, fmt.Errorf("no result returned from existence check")
	}

	return string(results[0].Rows[0][0]) == "t", nil
}

// TableExists checks if a table exists in the given schema using information_schema.
func TableExists(ctx context.Context, conn Connection, schema, table string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = '%s'
			AND table_name = '%s'
		)`, schema, table)

	return ExecExistsQuery(ctx, conn, query)
}

// IndexExists checks if an index exists in the given schema using pg_indexes.
func IndexExists(ctx context.Context, conn Connection, schema, index string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_indexes
			WHERE schemaname = '%s'
			AND indexname = '%s'
		)`, schema, index)

	return ExecExistsQuery(ctx, conn, query)
}
