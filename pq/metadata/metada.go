package metadata

import (
	"context"
	"github.com/Trendyol/go-pq-cdc/pq"
)

type MetaData struct {
	conn pq.PoolConnection
}

func NewMetaData(conn pq.PoolConnection) *MetaData {
	return &MetaData{
		conn: conn,
	}
}

func (m *MetaData) ExportSnapshot(ctx context.Context) (string, error) {
	return "", nil
}

func (m *MetaData) TableColumns(ctx context.Context, tables []string) (map[string][]string, error) {
	_ = `
SELECT
    column_name,
table_name,
table_schema
FROM
    information_schema.columns
WHERE
    table_schema = 'public'  -- Specify the schema (usually 'public')
    AND table_name = 'meal_orders_metric'   -- Specify your table name
ORDER BY
    ordinal_position;
`
	return nil, nil
}

func (m *MetaData) TableCounts(ctx context.Context, tables []string) (map[string]int, error) {
	return nil, nil
}
