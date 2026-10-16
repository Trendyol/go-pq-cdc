package online

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/jackc/pgx/v5"
)

var ErrRead = errors.New("online bounded read failed")

func CaptureUpper(ctx context.Context, conn *pgx.Conn, s TableSchema) (*int64, error) {
	var upper *int64
	err := conn.QueryRow(ctx, "SELECT max("+pgx.Identifier{s.Key}.Sanitize()+")::bigint FROM "+quoteTable(s.Table)).Scan(&upper)
	return upper, err
}

// ReadPage takes one fresh primary READ COMMITTED statement snapshot. Raw text
// bytes bound the retained chunk; Go object overhead is not this budget. Use
// pgtype text decoding, as pgoutput does, to keep replay content canonical.
func ReadPage(ctx context.Context, conn *pgx.Conn, s TableSchema, cursor, upper *int64, limit int, bytes int64, timeout time.Duration, counters ...*Counters) ([]SelectedRow, error) {
	started := time.Now()
	var used int64
	if len(counters) > 0 && counters[0] != nil {
		defer func() { counters[0].RecordRead(used, time.Since(started)) }()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	limitBody, err := pq.OnlineBodyLimit(bytes)
	if err != nil {
		return nil, errors.Join(ErrRead, err)
	}
	conn.PgConn().Frontend().SetMaxBodyLen(limitBody)
	key := pgx.Identifier{s.Key}.Sanitize()
	sql := "SELECT * FROM " + quoteTable(s.Table) + " WHERE "
	args := []any{}
	if upper == nil {
		sql += "false"
	} else {
		args = append(args, *upper)
		sql += key + " <= $1"
	}
	if cursor != nil {
		args = append(args, *cursor)
		sql += fmt.Sprintf(" AND %s > $%d", key, len(args))
	}
	args = append(args, limit)
	sql += fmt.Sprintf(" ORDER BY %s LIMIT $%d", key, len(args))
	args = append([]any{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, errors.Join(ErrRead, err)
	}
	defer rows.Close()
	var selected []SelectedRow
	types := tuple.NewOnlineTypeMap()
	fields := rows.FieldDescriptions()
	if len(fields) != len(s.Columns) {
		return nil, errors.Join(ErrRead, ErrSchema)
	}
	for i, f := range fields {
		if f.Name != s.Columns[i].Name || f.DataTypeOID != s.Columns[i].OID {
			return nil, errors.Join(ErrRead, ErrSchema)
		}
	}
	for rows.Next() {
		image := make(map[string]any, len(fields))
		for i, raw := range rows.RawValues() {
			used += int64(len(raw))
			if used > bytes {
				return nil, fmt.Errorf("%w: chunk byte limit %d", ErrRead, bytes)
			}
			value, err := tuple.DecodeOnlineText(types, fields[i].DataTypeOID, raw)
			if err != nil {
				return nil, errors.Join(ErrRead, err)
			}
			image[fields[i].Name] = value
		}
		if err = s.ValidateImage(image); err != nil {
			return nil, err
		}
		k, e := IntegerKey(image, s.Key)
		if e != nil {
			return nil, e
		}
		selected = append(selected, SelectedRow{Key: k, Image: image})
	}
	if err = rows.Err(); err != nil {
		return nil, errors.Join(ErrRead, err)
	}
	return selected, nil
}
