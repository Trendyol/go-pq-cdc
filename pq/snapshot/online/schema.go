package online

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"

	"github.com/jackc/pgx/v5"
)

var ErrSchema = errors.New("online schema/profile changed or unsupported")

type Column struct {
	Name     string
	OID      uint32
	Modifier int32
	NotNull  bool
}
type TableSchema struct {
	Table          Table
	OID            uint32
	Key            string
	Columns        []Column
	Fingerprint    string
	KeyFingerprint string
}

func quoteTable(t Table) string { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }

// ValidatePublication validates ALL published business tables, including those
// not selected for READs. It does not alter replica identity or silently repair
// an unsupported profile. Application DDL must remain frozen during a run.
func ValidatePublication(ctx context.Context, conn *pgx.Conn, name string) ([]TableSchema, error) {
	var all, insert, update, del, truncate, schemaWide bool
	err := conn.QueryRow(ctx, `SELECT puballtables,pubinsert,pubupdate,pubdelete,pubtruncate,EXISTS(SELECT 1 FROM pg_publication_namespace pn WHERE pn.pnpubid=p.oid) FROM pg_publication p WHERE pubname=$1`, name).Scan(&all, &insert, &update, &del, &truncate, &schemaWide)
	if err != nil {
		return nil, fmt.Errorf("%w: publication: %w", ErrSchema, err)
	}
	if all || schemaWide || !insert || !update || !del || !truncate {
		return nil, fmt.Errorf("%w: require explicit I/U/D/TRUNCATE publication", ErrSchema)
	}
	rows, err := conn.Query(ctx, `SELECT n.nspname,c.relname,c.oid,pr.prqual IS NOT NULL,pr.prattrs IS NOT NULL FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid JOIN pg_class c ON c.oid=pr.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE p.pubname=$1 ORDER BY n.nspname,c.relname`, name)
	if err != nil {
		return nil, err
	}
	type entry struct {
		table Table
		oid   uint32
	}
	var entries []entry
	watermark := false
	for rows.Next() {
		var e entry
		var filter, columns bool
		if err = rows.Scan(&e.table.Schema, &e.table.Name, &e.oid, &filter, &columns); err != nil {
			break
		}
		if filter || columns {
			err = fmt.Errorf("%w: filtered/column-listed %s", ErrSchema, quoteTable(e.table))
			break
		}
		if e.table.Schema == "cdc_online" {
			if e.table.Name != "watermarks" {
				err = fmt.Errorf("%w: progress table published", ErrSchema)
				break
			}
			watermark = true
			continue
		}
		entries = append(entries, e)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if !watermark {
		return nil, fmt.Errorf("%w: cdc_online.watermarks publication membership required", ErrSchema)
	}
	if err = EnsureWatermarkPublication(ctx, conn, name, false); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: no business tables", ErrSchema)
	}
	result := make([]TableSchema, 0, len(entries))
	for _, e := range entries {
		s, e2 := InspectTable(ctx, conn, e.table)
		if e2 != nil {
			return nil, e2
		}
		result = append(result, s)
	}
	return result, nil
}
func InspectTable(ctx context.Context, conn *pgx.Conn, table Table) (TableSchema, error) {
	s := TableSchema{Table: table}
	var kind, identity string
	var partition, rls, inherits bool
	err := conn.QueryRow(ctx, `SELECT c.oid,c.relkind::text,c.relreplident::text,c.relispartition,c.relrowsecurity OR c.relforcerowsecurity,EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`, table.Schema, table.Name).Scan(&s.OID, &kind, &identity, &partition, &rls, &inherits)
	fail := func(reason string) (TableSchema, error) {
		return s, fmt.Errorf("%w: %s: %s", ErrSchema, quoteTable(table), reason)
	}
	if err != nil {
		return s, fmt.Errorf("%w: %s: %w", ErrSchema, quoteTable(table), err)
	}
	if kind != "r" || identity != "f" || partition || rls || inherits {
		return fail("require ordinary FULL table without partitions/inheritance/RLS")
	}
	rows, err := conn.Query(ctx, `SELECT attname,atttypid,atttypmod,attnotnull,attgenerated::text,attndims FROM pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped ORDER BY attnum`, s.OID)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var c Column
		var generated string
		var dimensions int16
		if err = rows.Scan(&c.Name, &c.OID, &c.Modifier, &c.NotNull, &generated, &dimensions); err != nil {
			break
		}
		if generated != "" {
			err = ErrSchema
			break
		}
		if !tuple.OnlineBusinessOID(c.OID) || dimensions > 1 {
			err = fmt.Errorf("%w: business column %s OID %d is unqualified; POC allows bool/integer/text/float/JSON scalars and 1D arrays only (UUID is control-only)", ErrSchema, c.Name, c.OID)
			break
		}
		s.Columns = append(s.Columns, c)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return s, fmt.Errorf("%w: generated or unreadable columns: %w", ErrSchema, err)
	}
	var keys []string
	var valid bool
	err = conn.QueryRow(ctx, `SELECT array_agg(a.attname ORDER BY k.ord),bool_and(a.atttypid IN (20,21,23) AND a.attnotnull) FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(num,ord) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num WHERE i.indrelid=$1 AND i.indisprimary AND i.indisvalid AND k.ord<=i.indnkeyatts`, s.OID).Scan(&keys, &valid)
	if err != nil {
		return s, fmt.Errorf("%w: primary key catalog: %w", ErrSchema, err)
	}
	if len(keys) != 1 || !valid {
		return fail("require one integer-family primary key")
	}
	s.Key = keys[0]
	data, _ := json.Marshal(struct {
		OID     uint32
		Columns []Column
		Key     string
	}{s.OID, s.Columns, s.Key})
	hash := sha256.Sum256(data)
	s.Fingerprint = hex.EncodeToString(hash[:])
	keyHash := sha256.Sum256([]byte(s.Key))
	s.KeyFingerprint = hex.EncodeToString(keyHash[:])
	return s, nil
}
func ValidateSchemas(expected, actual []TableSchema) error {
	if len(expected) != len(actual) {
		return ErrSchema
	}
	for i := range expected {
		if expected[i].Table != actual[i].Table || expected[i].Fingerprint != actual[i].Fingerprint {
			return ErrSchema
		}
	}
	return nil
}
func IntegerKey(image map[string]any, key string) (int64, error) {
	switch v := image[key].(type) {
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	}
	return 0, fmt.Errorf("%w: absent/non-integer key %s", ErrSchema, key)
}
func (s TableSchema) ValidateImage(image map[string]any) error {
	if len(image) != len(s.Columns) {
		return fmt.Errorf("%w: incomplete FULL image for %s", ErrSchema, quoteTable(s.Table))
	}
	for _, c := range s.Columns {
		value, ok := image[c.Name]
		if !ok || (c.NotNull && value == nil) {
			return ErrSchema
		}
	}
	return nil
}
