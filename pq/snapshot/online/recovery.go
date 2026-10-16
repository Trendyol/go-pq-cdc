package online

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IdentifySource uses a dedicated replication connection, never the active
// CopyBoth connection. SQL identity/PG16/primary checks are repeated by Next.
func IdentifySource(ctx context.Context, dsn string) (SourceIdentity, error) {
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return SourceIdentity{}, err
	}
	defer conn.Close(context.Background())
	results, err := conn.Exec(ctx, "IDENTIFY_SYSTEM").ReadAll()
	if err != nil {
		return SourceIdentity{}, err
	}
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 4 {
		return SourceIdentity{}, ErrBinding
	}
	row := results[0].Rows[0]
	timeline, e := strconv.ParseInt(string(row[1]), 10, 32)
	if e != nil {
		return SourceIdentity{}, e
	}
	return SourceIdentity{SystemID: string(row[0]), Database: string(row[3]), Timeline: int32(timeline)}, nil
}
func InspectSlot(ctx context.Context, conn *pgx.Conn, name string) (SlotState, error) {
	s := SlotState{Name: name}
	var confirmed *string
	var bad bool
	err := conn.QueryRow(ctx, `SELECT confirmed_flush_lsn::text,slot_type<>'logical' OR plugin<>'pgoutput' OR database<>current_database() OR temporary OR two_phase OR wal_status='lost' OR active FROM pg_replication_slots WHERE slot_name=$1`, name).Scan(&confirmed, &bad)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	s.Exists = true
	s.Invalid = bad || confirmed == nil
	if confirmed != nil {
		s.ConfirmedLSN, err = pq.ParseLSN(*confirmed)
	}
	return s, err
}

// CreateFreshSlot returns proven creation provenance and the consistent safe
// position. An unknown COMMIT response leaves an unbound slot for explicit
// operator reconciliation. Never adopt/drop/recreate an existing slot.
func CreateFreshSlot(ctx context.Context, conn *pgx.Conn, name string, allow bool) (pq.LSN, error) {
	s, err := InspectSlot(ctx, conn, name)
	if err != nil {
		return 0, err
	}
	if s.Exists {
		return 0, fmt.Errorf("%w: existing unbound slot %q requires explicit reconciliation", ErrBinding, name)
	}
	if !allow {
		return 0, fmt.Errorf("%w: fresh online job requires slot.createIfNotExists", ErrBinding)
	}
	var created, pos string
	err = conn.QueryRow(ctx, `SELECT slot_name,lsn::text FROM pg_create_logical_replication_slot($1,'pgoutput',false,false)`, name).Scan(&created, &pos)
	if err != nil {
		return 0, err
	}
	if created != name {
		return 0, ErrBinding
	}
	lsn, err := pq.ParseLSN(pos)
	if lsn == 0 {
		return 0, ErrBinding
	}
	return lsn, err
}
