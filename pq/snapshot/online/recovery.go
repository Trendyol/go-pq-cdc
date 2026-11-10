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
// CopyBoth connection. SQL identity/primary checks are repeated by Next.
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

// slotInspectionSQL selects fields by server capability, without a major allowlist.
// conflicting is available from PG16; invalidation/failover fields from PG17.
func slotInspectionSQL(version int) string {
	bad := `slot_type<>'logical' OR plugin IS DISTINCT FROM 'pgoutput' OR database IS DISTINCT FROM current_database() OR temporary OR active`
	if version >= 130000 {
		bad += ` OR wal_status='lost'`
	}
	if version >= 140000 {
		bad += ` OR two_phase`
	}
	if version >= 160000 {
		bad += ` OR conflicting`
	}
	if version >= 170000 {
		bad += ` OR invalidation_reason IS NOT NULL OR failover OR synced`
	}
	if version >= 180000 {
		bad += ` OR two_phase_at IS NOT NULL`
	}
	return `SELECT confirmed_flush_lsn::text,` + bad + ` FROM pg_replication_slots WHERE slot_name=$1`
}

func InspectSlot(ctx context.Context, conn *pgx.Conn, name string) (SlotState, error) {
	s := SlotState{Name: name}
	version, err := serverVersion(ctx, conn)
	if err != nil {
		return s, err
	}
	query := slotInspectionSQL(version)
	var confirmed *string
	var bad bool
	err = conn.QueryRow(ctx, query, name).Scan(&confirmed, &bad)
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
