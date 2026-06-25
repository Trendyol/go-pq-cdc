package slot

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// A physical / not-yet-reserved slot reports an empty confirmed_flush_lsn.
// It must not blow up with a cryptic "lsn parse: EOF"; the empty column is
// skipped and the logical-type check yields a clear error instead.
func TestDecodeSlotInfoResult_EmptyConfirmedFlushLSN(t *testing.T) {
	result := &pgconn.Result{
		FieldDescriptions: []pgconn.FieldDescription{
			{Name: "slot_name", DataTypeOID: pgtype.TextOID},
			{Name: "slot_type", DataTypeOID: pgtype.TextOID},
			{Name: "restart_lsn", DataTypeOID: pgtype.TextOID},
			{Name: "confirmed_flush_lsn", DataTypeOID: pgtype.TextOID},
		},
		Rows: [][][]byte{{
			[]byte("contents_to_contentmedias_slot"),
			[]byte("physical"),
			[]byte("0/1A2B3C4"),
			[]byte(""), // NULL pg_lsn decoded as empty string
		}},
	}

	info, err := decodeSlotInfoResult(result)
	if err != nil {
		t.Fatalf("expected empty confirmed_flush_lsn to be skipped, got: %v", err)
	}
	if info.ConfirmedFlushLSN != 0 {
		t.Fatalf("expected zero ConfirmedFlushLSN, got: %v", info.ConfirmedFlushLSN)
	}
	if info.Type != Physical {
		t.Fatalf("expected Physical type, got: %v", info.Type)
	}
}
