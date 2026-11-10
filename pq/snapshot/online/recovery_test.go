package online

import (
	"strings"
	"testing"
)

func TestOnlineSlotCatalogVersion(t *testing.T) {
	for _, version := range []int{120022, 130000, 140015, 150007, 150015, 160003, 170011, 180006, 190000} {
		query := slotInspectionSQL(version)
		for _, column := range []string{"slot_type", "plugin", "database", "temporary", "active"} {
			if !strings.Contains(query, column) {
				t.Errorf("version %d: missing slot check %s", version, column)
			}
		}
		for _, feature := range []struct {
			column string
			since  int
		}{
			{"wal_status", 130000}, {" OR two_phase", 140000}, {"conflicting", 160000},
			{"invalidation_reason", 170000}, {"failover", 170000}, {"synced", 170000}, {"two_phase_at", 180000},
		} {
			if strings.Contains(query, feature.column) != (version >= feature.since) {
				t.Errorf("version %d: incompatible %s selection", version, feature.column)
			}
		}
	}
}
