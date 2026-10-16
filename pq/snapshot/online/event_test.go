package online

import (
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOnlineEventIdentityAndOrder(t *testing.T) {
	table := Table{Schema: "public", Name: "items"}
	id := CDCEventID("binding", pq.LSN(100), 2)
	require.Equal(t, id, CDCEventID("binding", 100, 2))
	require.NotEqual(t, id, CDCEventID("other", 100, 2))
	require.NotEqual(t, id, CDCEventID("binding", 100, 3))
	require.NotEqual(t, READEventID("job", "attempt", table, -1), READEventID("job", "attempt2", table, -1))
	require.NotEqual(t, READEventID("job", "attempt", Table{"a.b", "c"}, 1), READEventID("job", "attempt", Table{"a", "b.c"}, 1))
	tokens := []OrderToken{{100, 0, 0}, {100, 0, 1}, {100, 1, 0}, {101, 0, 0}, {pq.LSN(1 << 63), 0, 0}}
	for i := range tokens {
		copyOfToken := tokens[i]
		require.Zero(t, tokens[i].Compare(copyOfToken))
		if i > 0 {
			require.Equal(t, -1, tokens[i-1].Compare(tokens[i]))
			require.Equal(t, 1, tokens[i].Compare(tokens[i-1]))
		}
	}
	env := replication.OnlineCommittedEnvelope{CommitLSN: 99, EndLSN: 100, Changes: []replication.OnlineChange{{Ordinal: 2}}}
	event := Event{ID: CDCEventID("binding", env.EndLSN, env.Changes[0].Ordinal), Table: table, Key: -1, Operation: Insert, Image: map[string]any{"id": int64(-1), "nullable": nil}, CommitLSN: env.CommitLSN, Order: OrderToken{env.EndLSN, 0, 2}}
	require.NoError(t, event.Validate())
	event.Operation = Read
	event.Order.Phase = 1
	event.CommitLSN = 0
	require.NoError(t, event.Validate())
	event.Image = nil
	require.Error(t, event.Validate())
	event.Operation = Delete
	event.Order.Phase = 0
	require.NoError(t, event.Validate())
	event.Order.Phase = 2
	require.Error(t, event.Validate())
}

func TestOnlineResumeBinding(t *testing.T) {
	identity := SourceIdentity{SystemID: "123", Database: "db", Timeline: 1}
	job := Job{ID: "job", Slot: "slot", SlotBinding: "binding", Source: identity, ConfigFingerprint: "cfg", Checkpoint: 100}
	slot := SlotState{Name: "slot", Exists: true, ConfirmedLSN: 90}
	require.NoError(t, ValidateResume(job, slot, identity, "cfg"))
	for _, change := range []func(*SlotState){func(s *SlotState) { s.Exists = false }, func(s *SlotState) { s.Invalid = true }, func(s *SlotState) { s.Name = "other" }, func(s *SlotState) { s.ConfirmedLSN = 101 }} {
		bad := slot
		change(&bad)
		require.ErrorIs(t, ValidateResume(job, bad, identity, "cfg"), ErrBinding)
	}
	bad := identity
	bad.Timeline++
	require.ErrorIs(t, ValidateResume(job, slot, bad, "cfg"), ErrBinding)
	require.ErrorIs(t, ValidateResume(job, slot, identity, "changed"), ErrBinding)
	job.SlotBinding = ""
	require.ErrorIs(t, ValidateResume(job, slot, identity, "cfg"), ErrBinding)
}

func TestOnlineMetadataSQL(t *testing.T) {
	sql, err := ProvisionSQL(`cdc"role`)
	require.NoError(t, err)
	require.Contains(t, sql, `TO "cdc""role"`)
	require.Contains(t, sql, "schema_version")
	require.NotContains(t, sql, "json")
	require.NotContains(t, sql, "payload")
	_, err = ProvisionSQL("bad\x00role")
	require.Error(t, err)
	for range 10 {
		id, err := NewID()
		require.NoError(t, err)
		require.NoError(t, validateID(id))
	}
}
