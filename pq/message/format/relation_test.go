package format

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelation_New(t *testing.T) {
	data := []byte{82, 0, 0, 64, 6, 112, 117, 98, 108, 105, 99, 0, 116, 0, 100, 0, 2, 1, 105, 100, 0, 0, 0, 0, 23, 255, 255, 255, 255, 0, 110, 97, 109, 101, 0, 0, 0, 0, 25, 255, 255, 255, 255}

	rel, err := NewRelation(data, false)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Relation{
		OID:           16390,
		XID:           0,
		Namespace:     "public",
		Name:          "t",
		ReplicaID:     100,
		ColumnNumbers: 2,
		Columns: []tuple.RelationColumn{
			{
				Flags:        1,
				Name:         "id",
				DataType:     23,
				TypeModifier: 4294967295,
			},
			{
				Flags:        0,
				Name:         "name",
				DataType:     25,
				TypeModifier: 4294967295,
			},
		},
	}

	assert.Equal(t, expected, rel)
}

func TestRelation_TruncatedReturnsError(t *testing.T) {
	// 'R', OID(4), namespace "a\0", name "b\0", then nothing where the replica
	// identity byte is expected. This passes the only length guard (len < 8).
	data := []byte{'R', 0, 0, 0, 1, 'a', 0, 'b', 0}
	_, err := NewRelation(data, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "relation message missing replica identity")
}

// Every truncation point in the relation decoder must surface as an error with a
// message naming the missing field, so a wrong or reordered guard is caught.
func TestRelation_DecodeGuardsOnTruncatedInput(t *testing.T) {
	// 'R', OID(4), namespace "a\0", name "b\0". Everything after this prefix is
	// appended per case.
	prefix := []byte{'R', 0, 0, 0, 1, 'a', 0, 'b', 0}
	withTail := func(tail ...byte) []byte {
		return append(append([]byte{}, prefix...), tail...)
	}

	cases := []struct {
		name     string
		data     []byte
		streamed bool
		wantErr  string
	}{
		{name: "missing replica identity", data: withTail(), wantErr: "relation message missing replica identity"},
		{name: "missing column count", data: withTail(1), wantErr: "relation message missing column count"},
		{name: "one byte column count", data: withTail(1, 0), wantErr: "relation message missing column count"},
		{name: "first column missing flags", data: withTail(1, 0, 1), wantErr: "relation message columns[0] missing flags"},
		{name: "first column name unterminated", data: withTail(1, 0, 1, 1, 'x'), wantErr: "relation message columns[0].name decode error"},
		{name: "first column missing type info", data: withTail(1, 0, 1, 1, 'x', 0, 0, 0, 0, 23), wantErr: "relation message columns[0] missing type info"},
		{name: "second column missing flags", data: withTail(1, 0, 2, 1, 'x', 0, 0, 0, 0, 23, 255, 255, 255, 255), wantErr: "relation message columns[1] missing flags"},
		{name: "namespace unterminated", data: []byte{'R', 0, 0, 0, 1, 'a', 'b', 'c'}, wantErr: "relation message namespace decode error"},
		{name: "name unterminated", data: []byte{'R', 0, 0, 0, 1, 'a', 0, 'b', 'c'}, wantErr: "relation message name decode error"},
		{name: "header too short", data: []byte{'R', 0, 0}, wantErr: "relation message length must be at least 8 byte, but got 3"},
		{name: "streamed header too short", data: []byte{'R', 0, 0, 0, 1}, streamed: true, wantErr: "streamed transaction relation message length must be at least 12 byte, but got 5"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rel, err := NewRelation(tc.data, tc.streamed)

			require.Error(t, err)
			assert.Nil(t, rel)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// The smallest well formed relation message: empty namespace, empty name, and
// zero columns. It must decode, proving the guards do not reject valid input.
func TestRelation_MinimalZeroColumnMessageDecodes(t *testing.T) {
	data := []byte{'R', 0, 0, 0, 1, 0, 0, 'd', 0, 0}

	rel, err := NewRelation(data, false)

	require.NoError(t, err)
	assert.Equal(t, uint32(1), rel.OID)
	assert.Empty(t, rel.Namespace)
	assert.Empty(t, rel.Name)
	assert.Equal(t, uint8('d'), rel.ReplicaID)
	assert.Equal(t, uint16(0), rel.ColumnNumbers)
	assert.Empty(t, rel.Columns)
}
