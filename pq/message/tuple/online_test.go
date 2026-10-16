package tuple

import (
	"encoding/json"
	"github.com/jackc/pgx/v5/pgtype"
	"testing"
)

func TestOnlineLosslessValues(t *testing.T) {
	for _, oid := range []uint32{999999, pgtype.NumericOID, pgtype.TimestampOID, pgtype.ByteaOID} {
		if _, err := DecodeOnlineText(NewOnlineTypeMap(), oid, []byte("1")); err == nil {
			t.Fatalf("unqualified OID accepted: %d", oid)
		}
	}
	for _, raw := range []string{`{{1,2},{3,4}}`, `[0:1]={1,2}`} {
		if _, err := DecodeOnlineText(NewOnlineTypeMap(), pgtype.Int4ArrayOID, []byte(raw)); err == nil {
			t.Fatalf("unqualified array shape accepted: %s", raw)
		}
	}
	types := NewOnlineTypeMap()
	for _, oid := range []uint32{114, 3802} {
		value, err := DecodeOnlineText(types, oid, []byte(`{"n":9007199254740993}`))
		if err != nil {
			t.Fatal(err)
		}
		if value.(map[string]any)["n"] != json.Number("9007199254740993") {
			t.Fatalf("rounded JSON: %v", value)
		}
	}
	array, err := DecodeOnlineText(types, pgtype.JSONArrayOID, []byte(`{"{\"n\":9007199254740993}"}`))
	if err != nil {
		t.Fatal(err)
	}
	if array.([]any)[0].(map[string]any)["n"] != json.Number("9007199254740993") {
		t.Fatal("rounded JSON array")
	}
	floats, err := DecodeOnlineText(types, pgtype.Float8ArrayOID, []byte(`{Infinity,NaN,-Infinity,1.25}`))
	if err != nil {
		t.Fatal(err)
	}
	if floats.([]any)[0] != "Infinity" || floats.([]any)[1] != "NaN" || floats.([]any)[2] != "-Infinity" {
		t.Fatal("uncanonical float array")
	}
	for _, text := range []string{"Infinity", "-Infinity", "NaN"} {
		value, err := DecodeOnlineText(types, 701, []byte(text))
		if err != nil || value != text {
			t.Fatalf("special value %s: %v %v", text, value, err)
		}
	}
}
