package online

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

func TestOnlineControlIdentity(t *testing.T) {
	id, _ := NewID()
	image := map[string]any{"connector_id": [16]byte{}, "job_id": id, "generation": int64(1), "attempt_id": nil, "phase": "HEARTBEAT", "marker_id": id}
	change := replication.OnlineChange{Message: &format.Update{TableNamespace: "cdc_online", TableName: "watermarks", NewDecoded: image}}
	if _, control, err := DecodeControl(change); !control || err == nil {
		t.Fatal("zero UUID wire value accepted")
	}
	image["connector_id"] = id
	if _, control, err := DecodeControl(change); !control || err != nil {
		t.Fatalf("valid control rejected: %v", err)
	}
	image["phase"] = "HIGH"
	if _, _, err := DecodeControl(change); err == nil {
		t.Fatal("HIGH without attempt accepted")
	}
}
func FuzzOnlineControlIdentity(f *testing.F) {
	f.Add("LOW", "00000000-0000-4000-8000-000000000001", int64(1))
	f.Add("HEARTBEAT", "malformed", int64(-1))
	f.Fuzz(func(t *testing.T, phase, id string, generation int64) {
		image := map[string]any{"connector_id": id, "job_id": id, "generation": generation, "attempt_id": id, "phase": phase, "marker_id": id}
		_, control, err := DecodeControl(replication.OnlineChange{Message: &format.Insert{TableNamespace: "cdc_online", TableName: "watermarks", Decoded: image}})
		if !control {
			t.Fatal("metadata escaped classification")
		}
		if err == nil && (validateID(id) != nil || generation <= 0 || (phase != "LOW" && phase != "HIGH")) {
			t.Fatal("invalid identity accepted")
		}
	})
}
