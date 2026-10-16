package online

import (
	"fmt"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

const (
	metadataSchema = "cdc_online"
	watermarkTable = "watermarks"
)

func uuidText(v any) (string, error) {
	switch u := v.(type) {
	case string:
		if err := validateID(u); err != nil {
			return "", err
		}
		return u, nil
	case [16]byte:
		id := fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
		if err := validateID(id); err != nil {
			return "", err
		}
		return id, nil
	}
	return "", ErrConditional
}

// DecodeControl classifies only the exact watermark table. All required fields
// must decode, even for another connector. Unknown metadata is never business
// data and fails closed. No control implicitly advances source confirmation.
func DecodeControl(change replication.OnlineChange) (Marker, bool, error) {
	var table Table
	var image map[string]any
	switch m := change.Message.(type) {
	case *format.Insert:
		table = Table{m.TableNamespace, m.TableName}
		image = m.Decoded
	case *format.Update:
		table = Table{m.TableNamespace, m.TableName}
		image = m.NewDecoded
	case *format.Delete:
		table = Table{m.TableNamespace, m.TableName}
		if table.Schema == "cdc_online" {
			return Marker{}, true, ErrConditional
		}
	default:
		return Marker{}, false, ErrSchema
	}
	if table.Schema != "cdc_online" {
		return Marker{}, false, nil
	}
	if table.Name != "watermarks" || len(image) != 6 {
		return Marker{}, true, ErrConditional
	}
	marker, err := markerFromImage(image)
	return marker, true, err
}

func markerFromImage(image map[string]any) (Marker, error) {
	var marker Marker
	var err error
	marker.ConnectorID, err = uuidText(image["connector_id"])
	if err != nil {
		return marker, err
	}
	marker.JobID, err = uuidText(image["job_id"])
	if err != nil {
		return marker, err
	}
	marker.ID, err = uuidText(image["marker_id"])
	if err != nil {
		return marker, err
	}
	marker.Generation, err = IntegerKey(image, "generation")
	if err != nil || marker.Generation < 1 {
		return marker, ErrConditional
	}
	phase, ok := image["phase"].(string)
	if !ok {
		return marker, ErrConditional
	}
	marker.Phase = MarkerPhase(phase)
	switch marker.Phase {
	case Heartbeat:
		if image["attempt_id"] != nil {
			return marker, ErrConditional
		}
	case Low, High:
		marker.AttemptID, err = uuidText(image["attempt_id"])
		if err != nil {
			return marker, err
		}
	default:
		return marker, ErrConditional
	}
	return marker, nil
}
func BusinessEvent(change replication.OnlineChange, env *replication.OnlineCommittedEnvelope, schemas []TableSchema, binding string) (Event, error) {
	e := Event{ID: CDCEventID(binding, env.EndLSN, change.Ordinal), CommitLSN: env.CommitLSN, Order: OrderToken{EndLSN: env.EndLSN, Ordinal: change.Ordinal}}
	var old map[string]any
	switch m := change.Message.(type) {
	case *format.Insert:
		e.Table = Table{m.TableNamespace, m.TableName}
		e.Operation = Insert
		e.Image = m.Decoded
	case *format.Update:
		e.Table = Table{m.TableNamespace, m.TableName}
		e.Operation = Update
		e.Image = m.NewDecoded
		old = m.OldDecoded
	case *format.Delete:
		e.Table = Table{m.TableNamespace, m.TableName}
		e.Operation = Delete
		old = m.OldDecoded
	default:
		return e, ErrSchema
	}
	for _, s := range schemas {
		if s.Table != e.Table {
			continue
		}
		image := e.Image
		if e.Operation == Delete {
			image = old
		}
		if err := s.ValidateImage(image); err != nil {
			return e, err
		}
		key, err := IntegerKey(image, s.Key)
		if err != nil {
			return e, err
		}
		e.Key = key
		if e.Operation == Update {
			if err = s.ValidateImage(old); err != nil {
				return e, err
			}
			oldKey, err := IntegerKey(old, s.Key)
			if err != nil || oldKey != key {
				return e, fmt.Errorf("%w: primary key changed", ErrSchema)
			}
		}
		return e, nil
	}
	return e, fmt.Errorf("%w: uncataloged captured table", ErrSchema)
}

// matchMarker is also the model seam for replay fencing. A valid old generation
// is control-only: it neither satisfies a fresh fence nor grants ACK permission.
// Current-generation stale attempts/duplicates at a different fence fail closed.
func matchMarker(job Job, marker Marker, expected *Marker, changes int) (bool, error) {
	if marker.ConnectorID != job.ConnectorID {
		return false, nil
	}
	if marker.JobID != job.ID || marker.Generation > job.Generation {
		return false, ErrConditional
	}
	if marker.Generation < job.Generation {
		return false, nil
	}
	if marker.Phase == Heartbeat {
		return expected != nil && *expected == marker, nil
	}
	if expected == nil || marker != *expected || changes != 1 {
		return false, ErrConditional
	}
	return true, nil
}
