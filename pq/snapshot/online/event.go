// Package online provides the opt-in snapshot delivery and metadata contracts.
// It does not coordinate reads, delivery, source feedback, or slot creation.
package online

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
)

type Table struct {
	Schema string
	Name   string
}
type Operation string

const (
	Insert Operation = "INSERT"
	Update Operation = "UPDATE"
	Delete Operation = "DELETE"
	Read   Operation = "READ"
)

// OrderToken is a lexicographic delivery version, separate from CommitLSN.
// Phase 0 is CDC; phase 1 is READ at the dedicated HIGH transaction boundary.
type OrderToken struct {
	EndLSN  pq.LSN
	Phase   uint8
	Ordinal uint32
}

func (o OrderToken) Compare(other OrderToken) int {
	if o.EndLSN < other.EndLSN {
		return -1
	}
	if o.EndLSN > other.EndLSN {
		return 1
	}
	if o.Phase < other.Phase {
		return -1
	}
	if o.Phase > other.Phase {
		return 1
	}
	if o.Ordinal < other.Ordinal {
		return -1
	}
	if o.Ordinal > other.Ordinal {
		return 1
	}
	return 0
}

// Event carries a complete row Image (including NULLs), not a patch. DELETE
// may omit it; the key and version must still be retained as a tombstone.
// A handler returning nil certifies durable completion, not enqueue success.
type Event struct {
	ID        string
	Table     Table
	Key       int64
	Operation Operation
	Image     map[string]any
	CommitLSN pq.LSN // Source commit LSN for CDC; zero for a synthetic READ.
	Order     OrderToken
}
type Handler func(context.Context, Event) error

func (e Event) Validate() error {
	if e.ID == "" || e.Table.Schema == "" || e.Table.Name == "" || e.Order.EndLSN == 0 {
		return fmt.Errorf("online event requires identity, table and nonzero order boundary")
	}
	if e.Operation != Insert && e.Operation != Update && e.Operation != Delete && e.Operation != Read {
		return fmt.Errorf("unsupported online operation %q", e.Operation)
	}
	phase := uint8(0)
	if e.Operation == Read {
		phase = 1
	}
	if e.Order.Phase != phase {
		return fmt.Errorf("online operation/order phase mismatch")
	}
	if e.Operation != Delete && len(e.Image) == 0 {
		return fmt.Errorf("online event requires a complete image")
	}
	return nil
}

// IDs hash length-delimited components so arbitrary quoted table names cannot
// collide. CDC deliberately excludes job/generation: it is stable on restart.
func identity(kind string, parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(strconv.Itoa(len(part)) + ":"))
		h.Write([]byte(part))
	}
	return kind + ":" + hex.EncodeToString(h.Sum(nil))
}
func CDCEventID(slotBinding string, end pq.LSN, ordinal uint32) string {
	return identity("cdc", slotBinding, end.String(), strconv.FormatUint(uint64(ordinal), 10))
}
func READEventID(jobID, attemptID string, table Table, key int64) string {
	return identity("read", jobID, attemptID, table.Schema, table.Name, strconv.FormatInt(key, 10))
}

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func validateID(id string) error {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return fmt.Errorf("invalid online UUID")
	}
	b, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil || len(b) != 16 || strings.Trim(id, "0-") == "" {
		return fmt.Errorf("invalid online UUID")
	}
	return nil
}
