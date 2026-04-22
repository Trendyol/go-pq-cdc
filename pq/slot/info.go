// Package slot manages PostgreSQL replication slots for CDC.
package slot

import (
	"github.com/Trendyol/go-pq-cdc/pq"
)

// Replication slot type constants.
const (
	Logical  Type = "logical"
	Physical Type = "physical"
)

// Type represents a replication slot type (logical or physical).
type Type string

// Info holds status and position information for a replication slot.
type Info struct {
	Name              string `json:"name"`
	Type              Type   `json:"type"`
	WalStatus         string `json:"walStatus"`
	RestartLSN        pq.LSN `json:"restartLSN"`
	ConfirmedFlushLSN pq.LSN `json:"confirmedFlushLSN"`
	CurrentLSN        pq.LSN `json:"currentLSN"`
	RetainedWALSize   pq.LSN `json:"retainedWALSize"`
	Lag               pq.LSN `json:"lag"`
	ActivePID         int32  `json:"activePID"`
	Active            bool   `json:"active"`
}
