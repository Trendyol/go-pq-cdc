package slot

import (
	"github.com/Trendyol/go-pq-cdc/pq"
)

const (
	Logical  Type = "logical"
	Physical Type = "physical"
)

type Type string

type Info struct {
	Name              string `json:"name"`
	Type              Type   `json:"type"`
	Active            bool   `json:"active"`
	ActivePID         int32  `json:"activePID"`
	RestartLSN        pq.LSN `json:"restartLSN"`
	ConfirmedFlushLSN pq.LSN `json:"confirmedFlushLSN"`
	WalStatus         string `json:"walStatus"`
	CurrentLSN        pq.LSN `json:"currentLSN"`
	RetainedWALSize   pq.LSN `json:"retainedWALSize"`
	Lag               pq.LSN `json:"lag"`
}
