package slot

import (
	"github.com/vskurikhin/go-pq-cdc/pq"
)

const (
	Logical  Type = "logical"
	Physical Type = "physical"
)

type Type string

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
