package pq

import (
	"sync/atomic"
)

type Metric struct {
	TotalInsert atomic.Int64
	TotalUpdate atomic.Int64
	TotalDelete atomic.Int64

	DcpLatency     int64
	ProcessLatency int64
}

func (om *Metric) InsertOpIncrement(count int64) {
	om.TotalInsert.Add(count)
}

func (om *Metric) UpdateOpIncrement(count int64) {
	om.TotalUpdate.Add(count)
}

func (om *Metric) DeleteOpIncrement(count int64) {
	om.TotalDelete.Add(count)
}
