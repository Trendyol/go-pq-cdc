package replication

import (
	"context"
	"sync"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5"
)

type streamOnline struct {
	*stream
	conn pq.Connection

	// used to fetch snapshot data
	snapshotConn *pgx.Conn
}

func newOnlineStreamer(dsn string, cfg config.Config, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	return streamOnline{
		stream: &stream{
			conn:         pq.NewConnectionTemplate(dsn),
			metric:       m,
			config:       cfg,
			relation:     make(map[uint32]*format.Relation),
			messageCH:    make(chan *Message, 1000),
			listenerFunc: listenerFunc,
			lastXLogPos:  0,
			sinkEnd:      make(chan struct{}, 1),
			processEnd:   make(chan struct{}, 1),
			mu:           &sync.RWMutex{},
		},
		snapshotConn: nil,
	}
}

func (s *streamOnline) Open(ctx context.Context) error {
	//
	s.handlePayload()
	s.conn.(ctx)

	// loop
	// low
	// snapshot
	//high

	// loop end

	return s.open(ctx, s.runWithOnlineSnapshot)
}

func (s *streamOnline) runWithOnlineSnapshot(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Track this whole pipeline as one lifecycle operation.
	// Close must cancel it and wait for its completion.
	s.sinkStarted.Store(true)
	s.processStarted.Store(true)

	// todo; sink kullanmam loop icinde surekli cekiyor. bunu pauase edip low high emit edip resume etmemiz gerekecek
	go s.sink(runCtx)
	go s.onlineProcess(runCtx)
}

func (s *streamOnline) onlineProcess(ctx context.Context) {

}
