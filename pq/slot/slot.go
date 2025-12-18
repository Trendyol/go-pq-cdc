package slot

import (
	"context"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorSlotIsNotExists = goerrors.New("slot is not exists")
	ErrorNotConnected    = goerrors.New("slot is not connected")
	ErrorSlotClosed      = goerrors.New("slot is closed")
)

var typeMap = pgtype.NewMap()

type XLogUpdater interface {
	UpdateXLogPos(l pq.LSN)
}

type Slot struct {
	conn            pq.Connection
	replicationConn pq.Connection
	metric          metric.Metric
	ticker          *time.Ticker
	statusSQL       string
	cfg             Config
	mu              sync.Mutex
	closed          atomic.Bool
}

func NewSlot(replicationDSN, standardDSN string, cfg Config, m metric.Metric) *Slot {
	query := fmt.Sprintf("SELECT slot_name, slot_type, active, active_pid, restart_lsn, confirmed_flush_lsn, wal_status, PG_CURRENT_WAL_LSN() AS current_lsn FROM pg_replication_slots WHERE slot_name = '%s';", cfg.Name)

	return &Slot{
		cfg:             cfg,
		conn:            pq.NewConnectionTemplate(standardDSN),
		replicationConn: pq.NewConnectionTemplate(replicationDSN),
		statusSQL:       query,
		metric:          m,
		ticker:          time.NewTicker(time.Millisecond * cfg.SlotActivityCheckerInterval),
	}
}

func (s *Slot) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Connect(ctx)
}

func (s *Slot) Create(ctx context.Context) (*Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.conn.Connect(ctx); err != nil {
		return nil, errors.Wrap(err, "slot connect")
	}
	defer func() {
		_ = s.conn.Close(ctx)
	}()

	info, err := s.infoLocked(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorSlotIsNotExists) || !s.cfg.CreateIfNotExists {
			return nil, errors.Wrap(err, "replication slot info")
		}
	} else {
		logger.Warn("replication slot already exists")
		return info, nil
	}

	// Slot needs replication connection for CREATE_REPLICATION_SLOT command
	if err := s.createSlotWithReplicationConn(ctx); err != nil {
		return nil, err
	}

	logger.Info("replication slot created", "name", s.cfg.Name)

	return s.infoLocked(ctx)
}

func (s *Slot) createSlotWithReplicationConn(ctx context.Context) error {
	if err := s.replicationConn.Connect(ctx); err != nil {
		return errors.Wrap(err, "slot replication connect")
	}
	defer func() {
		_ = s.replicationConn.Close(ctx)
	}()

	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", s.cfg.Name)
	resultReader := s.replicationConn.Exec(ctx, sql)
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "replication slot create result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "replication slot create result reader close")
	}

	return nil
}

func (s *Slot) Info(ctx context.Context) (*Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return nil, ErrorSlotClosed
	}

	return s.infoLocked(ctx)
}

func (s *Slot) infoLocked(ctx context.Context) (*Info, error) {
	resultReader := s.conn.Exec(ctx, s.statusSQL)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "replication slot info result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorSlotIsNotExists
	}

	slotInfo, err := decodeSlotInfoResult(results[0])
	if err != nil {
		return nil, errors.Wrap(err, "replication slot info result decode")
	}

	if slotInfo.Type != Logical {
		return nil, errors.Newf("'%s' replication slot must be logical but it is %s", slotInfo.Name, slotInfo.Type)
	}

	return slotInfo, nil
}

func (s *Slot) Metrics(ctx context.Context) {
	for range s.ticker.C {
		if s.closed.Load() {
			return
		}

		slotInfo, err := s.Info(ctx)
		if err != nil {
			if goerrors.Is(err, ErrorSlotClosed) {
				return
			}
			logger.Error("slot metrics", "error", err)
			continue
		}

		s.metric.SetSlotActivity(slotInfo.Active)
		s.metric.SetSlotCurrentLSN(float64(slotInfo.CurrentLSN))
		s.metric.SetSlotConfirmedFlushLSN(float64(slotInfo.ConfirmedFlushLSN))
		s.metric.SetSlotRetainedWALSize(float64(slotInfo.RetainedWALSize))
		s.metric.SetSlotLag(float64(slotInfo.Lag))

		logger.Debug("slot metrics", "info", slotInfo)
	}
}

func (s *Slot) Close(ctx context.Context) {
	s.closed.Store(true)
	s.ticker.Stop()

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
	}
}

func decodeSlotInfoResult(result *pgconn.Result) (*Info, error) {
	var slotInfo Info
	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "slot_name":
			slotInfo.Name = v.(string)
		case "slot_type":
			slotInfo.Type = Type(v.(string))
		case "active":
			slotInfo.Active = v.(bool)
		case "active_pid":
			slotInfo.ActivePID = v.(int32)
		case "restart_lsn":
			slotInfo.RestartLSN, _ = pq.ParseLSN(v.(string))
		case "confirmed_flush_lsn":
			slotInfo.ConfirmedFlushLSN, _ = pq.ParseLSN(v.(string))
		case "wal_status":
			slotInfo.WalStatus = v.(string)
		case "current_lsn":
			slotInfo.CurrentLSN, _ = pq.ParseLSN(v.(string))
		}
	}

	slotInfo.RetainedWALSize = slotInfo.CurrentLSN - slotInfo.RestartLSN
	slotInfo.Lag = slotInfo.CurrentLSN - slotInfo.ConfirmedFlushLSN

	return &slotInfo, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
