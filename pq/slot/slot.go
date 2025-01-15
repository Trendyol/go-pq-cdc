package slot

import (
	"context"
	goerrors "errors"
	"fmt"
	"time"

	"github.com/vskurikhin/go-pq-cdc/internal/metric"
	"github.com/vskurikhin/go-pq-cdc/logger"
	"github.com/vskurikhin/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorSlotIsNotExists = goerrors.New("slot is not exists")
)

var typeMap = pgtype.NewMap()

type XLogUpdater interface {
	UpdateXLogPos(l pq.LSN)
}

type Slot struct {
	conn       pq.Connection
	metric     metric.Metric
	logUpdater XLogUpdater
	ticker     *time.Ticker
	statusSQL  string
	cfg        Config
}

func NewSlot(ctx context.Context, dsn string, cfg Config, m metric.Metric, updater XLogUpdater) (*Slot, error) {
	query := fmt.Sprintf("SELECT slot_name, slot_type, active, active_pid, restart_lsn, confirmed_flush_lsn, wal_status, PG_CURRENT_WAL_LSN() AS current_lsn FROM pg_replication_slots WHERE slot_name = '%s';", cfg.Name)

	conn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "new slot connection")
	}

	return &Slot{
		cfg:        cfg,
		conn:       conn,
		statusSQL:  query,
		metric:     m,
		ticker:     time.NewTicker(time.Millisecond * cfg.SlotActivityCheckerInterval),
		logUpdater: updater,
	}, nil
}

func (s *Slot) Create(ctx context.Context) (*Info, error) {
	info, err := s.Info(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorSlotIsNotExists) || !s.cfg.CreateIfNotExists {
			return nil, errors.Wrap(err, "replication slot info")
		}
	} else {
		logger.Warn("replication slot already exists")
		return info, nil
	}

	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", s.cfg.Name)
	resultReader := s.conn.Exec(ctx, sql)
	_, err = resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "replication slot create result")
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "replication slot create result reader close")
	}

	logger.Info("replication slot created", "name", s.cfg.Name)

	return s.Info(ctx)
}

func (s *Slot) Info(ctx context.Context) (*Info, error) {
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
		slotInfo, err := s.Info(ctx)
		if err != nil {
			logger.Error("slot metrics", "error", err)
			continue
		}

		s.metric.SetSlotActivity(slotInfo.Active)
		s.metric.SetSlotCurrentLSN(float64(slotInfo.CurrentLSN))
		s.metric.SetSlotConfirmedFlushLSN(float64(slotInfo.ConfirmedFlushLSN))
		s.metric.SetSlotRetainedWALSize(float64(slotInfo.RetainedWALSize))
		s.metric.SetSlotLag(float64(slotInfo.Lag))

		s.logUpdater.UpdateXLogPos(slotInfo.CurrentLSN)

		logger.Debug("slot metrics", "info", slotInfo)
	}
}

func (s *Slot) Close() {
	s.ticker.Stop()
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
