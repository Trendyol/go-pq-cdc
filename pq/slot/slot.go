package slot

import (
	"context"
	goerrors "errors"
	"fmt"
	"strconv"
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

// failoverMinServerVersion is the first server_version_num with
// CREATE_REPLICATION_SLOT ... (FAILOVER true) and ALTER_REPLICATION_SLOT.
const failoverMinServerVersion = 170000

type XLogUpdater interface {
	UpdateXLogPos(l pq.LSN)
}

type Slot struct {
	conn            pq.Connection
	replicationConn pq.Connection
	metric          metric.Metric
	logUpdater      XLogUpdater
	ticker          *time.Ticker
	statusSQL       string
	cfg             Config
	mu              sync.Mutex
	closed          atomic.Bool
}

func NewSlot(replicationDSN, standardDSN string, cfg Config, m metric.Metric, updater XLogUpdater) *Slot {
	query := fmt.Sprintf("SELECT slot_name, slot_type, active, active_pid, restart_lsn, confirmed_flush_lsn, wal_status, PG_CURRENT_WAL_LSN() AS current_lsn FROM pg_replication_slots WHERE slot_name = '%s';", cfg.Name)

	return &Slot{
		cfg:             cfg,
		conn:            pq.NewConnectionTemplate(standardDSN),
		replicationConn: pq.NewConnectionTemplate(replicationDSN),
		statusSQL:       query,
		metric:          m,
		ticker:          time.NewTicker(time.Millisecond * cfg.SlotActivityCheckerInterval),
		logUpdater:      updater,
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

	if s.cfg.Failover {
		if err := s.checkFailoverSupportLocked(ctx); err != nil {
			return nil, err
		}
	}

	info, err := s.infoLocked(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorSlotIsNotExists) || !s.cfg.CreateIfNotExists {
			return nil, errors.Wrap(err, "replication slot info")
		}
	} else {
		logger.Warn("replication slot already exists")
		if s.cfg.Failover {
			if err := s.enableFailoverLocked(ctx, info); err != nil {
				return nil, err
			}
		}
		return info, nil
	}

	// Slot needs replication connection for CREATE_REPLICATION_SLOT command
	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", s.cfg.Name)
	if s.cfg.Failover {
		sql += " (FAILOVER true)"
	}
	if err := s.execReplicationCommand(ctx, sql); err != nil {
		return nil, errors.Wrap(err, "replication slot create")
	}

	logger.Info("replication slot created", "name", s.cfg.Name, "failover", s.cfg.Failover)

	return s.infoLocked(ctx)
}

// checkFailoverSupportLocked rejects slot.failover on servers older than 17
// with a clear error instead of a CREATE_REPLICATION_SLOT syntax error.
func (s *Slot) checkFailoverSupportLocked(ctx context.Context) error {
	raw, err := s.scalarLocked(ctx, "SHOW server_version_num")
	if err != nil {
		return errors.Wrap(err, "server version")
	}
	version, err := strconv.Atoi(raw)
	if err != nil {
		return errors.Wrapf(err, "server version %q", raw)
	}
	if version < failoverMinServerVersion {
		return errors.Newf("slot.failover requires PostgreSQL 17 or newer (server_version_num %d)", version)
	}
	return nil
}

// enableFailoverLocked turns FAILOVER on for an existing slot created without
// it. ALTER_REPLICATION_SLOT needs the slot to be free: while another
// walsender holds it (rolling deployment) the ALTER is skipped with a warning
// and retried by the next Create.
func (s *Slot) enableFailoverLocked(ctx context.Context, info *Info) error {
	failover, err := s.scalarLocked(ctx, fmt.Sprintf("SELECT failover FROM pg_replication_slots WHERE slot_name = '%s'", s.cfg.Name))
	if err != nil {
		return errors.Wrap(err, "replication slot failover flag")
	}
	if failover == "t" {
		return nil
	}
	if info.Active {
		logger.Warn("replication slot is active for another process, failover will be enabled once it is released", "name", s.cfg.Name, "activePID", info.ActivePID)
		return nil
	}
	if err := s.execReplicationCommand(ctx, fmt.Sprintf("ALTER_REPLICATION_SLOT %s (FAILOVER true)", s.cfg.Name)); err != nil {
		return errors.Wrap(err, "replication slot enable failover")
	}
	logger.Info("replication slot failover enabled", "name", s.cfg.Name)
	return nil
}

// scalarLocked runs sql on the regular connection and returns the first
// column of the first row as text.
func (s *Slot) scalarLocked(ctx context.Context, sql string) (string, error) {
	results, err := s.conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return "", err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return "", errors.Newf("no rows: %s", sql)
	}
	return string(results[0].Rows[0][0]), nil
}

// execReplicationCommand runs a replication-protocol command
// (CREATE_REPLICATION_SLOT, ALTER_REPLICATION_SLOT) on a short-lived
// replication connection.
func (s *Slot) execReplicationCommand(ctx context.Context, sql string) error {
	if err := s.replicationConn.Connect(ctx); err != nil {
		return errors.Wrap(err, "slot replication connect")
	}
	defer func() {
		_ = s.replicationConn.Close(ctx)
	}()

	resultReader := s.replicationConn.Exec(ctx, sql)
	if _, err := resultReader.ReadAll(); err != nil {
		return errors.Wrap(err, "result")
	}
	if err := resultReader.Close(); err != nil {
		return errors.Wrap(err, "result reader close")
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

		// NULL/empty LSN columns (e.g. confirmed_flush_lsn on a physical or
		// not-yet-reserved slot) must be skipped; ParseLSN("") returns a cryptic EOF.
		if v == nil || v == "" {
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
			lsn, err := pq.ParseLSN(v.(string))
			if err != nil {
				return nil, errors.Wrap(err, "parse restart_lsn")
			}
			slotInfo.RestartLSN = lsn
		case "confirmed_flush_lsn":
			lsn, err := pq.ParseLSN(v.(string))
			if err != nil {
				return nil, errors.Wrap(err, "parse confirmed_flush_lsn")
			}
			slotInfo.ConfirmedFlushLSN = lsn
		case "wal_status":
			slotInfo.WalStatus = v.(string)
		case "current_lsn":
			lsn, err := pq.ParseLSN(v.(string))
			if err != nil {
				return nil, errors.Wrap(err, "parse current_lsn")
			}
			slotInfo.CurrentLSN = lsn
		}
	}

	slotInfo.RetainedWALSize = subtractLSN(slotInfo.CurrentLSN, slotInfo.RestartLSN)
	slotInfo.Lag = subtractLSN(slotInfo.CurrentLSN, slotInfo.ConfirmedFlushLSN)

	return &slotInfo, nil
}

func subtractLSN(current, previous pq.LSN) pq.LSN {
	if current <= previous {
		return 0
	}
	return current - previous
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
