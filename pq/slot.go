package pq

import (
	"context"
	"fmt"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	Logical  SlotType = "logical"
	Physical SlotType = "physical"
)

var typeMap = pgtype.NewMap()

type SlotType string

type Slot struct {
	conn      Connection
	statusSQL string
}

type SlotInfo struct {
	Name              string   `json:"name"`
	Type              SlotType `json:"type"`
	Active            bool     `json:"active"`
	ActivePID         int32    `json:"activePID"`
	RestartLSN        string   `json:"restartLSN"`
	ConfirmedFlushLSN string   `json:"confirmedFlushLSN"`
	WalStatus         string   `json:"walStatus"`
	CurrentLSN        string   `json:"currentLSN"`
	RetainedWALSize   string   `json:"retainedWALSize"` // currentLSN - restartLSN
	Lag               string   `json:"lag"`             // currentLSN -  confirmedLSN
}

func NewSlot(slotName string, conn Connection) (*Slot, error) {
	query := fmt.Sprintf("SELECT slot_name, slot_type, active, active_pid, restart_lsn, confirmed_flush_lsn, wal_status, PG_CURRENT_WAL_LSN() AS current_lsn, PG_SIZE_PRETTY(PG_WAL_LSN_DIFF(PG_CURRENT_WAL_LSN(), restart_lsn))AS retained_walsize, PG_SIZE_PRETTY(PG_WAL_LSN_DIFF(PG_CURRENT_WAL_LSN(), confirmed_flush_lsn)) AS subscriber_lag FROM pg_replication_slots WHERE slot_name = '%s';", slotName)

	return &Slot{
		conn:      conn,
		statusSQL: query,
	}, nil
}

func (s *Slot) GetInfo(ctx context.Context) (*SlotInfo, error) {
	resultReader := s.conn.Exec(ctx, s.statusSQL)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "replication slot info result")
	}

	if len(results) == 0 {
		return nil, errors.New("replication slot info result empty")
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

func decodeSlotInfoResult(result *pgconn.Result) (*SlotInfo, error) {
	var slotInfo SlotInfo
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
			slotInfo.Type = SlotType(v.(string))
		case "active":
			slotInfo.Active = v.(bool)
		case "active_pid":
			slotInfo.ActivePID = v.(int32)
		case "restart_lsn":
			slotInfo.RestartLSN = v.(string)
		case "confirmed_flush_lsn":
			slotInfo.ConfirmedFlushLSN = v.(string)
		case "wal_status":
			slotInfo.WalStatus = v.(string)
		case "current_lsn":
			slotInfo.CurrentLSN = v.(string)
		case "retained_walsize":
			slotInfo.RetainedWALSize = v.(string)
		case "subscriber_lag":
			slotInfo.Lag = v.(string)
		}
	}

	return &slotInfo, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
