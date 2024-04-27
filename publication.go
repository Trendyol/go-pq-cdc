package dcp

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"time"
)

const (
	StandbyStatusUpdateByteID = 'r'
)

func DropPublication(ctx context.Context, conn *pgconn.PgConn, publicationName string) error {
	resultReader := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+publicationName)
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "drop publication result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "drop publication result reader close")
	}

	return nil
}

// TODO: Support all publication types [filtered, publish based, extract field, query based]

func CreatePublication(ctx context.Context, conn *pgconn.PgConn, publicationName string) error {
	resultReader := conn.Exec(ctx, "CREATE PUBLICATION "+publicationName+" FOR ALL TABLES;")
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "create publication result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "create publication result reader close")
	}

	return nil
}

func CreateReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) error {
	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", slotName)
	resultReader := conn.Exec(ctx, sql)
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "create replication slot result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "create replication slot result reader close")
	}

	return nil
}

func SendStandbyStatusUpdate(_ context.Context, conn *pgconn.PgConn, WALWritePosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, WALWritePosition)
	data = AppendUint64(data, WALWritePosition)
	data = AppendUint64(data, WALWritePosition)
	data = AppendUint64(data, timeToPgTime(time.Now()))
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func timeToPgTime(t time.Time) uint64 {
	return uint64(t.Unix()*1000000 + int64(t.Nanosecond())/1000 - microSecFromUnixEpochToY2K)
}
