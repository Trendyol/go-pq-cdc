package replication

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
)

type wireTupleCol struct {
	dataType uint8
	text     string
}

func wireTextCol(v string) wireTupleCol {
	return wireTupleCol{dataType: tuple.DataTypeText, text: v}
}

func wireToastCol() wireTupleCol {
	return wireTupleCol{dataType: tuple.DataTypeToast}
}

func wireNullCol() wireTupleCol {
	return wireTupleCol{dataType: tuple.DataTypeNull}
}

func encodeStreamStart(xid uint32, firstSegment uint8) []byte {
	b := make([]byte, 6)
	b[0] = byte(message.StreamStartByte)
	binary.BigEndian.PutUint32(b[1:5], xid)
	b[5] = firstSegment
	return b
}

func encodeStreamStop() []byte {
	return []byte{byte(message.StreamStopByte)}
}

func encodeStreamCommit(xid uint32, commitLSN, endLSN pq.LSN) []byte {
	b := make([]byte, 30)
	b[0] = byte(message.StreamCommitByte)
	binary.BigEndian.PutUint32(b[1:5], xid)
	b[5] = 0
	binary.BigEndian.PutUint64(b[6:14], uint64(commitLSN))
	binary.BigEndian.PutUint64(b[14:22], uint64(endLSN))
	binary.BigEndian.PutUint64(b[22:30], 0)
	return b
}

func encodeStreamAbort(xid, subXid uint32) []byte {
	b := make([]byte, 9)
	b[0] = byte(message.StreamAbortByte)
	binary.BigEndian.PutUint32(b[1:5], xid)
	binary.BigEndian.PutUint32(b[5:9], subXid)
	return b
}

func encodeRelation(streamed bool, xid, oid uint32, namespace, name string, replicaID uint8, cols []tuple.RelationColumn) []byte {
	var b []byte
	b = append(b, byte(message.RelationByte))
	if streamed {
		b = binary.BigEndian.AppendUint32(b, xid)
	}
	b = binary.BigEndian.AppendUint32(b, oid)
	b = append(b, namespace...)
	b = append(b, 0)
	b = append(b, name...)
	b = append(b, 0)
	b = append(b, replicaID)
	b = binary.BigEndian.AppendUint16(b, uint16(len(cols)))
	for _, c := range cols {
		b = append(b, c.Flags)
		b = append(b, c.Name...)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint32(b, c.DataType)
		b = binary.BigEndian.AppendUint32(b, c.TypeModifier)
	}
	return b
}

func encodeStreamedInsert(xid, oid uint32, cols ...wireTupleCol) []byte {
	var b []byte
	b = append(b, byte(message.InsertByte))
	b = binary.BigEndian.AppendUint32(b, xid)
	b = binary.BigEndian.AppendUint32(b, oid)
	b = append(b, format.InsertTupleDataType)
	b = appendTupleColumns(b, cols)
	return b
}

func encodeStreamedUpdate(xid, oid uint32, oldType uint8, oldCols, newCols []wireTupleCol) []byte {
	var b []byte
	b = append(b, byte(message.UpdateByte))
	b = binary.BigEndian.AppendUint32(b, xid)
	b = binary.BigEndian.AppendUint32(b, oid)
	if oldType != 0 {
		b = append(b, oldType)
		b = appendTupleColumns(b, oldCols)
	}
	b = append(b, format.UpdateTupleTypeNew)
	b = appendTupleColumns(b, newCols)
	return b
}

func encodeStreamedDelete(xid, oid uint32, oldType uint8, oldCols []wireTupleCol) []byte {
	var b []byte
	b = append(b, byte(message.DeleteByte))
	b = binary.BigEndian.AppendUint32(b, xid)
	b = binary.BigEndian.AppendUint32(b, oid)
	b = append(b, oldType)
	b = appendTupleColumns(b, oldCols)
	return b
}

func encodeStreamedTruncate(xid uint32, options uint8, oids ...uint32) []byte {
	var b []byte
	b = append(b, byte(message.TruncateByte))
	b = binary.BigEndian.AppendUint32(b, xid)
	b = binary.BigEndian.AppendUint32(b, uint32(len(oids)))
	b = append(b, options)
	for _, oid := range oids {
		b = binary.BigEndian.AppendUint32(b, oid)
	}
	return b
}

func encodeStreamedLogicalMsg(xid uint32, flags uint8, lsn pq.LSN, prefix string, content []byte) []byte {
	var b []byte
	b = append(b, byte(message.LogicalByte))
	b = binary.BigEndian.AppendUint32(b, xid)
	b = append(b, flags)
	b = binary.BigEndian.AppendUint64(b, uint64(lsn))
	b = append(b, prefix...)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(len(content)))
	b = append(b, content...)
	return b
}

func encodeXLogData(walStart, walEnd pq.LSN, serverTime time.Time, walData []byte) []byte {
	b := make([]byte, 24+len(walData))
	binary.BigEndian.PutUint64(b[0:8], uint64(walStart))
	binary.BigEndian.PutUint64(b[8:16], uint64(walEnd))
	pgTime := serverTime.UTC().UnixMicro() - microSecFromUnixEpochToY2K
	binary.BigEndian.PutUint64(b[16:24], uint64(pgTime))
	copy(b[24:], walData)
	return b
}

func appendTupleColumns(dst []byte, cols []wireTupleCol) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(cols)))
	for _, col := range cols {
		dst = append(dst, col.dataType)
		switch col.dataType {
		case tuple.DataTypeText, tuple.DataTypeBinary:
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(col.text)))
			dst = append(dst, col.text...)
		}
	}
	return dst
}
