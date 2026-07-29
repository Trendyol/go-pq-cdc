package message

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
)

const (
	StreamAbortByte  Type = 'A'
	BeginByte        Type = 'B'
	CommitByte       Type = 'C'
	DeleteByte       Type = 'D'
	StreamStopByte   Type = 'E'
	InsertByte       Type = 'I'
	LogicalByte      Type = 'M'
	OriginByte       Type = 'O'
	RelationByte     Type = 'R'
	StreamStartByte  Type = 'S'
	TruncateByte     Type = 'T'
	UpdateByte       Type = 'U'
	TypeByte         Type = 'Y'
	StreamCommitByte Type = 'c'
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

var ErrorByteNotSupported = errors.New("message byte not supported")

type Type uint8

// New decodes a single pgoutput logical replication message. streamedTransaction
// reports whether the stream is currently inside a streamed in-progress
// transaction chunk (proto v2), in which case DML and Relation messages carry an
// XID prefix. The caller owns that state (it flips on the StreamStart/StreamStop/
// StreamCommit/StreamAbort messages this function returns) — it must NOT be
// process-global, since one process may run several replication streams.
func New(data []byte, streamedTransaction bool, serverTime time.Time, relation map[uint32]*format.Relation) (any, error) {
	switch Type(data[0]) {
	case BeginByte:
		return format.NewBegin(data)
	case CommitByte:
		return format.NewCommit(data)
	case InsertByte:
		return format.NewInsert(data, streamedTransaction, relation, serverTime)
	case UpdateByte:
		return format.NewUpdate(data, streamedTransaction, relation, serverTime)
	case DeleteByte:
		return format.NewDelete(data, streamedTransaction, relation, serverTime)
	case TruncateByte:
		return format.NewTruncate(data, streamedTransaction, relation, serverTime)
	case StreamStartByte:
		return format.NewStreamStart(data)
	case StreamStopByte:
		return &format.StreamStop{}, nil
	case StreamAbortByte:
		return format.NewStreamAbort(data)
	case StreamCommitByte:
		return format.NewStreamCommit(data)
	case RelationByte:
		msg, err := format.NewRelation(data, streamedTransaction)
		if err == nil {
			relation[msg.OID] = msg
		}
		return msg, err
	default:
		return nil, errors.Wrap(ErrorByteNotSupported, string(data[0]))
	}
}
