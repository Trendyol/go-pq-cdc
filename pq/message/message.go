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

var streamedTransaction bool

func New(data []byte, serverTime time.Time, relation map[uint32]*format.Relation) (any, error) {
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
	case StreamStartByte:
		streamedTransaction = true
		return format.NewStreamStart(data)
	case StreamStopByte:
		streamedTransaction = false
		return &format.StreamStop{}, nil
	case StreamAbortByte:
		streamedTransaction = false
		return format.NewStreamAbort(data)
	case StreamCommitByte:
		streamedTransaction = false
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
