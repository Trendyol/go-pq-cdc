package message

import (
	. "github.com/Trendyol/go-pq-cdc/pq/message/format"
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

var ByteNotSupported = errors.New("message byte not supported")

type Type uint8

var streamedTransaction bool

func New(data []byte, relation map[uint32]*Relation) (any, error) {
	switch Type(data[0]) {
	case InsertByte:
		return NewInsert(data, streamedTransaction, relation)
	case UpdateByte:
		return NewUpdate(data, streamedTransaction, relation)
	case DeleteByte:
		return NewDelete(data, streamedTransaction, relation)
	case StreamStopByte, StreamAbortByte, StreamCommitByte:
		streamedTransaction = false
		return nil, nil
	case RelationByte:
		msg, err := NewRelation(data, streamedTransaction)
		if err == nil {
			relation[msg.OID] = msg
		}
		return msg, err
	case StreamStartByte:
		streamedTransaction = true
		return nil, nil
	default:
		// return nil, errors.Wrap(ByteNotSupported, string(data[0]))
		return nil, nil
	}
}
