package format

import (
	"encoding/binary"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"time"
)

type Begin struct {
	//FinalLSN is the final LSN of the transaction.
	FinalLSN pq.LSN
	// CommitTime is the commit timestamp of the transaction.
	CommitTime time.Time
	// Xid of the transaction.
	Xid uint32
}

/*
0 = {uint8} 66
1 = {uint8} 0
2 = {uint8} 0
3 = {uint8} 0
4 = {uint8} 0
5 = {uint8} 1
6 = {uint8} 150
7 = {uint8} 109
8 = {uint8} 216
9 = {uint8} 0
10 = {uint8} 2
11 = {uint8} 233
12 = {uint8} 255
13 = {uint8} 174
14 = {uint8} 61
15 = {uint8} 153
16 = {uint8} 141
17 = {uint8} 0
18 = {uint8} 0
19 = {uint8} 2
20 = {uint8} 236
*/
func NewBegin(data []byte) (*Begin, error) {
	msg := &Begin{}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (b *Begin) decode(data []byte) error {
	skipByte := 1

	if len(data) < 20 {
		return errors.Newf("begin message length must be at least 20 byte, but got %d", len(data))
	}

	b.FinalLSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8
	b.CommitTime = time.Unix(int64(binary.BigEndian.Uint64(data[skipByte:])), 0)
	skipByte += 8
	b.Xid = binary.BigEndian.Uint32(data[skipByte:])

	return nil
}
