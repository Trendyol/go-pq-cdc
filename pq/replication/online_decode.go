package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrOnlineProtocol         = errors.New("invalid or unsupported online protocol-1 input")
	ErrOnlineTransactionLimit = errors.New("online transaction encoded byte limit exceeded")
	ErrOnlineMetadataLimit    = errors.New("online relation metadata encoded byte limit exceeded")
)

// OnlineChange ordinals are zero-based across all I/U/D messages in the source
// transaction, including control DML. Metadata never consumes an ordinal.
type OnlineChange struct {
	Message      any // *format.Insert, *format.Update, or *format.Delete
	Ordinal      uint32
	EncodedBytes int64 // pgoutput message bytes, including the message tag
}

// OnlineCommittedEnvelope is immutable after publication by the reader.
// EndLSN, not a row's WALStart or CommitLSN, is its checkpoint boundary.
// Empty/control-only transactions are retained so their visibility is gated too.
type OnlineCommittedEnvelope struct {
	Changes      []OnlineChange
	CommitLSN    pq.LSN
	EndLSN       pq.LSN
	XID          uint32
	EncodedBytes int64 // all pgoutput bytes B..C, including metadata; excludes CopyData framing
}

type onlineAssembler struct {
	types            *pgtype.Map
	relations        map[uint32]*format.Relation
	relationBytes    map[uint32]int64
	metadataBytes    int64
	metadataLimit    int64
	transactionLimit int64
	tx               *OnlineCommittedEnvelope
}

func newOnlineAssembler(transactionLimit, metadataLimit int64) *onlineAssembler {
	return &onlineAssembler{types: tuple.NewOnlineTypeMap(), relations: make(map[uint32]*format.Relation), relationBytes: make(map[uint32]int64), transactionLimit: transactionLimit, metadataLimit: metadataLimit}
}
func (a *onlineAssembler) inTransaction() bool { return a.tx != nil }

func (a *onlineAssembler) accept(xld XLogData) (*OnlineCommittedEnvelope, error) {
	b := xld.WALData
	if err := validateOnlineWire(b, a.relations); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOnlineProtocol, err)
	}
	tag := b[0]
	if tag == 'B' {
		if a.tx != nil {
			return nil, fmt.Errorf("%w: nested BEGIN", ErrOnlineProtocol)
		}
		begin, err := format.NewBegin(b)
		if err != nil {
			return nil, err
		}
		if begin.Xid == 0 || begin.FinalLSN == 0 {
			return nil, fmt.Errorf("%w: zero BEGIN xid/LSN", ErrOnlineProtocol)
		}
		a.tx = &OnlineCommittedEnvelope{XID: begin.Xid, CommitLSN: begin.FinalLSN}
	} else if a.tx == nil && tag != 'R' && tag != 'Y' {
		return nil, fmt.Errorf("%w: %q outside transaction", ErrOnlineProtocol, tag)
	}
	if a.tx != nil {
		if int64(len(b)) > a.transactionLimit-a.tx.EncodedBytes {
			return nil, ErrOnlineTransactionLimit
		}
		a.tx.EncodedBytes += int64(len(b))
	}
	switch tag {
	case 'B':
		return nil, nil
	case 'Y', 'O':
		return nil, nil // normal Type/Origin metadata: validated, no row ordinal
	case 'R':
		oid := binary.BigEndian.Uint32(b[1:])
		size := int64(len(b))
		next := a.metadataBytes - a.relationBytes[oid] + size
		if next > a.metadataLimit {
			return nil, ErrOnlineMetadataLimit
		}
		_, err := message.New(b, false, xld.ServerTime, a.relations)
		if err != nil {
			return nil, err
		}
		a.relationBytes[oid] = size
		a.metadataBytes = next
		return nil, nil
	case 'C':
		commit, err := format.NewCommit(b)
		if err != nil {
			return nil, err
		}
		if commit.CommitLSN != a.tx.CommitLSN || commit.TransactionEndLSN <= commit.CommitLSN {
			return nil, fmt.Errorf("%w: COMMIT LSN does not match BEGIN/end boundary", ErrOnlineProtocol)
		}
		a.tx.EndLSN = commit.TransactionEndLSN
		env := a.tx
		a.tx = nil
		return env, nil
	default:
		// Reuse tuple/TOAST wire parsing without running legacy value codecs.
		// OID 0 yields raw text; the shared online decoder below applies the
		// ORIGINAL column OIDs exactly once (including huge JSON numbers).
		oid := binary.BigEndian.Uint32(b[1:])
		rawRelation := *a.relations[oid]
		rawRelation.Columns = append([]tuple.RelationColumn(nil), rawRelation.Columns...)
		for i := range rawRelation.Columns {
			rawRelation.Columns[i].DataType = 0
		}
		decoded, err := message.New(b, false, xld.ServerTime, map[uint32]*format.Relation{oid: &rawRelation})
		if err != nil {
			return nil, fmt.Errorf("%w: decode %q: %v", ErrOnlineProtocol, tag, err)
		}
		switch m := decoded.(type) {
		case *format.Insert:
			m.LSN = xld.WALStart
			m.Decoded, err = tuple.DecodeOnlineData(a.types, m.TupleData, a.relations[m.OID].Columns)
		case *format.Update:
			m.LSN = xld.WALStart
			m.NewDecoded, err = tuple.DecodeOnlineData(a.types, m.NewTupleData, a.relations[m.OID].Columns)
			if err == nil && m.OldTupleData != nil {
				m.OldDecoded, err = tuple.DecodeOnlineData(a.types, m.OldTupleData, a.relations[m.OID].Columns)
			}
		case *format.Delete:
			m.LSN = xld.WALStart
			m.OldDecoded, err = tuple.DecodeOnlineData(a.types, m.OldTupleData, a.relations[m.OID].Columns)
		default:
			return nil, fmt.Errorf("%w: unexpected decoded type %T", ErrOnlineProtocol, decoded)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: full image: %v", ErrOnlineProtocol, err)
		}
		a.tx.Changes = append(a.tx.Changes, OnlineChange{Message: decoded, Ordinal: uint32(len(a.tx.Changes)), EncodedBytes: int64(len(b))})
		return nil, nil
	}
}

// Existing decoders intentionally accept some trailing bytes and assume parts
// of Relation/Tuple are present. Validate the complete protocol-1 shape before
// calling them, so malformed source input is an error rather than a skip/panic.
type onlineCursor struct {
	b   []byte
	pos int
	err error
}

func (c *onlineCursor) take(n int) []byte {
	if c.err != nil {
		return nil
	}
	if n < 0 || n > len(c.b)-c.pos {
		c.err = errors.New("truncated message")
		return nil
	}
	b := c.b[c.pos : c.pos+n]
	c.pos += n
	return b
}
func (c *onlineCursor) byte() byte {
	b := c.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (c *onlineCursor) u16() uint16 {
	b := c.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (c *onlineCursor) u32() uint32 {
	b := c.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (c *onlineCursor) str() {
	if c.err != nil {
		return
	}
	n := bytes.IndexByte(c.b[c.pos:], 0)
	if n < 0 {
		c.err = errors.New("unterminated metadata string")
		return
	}
	c.take(n + 1)
}
func (c *onlineCursor) tuple(expected int, tag byte) {
	if c.byte() != tag && c.err == nil {
		c.err = errors.New("invalid tuple tag")
		return
	}
	count := int(c.u16())
	if c.err != nil {
		return
	}
	if count != expected {
		c.err = errors.New("tuple column count does not match relation")
		return
	}
	for i := 0; i < count && c.err == nil; i++ {
		switch c.byte() {
		case 'n', 'u':
		case 't':
			n := c.u32()
			if uint64(n) > uint64(len(c.b)-c.pos) {
				c.err = errors.New("truncated column data")
				return
			}
			c.take(int(n))
		default:
			c.err = errors.New("unsupported tuple column kind (text protocol required)")
		}
	}
}
func validateOnlineWire(b []byte, relations map[uint32]*format.Relation) error {
	c := onlineCursor{b: b}
	tag := c.byte()
	if c.err != nil {
		return c.err
	}
	switch tag {
	case 'B':
		c.take(20)
	case 'C':
		if c.byte() != 0 {
			return errors.New("unsupported COMMIT flags")
		}
		c.take(24)
	case 'R':
		if c.u32() == 0 && c.err == nil {
			return errors.New("zero relation OID")
		}
		c.str()
		c.str()
		id := c.byte()
		if id != 'd' && id != 'n' && id != 'f' && id != 'i' {
			return errors.New("invalid replica identity")
		}
		n := int(c.u16())
		names := make(map[string]bool)
		for i := 0; i < n && c.err == nil; i++ {
			if c.byte() > 1 {
				return errors.New("invalid relation column flags")
			}
			start := c.pos
			c.str()
			if c.err != nil {
				break
			}
			name := string(b[start : c.pos-1])
			if name == "" || names[name] {
				return errors.New("empty or duplicate relation column name")
			}
			names[name] = true
			c.take(8)
		}
	case 'Y':
		c.take(4)
		c.str()
		c.str()
	case 'O':
		c.take(8)
		c.str()
	case 'I', 'U', 'D':
		rel := relations[c.u32()]
		if c.err != nil {
			return c.err
		}
		if rel == nil {
			return errors.New("relation not found")
		}
		n := len(rel.Columns)
		switch tag {
		case 'I':
			c.tuple(n, 'N')
		case 'U':
			kind := c.byte()
			if c.err != nil {
				break
			}
			c.pos--
			if kind == 'K' || kind == 'O' {
				c.tuple(n, kind)
			} else if kind != 'N' {
				return errors.New("invalid UPDATE tuple tag")
			}
			c.tuple(n, 'N')
		case 'D':
			kind := c.byte()
			if c.err != nil {
				break
			}
			c.pos--
			if kind != 'K' && kind != 'O' {
				return errors.New("invalid DELETE tuple tag")
			}
			c.tuple(n, kind)
		}
	case 'T':
		return errors.New("TRUNCATE is unsupported in online mode; stop before checkpoint")
	default:
		return fmt.Errorf("unsupported required message %q (protocol 2/two-phase/logical messages are not accepted)", tag)
	}
	if c.err != nil {
		return c.err
	}
	if c.pos != len(b) {
		return errors.New("trailing message bytes")
	}
	return nil
}
