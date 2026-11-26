package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

// ErrStreamSpillDiskLimitExceeded is returned when spilling or appending to a
// spilled transaction would exceed slot.spill.maxDiskBytes.
var ErrStreamSpillDiskLimitExceeded = errors.New("stream spill disk limit exceeded")

const (
	frameKindWire    byte = 1
	frameKindRelBind byte = 2

	wireFrameHeaderSize = 21 // [kind:1][walStart:8][serverTimeUnixMicro:8][dataLen:4]
	relBindFrameSize    = 9  // [kind:1][oid:4][relIdx:4]
	spillFileBufSize    = 64 * 1024
)

// streamTxBuffer accumulates raw pgoutput WAL frames from streaming
// in-progress transactions (proto v2).
//
// Frames stay in a flat, pointer-free byte slice per transaction while within
// the global memory budget, and tuples are decoded lazily on STREAM COMMIT.
// When slot.spill.enabled is true and totalMemBytes exceeds maxMemoryBytes,
// the largest in-memory transaction(s) are spilled to temporary files using the
// exact same binary frame layout, keeping at most one file descriptor open.
type streamTxBuffer struct {
	txns           map[uint32]*streamTx
	metric         metric.Metric
	sessionDir     string
	spillCfg       slot.StreamSpillConfig
	totalMemBytes  int64
	totalDiskBytes int64
	fileSeq        uint64
	activeXid      uint32
	streaming      bool
	closed         bool
}

type streamTx struct {
	memBuf   []byte
	rels     []*format.Relation
	subXacts []subXact
	filePath string
	boundRel map[uint32]*format.Relation
	file     *os.File
	bw       *bufio.Writer
	size     int64
	xid      uint32
	spilled  bool
}

type subXact struct {
	offset int64
	xid    uint32
}

type relBind struct {
	oid    uint32
	relIdx uint32
}

func newStreamTxBuffer(cfg slot.StreamSpillConfig, slotName string, m metric.Metric) (*streamTxBuffer, error) {
	sb := &streamTxBuffer{
		spillCfg: cfg,
		metric:   m,
	}
	if !cfg.Enabled {
		return sb, nil
	}

	dir := strings.TrimSpace(cfg.SpillDir)
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create stream spill dir %q: %w", dir, err)
	}
	sessionDir, err := os.MkdirTemp(dir, "go-pq-cdc-"+sanitizeSlotName(slotName)+"-")
	if err != nil {
		return nil, fmt.Errorf("create stream spill session dir in %q: %w", dir, err)
	}
	sb.sessionDir = sessionDir
	return sb, nil
}

func sanitizeSlotName(name string) string {
	if name == "" {
		return "slot"
	}
	var b strings.Builder
	b.Grow(len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if isAlphaNum || c == '_' || c == '-' {
			b.WriteByte(c)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func (s *streamTxBuffer) hasOpenTxns() bool {
	return len(s.txns) > 0
}

func (s *streamTxBuffer) startTx(xid uint32) error {
	if s.streaming && s.activeXid != xid {
		if err := s.closeActiveWriter(); err != nil {
			return err
		}
	}
	if s.txns == nil {
		s.txns = make(map[uint32]*streamTx)
	}
	if _, ok := s.txns[xid]; !ok {
		s.txns[xid] = &streamTx{xid: xid}
	}
	s.activeXid = xid
	s.streaming = true
	s.updateActiveTxnMetric()
	return nil
}

func (s *streamTxBuffer) stopTx() error {
	s.streaming = false
	return s.closeActiveWriter()
}

func (s *streamTxBuffer) closeActiveWriter() error {
	if s.txns == nil {
		return nil
	}
	tx := s.txns[s.activeXid]
	if tx == nil {
		return nil
	}
	return tx.closeWriter()
}

func (s *streamTxBuffer) appendRaw(walData []byte, walStart pq.LSN, serverTime time.Time, relations map[uint32]*format.Relation) error {
	if len(walData) == 0 {
		return nil
	}
	msgType := message.Type(walData[0])
	switch msgType {
	case message.OriginByte, message.TypeByte:
		return nil
	case message.InsertByte, message.UpdateByte, message.DeleteByte, message.TruncateByte, message.RelationByte, message.LogicalByte:
	default:
		logger.Warn("wal data message parsing error, message dropped",
			"error", fmt.Errorf("%w: %s", message.ErrorByteNotSupported, string(walData[0])),
			"walStart", walStart)
		return nil
	}
	if len(walData) < 5 {
		logger.Warn("wal data message parsing error, message dropped",
			"error", fmt.Errorf("streamed message too short: %d bytes", len(walData)),
			"walStart", walStart)
		return nil
	}

	tx := s.txns[s.activeXid]
	if tx == nil {
		return nil
	}

	subXid := binary.BigEndian.Uint32(walData[1:5])
	if msgType == message.RelationByte {
		return s.appendRelation(tx, subXid, walData, walStart, serverTime, relations)
	}

	tx.recordSubXact(subXid)
	binds := prepareRelationBinds(tx, walData, relations)
	if err := s.writeFrames(tx, binds, walStart, serverTime, walData); err != nil {
		return err
	}
	s.updateByteMetrics()
	return nil
}

func (s *streamTxBuffer) appendRelation(
	tx *streamTx,
	subXid uint32,
	walData []byte,
	walStart pq.LSN,
	serverTime time.Time,
	relations map[uint32]*format.Relation,
) error {
	rel, err := format.NewRelation(walData, true)
	if err != nil {
		logger.Warn("wal data message parsing error, message dropped", "error", err, "walStart", walStart)
		return nil
	}
	if relations != nil {
		relations[rel.OID] = rel
	}
	tx.recordSubXact(subXid)
	if err := s.writeFrames(tx, nil, walStart, serverTime, walData); err != nil {
		return err
	}
	if tx.boundRel == nil {
		tx.boundRel = make(map[uint32]*format.Relation)
	}
	tx.boundRel[rel.OID] = rel
	s.updateByteMetrics()
	return nil
}

func prepareRelationBinds(tx *streamTx, walData []byte, relations map[uint32]*format.Relation) []relBind {
	if relations == nil {
		return nil
	}
	var oidStack [1]uint32
	oids, ok := extractStreamedRelationOIDs(walData, &oidStack)
	if !ok {
		return nil
	}

	var binds []relBind
	for _, oid := range oids {
		rel := relations[oid]
		if rel == nil || tx.boundRel[oid] == rel {
			continue
		}
		if tx.boundRel == nil {
			tx.boundRel = make(map[uint32]*format.Relation)
		}
		tx.boundRel[oid] = rel
		binds = append(binds, relBind{
			oid:    oid,
			relIdx: tx.internRel(rel),
		})
	}
	return binds
}

func extractStreamedRelationOIDs(walData []byte, single *[1]uint32) ([]uint32, bool) {
	switch message.Type(walData[0]) {
	case message.InsertByte, message.UpdateByte, message.DeleteByte:
		if len(walData) < 9 {
			return nil, false
		}
		single[0] = binary.BigEndian.Uint32(walData[5:9])
		return single[:1], true
	case message.TruncateByte:
		if len(walData) < 10 {
			return nil, false
		}
		relCount := binary.BigEndian.Uint32(walData[5:9])
		avail := uint32((len(walData) - 10) / 4)
		if relCount == 0 || relCount > avail {
			return nil, false
		}
		if relCount == 1 {
			single[0] = binary.BigEndian.Uint32(walData[10:14])
			return single[:1], true
		}
		oids := make([]uint32, relCount)
		off := 10
		for i := range oids {
			oids[i] = binary.BigEndian.Uint32(walData[off : off+4])
			off += 4
		}
		return oids, true
	default:
		return nil, false
	}
}

func (s *streamTxBuffer) writeFrames(tx *streamTx, binds []relBind, walStart pq.LSN, serverTime time.Time, walData []byte) error {
	delta := int64(len(binds)*relBindFrameSize + wireFrameHeaderSize + len(walData))
	if tx.spilled {
		return s.writeFramesDisk(tx, binds, walStart, serverTime, walData, delta)
	}

	tx.memBuf = appendBindFrames(tx.memBuf, binds)
	tx.memBuf = appendWireFrame(tx.memBuf, walStart, serverTime, walData)
	tx.size += delta
	s.totalMemBytes += delta

	if s.spillCfg.Enabled && s.totalMemBytes > s.spillCfg.MaxMemoryBytes {
		return s.evictToBudget()
	}
	return nil
}

func (s *streamTxBuffer) writeFramesDisk(tx *streamTx, binds []relBind, walStart pq.LSN, serverTime time.Time, walData []byte, delta int64) error {
	if s.spillCfg.MaxDiskBytes > 0 && s.totalDiskBytes+delta > s.spillCfg.MaxDiskBytes {
		return ErrStreamSpillDiskLimitExceeded
	}
	if err := s.ensureSpillWriter(tx); err != nil {
		return err
	}

	var bindBuf [relBindFrameSize]byte
	for _, b := range binds {
		encodeRelBindFrame(bindBuf[:], b)
		if _, err := tx.bw.Write(bindBuf[:]); err != nil {
			return fmt.Errorf("write spill relation bind frame: %w", err)
		}
	}

	var hdr [wireFrameHeaderSize]byte
	encodeWireHeader(hdr[:], walStart, serverTime, len(walData))
	if _, err := tx.bw.Write(hdr[:]); err != nil {
		return fmt.Errorf("write spill wire frame header: %w", err)
	}
	if _, err := tx.bw.Write(walData); err != nil {
		return fmt.Errorf("write spill wire frame payload: %w", err)
	}

	tx.size += delta
	s.totalDiskBytes += delta
	if s.metric != nil {
		s.metric.StreamingSpillBytesAdd(delta)
	}
	return nil
}

func (s *streamTxBuffer) ensureSpillWriter(tx *streamTx) error {
	if tx.bw != nil {
		return nil
	}
	f, err := os.OpenFile(tx.filePath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen stream spill file %q: %w", tx.filePath, err)
	}
	tx.file = f
	tx.bw = bufio.NewWriterSize(f, spillFileBufSize)
	return nil
}

func (s *streamTxBuffer) evictToBudget() error {
	for s.totalMemBytes > s.spillCfg.MaxMemoryBytes {
		victim := s.pickEvictionVictim()
		if victim == nil {
			return nil
		}
		if err := s.spillTxToDisk(victim); err != nil {
			return err
		}
	}
	return nil
}

func (s *streamTxBuffer) pickEvictionVictim() *streamTx {
	var victim *streamTx
	for _, tx := range s.txns {
		if tx.spilled || len(tx.memBuf) == 0 {
			continue
		}
		if victim == nil || len(tx.memBuf) > len(victim.memBuf) ||
			(len(tx.memBuf) == len(victim.memBuf) && tx.xid < victim.xid) {
			victim = tx
		}
	}
	return victim
}

func (s *streamTxBuffer) spillTxToDisk(tx *streamTx) error {
	memBytes := int64(len(tx.memBuf))
	if s.spillCfg.MaxDiskBytes > 0 && s.totalDiskBytes+memBytes > s.spillCfg.MaxDiskBytes {
		return ErrStreamSpillDiskLimitExceeded
	}

	s.fileSeq++
	path := filepath.Join(s.sessionDir, fmt.Sprintf("tx-%d-%d.wal", tx.xid, s.fileSeq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create stream spill file %q: %w", path, err)
	}

	if err := writeFull(f, tx.memBuf); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write stream spill file %q: %w", path, err)
	}

	keepOpen := s.streaming && tx.xid == s.activeXid && (s.totalMemBytes-memBytes) <= s.spillCfg.MaxMemoryBytes
	if keepOpen {
		tx.file = f
		tx.bw = bufio.NewWriterSize(f, spillFileBufSize)
	} else if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close stream spill file %q: %w", path, err)
	}

	tx.memBuf = nil
	tx.filePath = path
	tx.spilled = true
	s.totalMemBytes -= memBytes
	s.totalDiskBytes += memBytes

	if s.metric != nil {
		s.metric.StreamingSpillTransactionsIncrement()
		s.metric.StreamingSpillBytesAdd(memBytes)
	}
	logger.Info("streamed transaction spilled to disk", "xid", tx.xid, "bytes", memBytes, "path", path)
	return nil
}

func (s *streamTxBuffer) abortTx(xid, subXid uint32) error {
	s.streaming = false
	if err := s.closeActiveWriter(); err != nil {
		return err
	}

	tx := s.txns[xid]
	if tx == nil {
		return nil
	}

	if subXid == xid {
		return s.discardTx(tx)
	}
	return s.truncateSubTx(tx, subXid)
}

func (s *streamTxBuffer) discardTx(tx *streamTx) error {
	tx.discardOpenFile()
	if tx.spilled {
		if err := os.Remove(tx.filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove aborted spill file %q: %w", tx.filePath, err)
		}
		s.totalDiskBytes -= tx.size
	} else {
		s.totalMemBytes -= int64(len(tx.memBuf))
	}

	delete(s.txns, tx.xid)
	if s.metric != nil {
		s.metric.StreamingAbortedTransactionsIncrement()
	}
	s.updateActiveTxnMetric()
	s.updateByteMetrics()
	return nil
}

func (s *streamTxBuffer) truncateSubTx(tx *streamTx, subXid uint32) error {
	idx := -1
	for i := len(tx.subXacts) - 1; i >= 0; i-- {
		if tx.subXacts[i].xid == subXid {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}

	targetOffset := tx.subXacts[idx].offset
	dropped := tx.size - targetOffset

	if tx.spilled {
		if err := tx.closeWriter(); err != nil {
			return err
		}
		if err := truncateFile(tx.filePath, targetOffset); err != nil {
			return err
		}
		s.totalDiskBytes -= dropped
	} else {
		if targetOffset == 0 {
			tx.memBuf = nil
		} else {
			tx.memBuf = tx.memBuf[:targetOffset]
			if cap(tx.memBuf) > spillFileBufSize && int64(cap(tx.memBuf)) > targetOffset*4 {
				tx.memBuf = bytes.Clone(tx.memBuf)
			}
		}
		s.totalMemBytes -= dropped
	}

	tx.size = targetOffset
	tx.subXacts = tx.subXacts[:idx]
	clear(tx.boundRel)
	s.updateByteMetrics()
	logger.Debug("truncated streamed subtransaction", "xid", tx.xid, "subXid", subXid, "offset", targetOffset, "spilled", tx.spilled)
	return nil
}

func (s *streamTxBuffer) flushTx(xid uint32, send func(*Message) bool, commitLSN, endLSN pq.LSN) error {
	s.streaming = false
	if err := s.closeActiveWriter(); err != nil {
		return err
	}

	tx := s.txns[xid]
	if tx == nil {
		return nil
	}

	start := time.Now()
	completed, err := s.replayTx(tx, send, commitLSN, endLSN)
	if err != nil {
		return err
	}
	if !completed {
		return nil
	}

	if tx.spilled {
		if err := os.Remove(tx.filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove committed spill file %q: %w", tx.filePath, err)
		}
		s.totalDiskBytes -= tx.size
	} else {
		s.totalMemBytes -= int64(len(tx.memBuf))
	}

	delete(s.txns, xid)
	s.updateActiveTxnMetric()
	s.updateByteMetrics()
	logger.Debug("replayed streamed transaction",
		"xid", xid,
		"spilled", tx.spilled,
		"bytes", tx.size,
		"duration", time.Since(start))
	return nil
}

type replayState struct {
	replayRel  map[uint32]*format.Relation
	prev       *Message
	payloadBuf []byte
	hdrBuf     [wireFrameHeaderSize - 1]byte
	bindBuf    [relBindFrameSize - 1]byte
	bytesRead  int64
}

func (s *streamTxBuffer) replayTx(tx *streamTx, send func(*Message) bool, commitLSN, endLSN pq.LSN) (bool, error) {
	reader, closeReader, err := openTxFrameReader(tx)
	if err != nil {
		return false, err
	}
	defer closeReader()

	st := replayState{replayRel: make(map[uint32]*format.Relation)}
	for {
		kind, err := reader.ReadByte()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false, fmt.Errorf("read stream frame kind: %w", err)
		}
		st.bytesRead++

		cont, err := replayFrameStep(reader, kind, tx, &st, send, commitLSN)
		if err != nil || !cont {
			return false, err
		}
	}

	if st.bytesRead != tx.size {
		return false, fmt.Errorf("truncated stream spill data: read %d of %d bytes", st.bytesRead, tx.size)
	}
	if st.prev != nil {
		st.prev.walStart = int64(endLSN)
		if !send(st.prev) {
			return false, nil
		}
	}
	return true, nil
}

func replayFrameStep(
	reader *bufio.Reader,
	kind byte,
	tx *streamTx,
	st *replayState,
	send func(*Message) bool,
	commitLSN pq.LSN,
) (bool, error) {
	switch kind {
	case frameKindRelBind:
		consumed, err := readRelBindFrame(reader, st.bindBuf[:], tx.size-st.bytesRead, tx, st.replayRel)
		if err != nil {
			return false, err
		}
		st.bytesRead += consumed
		return true, nil

	case frameKindWire:
		msg, nextBuf, consumed, err := readAndDecodeWireFrame(reader, st.hdrBuf[:], st.payloadBuf, tx.size-st.bytesRead, st.replayRel, tx.xid, commitLSN)
		st.payloadBuf = nextBuf
		if err != nil {
			return false, err
		}
		st.bytesRead += consumed
		if msg == nil {
			return true, nil
		}
		if st.prev != nil && !send(st.prev) {
			return false, nil
		}
		st.prev = msg
		return true, nil

	default:
		return false, fmt.Errorf("unknown stream frame kind %d", kind)
	}
}

func readRelBindFrame(reader *bufio.Reader, bindBuf []byte, remaining int64, tx *streamTx, replayRel map[uint32]*format.Relation) (int64, error) {
	const bodyBytes = int64(relBindFrameSize - 1)
	if remaining < bodyBytes {
		return 0, fmt.Errorf("truncated stream spill data: expected %d relation bind bytes, %d remaining", bodyBytes, remaining)
	}
	if _, err := io.ReadFull(reader, bindBuf); err != nil {
		return 0, fmt.Errorf("read relation bind frame: %w", err)
	}
	oid := binary.BigEndian.Uint32(bindBuf[0:4])
	rawRelIdx := binary.BigEndian.Uint32(bindBuf[4:8])
	if uint64(rawRelIdx) >= uint64(len(tx.rels)) {
		return 0, fmt.Errorf("invalid relation index %d (rels=%d)", rawRelIdx, len(tx.rels))
	}
	replayRel[oid] = tx.rels[rawRelIdx]
	return bodyBytes, nil
}

func readAndDecodeWireFrame(
	reader *bufio.Reader,
	hdrBuf []byte,
	payloadBuf []byte,
	remaining int64,
	replayRel map[uint32]*format.Relation,
	xid uint32,
	commitLSN pq.LSN,
) (*Message, []byte, int64, error) {
	const hdrBytes = int64(wireFrameHeaderSize - 1)
	if remaining < hdrBytes {
		return nil, payloadBuf, 0, fmt.Errorf("truncated wire frame header: %d bytes remaining", remaining)
	}
	if _, err := io.ReadFull(reader, hdrBuf); err != nil {
		return nil, payloadBuf, 0, fmt.Errorf("read wire frame header: %w", err)
	}
	walStart := pq.LSN(binary.BigEndian.Uint64(hdrBuf[0:8]))
	serverMicro := int64(binary.BigEndian.Uint64(hdrBuf[8:16]))
	rawDataLen := binary.BigEndian.Uint32(hdrBuf[16:20])
	if uint64(rawDataLen) > uint64(remaining-hdrBytes) || uint64(rawDataLen) > uint64(math.MaxInt) {
		return nil, payloadBuf, 0, fmt.Errorf("invalid wire frame payload length %d (remaining %d)", rawDataLen, remaining-hdrBytes)
	}
	dataLen := int(rawDataLen)

	if cap(payloadBuf) < dataLen {
		payloadBuf = make([]byte, dataLen)
	} else {
		payloadBuf = payloadBuf[:dataLen]
	}
	if _, err := io.ReadFull(reader, payloadBuf); err != nil {
		return nil, payloadBuf, 0, fmt.Errorf("read wire frame payload: %w", err)
	}
	consumed := hdrBytes + int64(dataLen)

	serverTime := time.UnixMicro(serverMicro).UTC()
	decodedMsg, err := message.New(payloadBuf, true, serverTime, replayRel)
	if err != nil {
		logger.Warn("wal data message parsing error, message dropped", "error", err, "walStart", walStart)
		return nil, payloadBuf, consumed, nil
	}
	if decodedMsg == nil {
		return nil, payloadBuf, consumed, nil
	}

	stampMessageLSN(decodedMsg, walStart)
	return &Message{
		message:   decodedMsg,
		walStart:  int64(walStart),
		xid:       xid,
		commitLSN: commitLSN,
	}, payloadBuf, consumed, nil
}

func openTxFrameReader(tx *streamTx) (*bufio.Reader, func(), error) {
	if !tx.spilled {
		return bufio.NewReader(bytes.NewReader(tx.memBuf)), func() {}, nil
	}
	f, err := os.Open(tx.filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open stream spill file %q: %w", tx.filePath, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("stat stream spill file %q: %w", tx.filePath, err)
	}
	if info.Size() != tx.size {
		_ = f.Close()
		return nil, nil, fmt.Errorf("truncated stream spill file %q: expected %d bytes, got %d", tx.filePath, tx.size, info.Size())
	}
	return bufio.NewReaderSize(f, spillFileBufSize), func() { _ = f.Close() }, nil
}

func (s *streamTxBuffer) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	for _, tx := range s.txns {
		tx.discardOpenFile()
	}
	clear(s.txns)
	s.totalMemBytes = 0
	s.totalDiskBytes = 0
	s.activeXid = 0
	s.streaming = false

	if s.sessionDir != "" {
		if err := os.RemoveAll(s.sessionDir); err != nil {
			errs = append(errs, fmt.Errorf("remove stream spill dir %q: %w", s.sessionDir, err))
		}
		s.sessionDir = ""
	}
	s.updateActiveTxnMetric()
	s.updateByteMetrics()
	return errors.Join(errs...)
}

func (s *streamTxBuffer) updateActiveTxnMetric() {
	if s.metric != nil {
		s.metric.SetStreamingActiveTransactions(len(s.txns))
	}
}

func (s *streamTxBuffer) updateByteMetrics() {
	if s.metric != nil {
		s.metric.SetStreamingBufferBytes(s.totalMemBytes)
		s.metric.SetStreamingSpilledBytes(s.totalDiskBytes)
	}
}

func (tx *streamTx) recordSubXact(subXid uint32) {
	if subXid == tx.xid || tx.hasSubXact(subXid) {
		return
	}
	tx.subXacts = append(tx.subXacts, subXact{
		offset: tx.size,
		xid:    subXid,
	})
}

func (tx *streamTx) hasSubXact(xid uint32) bool {
	for i := len(tx.subXacts) - 1; i >= 0; i-- {
		if tx.subXacts[i].xid == xid {
			return true
		}
	}
	return false
}

func (tx *streamTx) internRel(rel *format.Relation) uint32 {
	for i := len(tx.rels) - 1; i >= 0; i-- {
		if tx.rels[i] == rel {
			return uint32(i)
		}
	}
	tx.rels = append(tx.rels, rel)
	return uint32(len(tx.rels) - 1)
}

func (tx *streamTx) closeWriter() error {
	if tx.file == nil {
		tx.bw = nil
		return nil
	}
	var flushErr error
	if tx.bw != nil {
		flushErr = tx.bw.Flush()
		tx.bw = nil
	}
	closeErr := tx.file.Close()
	tx.file = nil
	if flushErr != nil {
		return fmt.Errorf("flush stream spill file %q: %w", tx.filePath, flushErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close stream spill file %q: %w", tx.filePath, closeErr)
	}
	return nil
}

func (tx *streamTx) discardOpenFile() {
	tx.bw = nil
	if tx.file != nil {
		_ = tx.file.Close()
		tx.file = nil
	}
}

func appendBindFrames(dst []byte, binds []relBind) []byte {
	for _, b := range binds {
		pos := len(dst)
		dst = append(dst, make([]byte, relBindFrameSize)...)
		encodeRelBindFrame(dst[pos:], b)
	}
	return dst
}

func appendWireFrame(dst []byte, walStart pq.LSN, serverTime time.Time, walData []byte) []byte {
	pos := len(dst)
	dst = append(dst, make([]byte, wireFrameHeaderSize)...)
	encodeWireHeader(dst[pos:], walStart, serverTime, len(walData))
	return append(dst, walData...)
}

func encodeRelBindFrame(dst []byte, b relBind) {
	dst[0] = frameKindRelBind
	binary.BigEndian.PutUint32(dst[1:5], b.oid)
	binary.BigEndian.PutUint32(dst[5:9], b.relIdx)
}

func encodeWireHeader(dst []byte, walStart pq.LSN, serverTime time.Time, dataLen int) {
	dst[0] = frameKindWire
	binary.BigEndian.PutUint64(dst[1:9], uint64(walStart))
	binary.BigEndian.PutUint64(dst[9:17], uint64(serverTime.UTC().UnixMicro()))
	binary.BigEndian.PutUint32(dst[17:21], uint32(dataLen))
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func truncateFile(path string, offset int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open spill file for truncate %q: %w", path, err)
	}
	truncErr := f.Truncate(offset)
	closeErr := f.Close()
	if truncErr != nil {
		return fmt.Errorf("truncate spill file %q to %d: %w", path, offset, truncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close spill file %q after truncate: %w", path, closeErr)
	}
	return nil
}

func stampMessageLSN(decodedMsg any, walStart pq.LSN) {
	switch m := decodedMsg.(type) {
	case *format.Insert:
		m.LSN = walStart
	case *format.Update:
		m.LSN = walStart
	case *format.Delete:
		m.LSN = walStart
	}
}
