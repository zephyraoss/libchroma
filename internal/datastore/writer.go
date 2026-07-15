package datastore

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/v2/internal/cktype"
	"github.com/zephyraoss/libchroma/v2/internal/wire"
)

const (
	gatherBatchBytes  = 8 << 20
	gatherQueueFactor = 2
)

type Builder struct {
	f           *os.File
	path        string
	compression cktype.CompressionMethod
	datasetID   uuid.UUID
	sourceDate  uint64
	records     []buildRecord

	spool        *os.File
	spoolW       *bufio.Writer
	spoolOffset  uint64
	spillRecords []spillRecord

	concurrency int
	logFn       func(format string, args ...any)
}

type BuilderOptions struct {
	SpillDir string

	Concurrency int

	Logf func(format string, args ...any)
}

type buildRecord struct {
	fingerprintID uint32
	durationMs    uint32
	compressed    []byte
	rawCount      uint16
}

type spillRecord struct {
	fingerprintID uint32
	durationMs    uint32
	spoolOffset   uint64
	length        uint16
	rawCount      uint16
}

type recordMeta struct {
	fingerprintID uint32
	durationMs    uint32
	length        uint16
	rawCount      uint16
}

func NewBuilder(path string, compression cktype.CompressionMethod) (*Builder, error) {
	return NewBuilderWithOptions(path, compression, BuilderOptions{})
}

func NewBuilderWithOptions(path string, compression cktype.CompressionMethod, opts BuilderOptions) (*Builder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	b := &Builder{
		f:           f,
		path:        path,
		compression: compression,
		concurrency: opts.Concurrency,
		logFn:       opts.Logf,
	}
	if opts.SpillDir != "" {
		spool, err := os.CreateTemp(opts.SpillDir, "ckd-spool-*.tmp")
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("creating spool file: %w", err)
		}
		b.spool = spool
		b.spoolW = bufio.NewWriterSize(spool, 1<<20)
	}
	return b, nil
}

func (b *Builder) logf(format string, args ...any) {
	if b.logFn != nil {
		b.logFn(format, args...)
	}
}

func (b *Builder) SetSourceDate(t uint64) {
	b.sourceDate = t
}

func (b *Builder) SetDatasetID(id uuid.UUID) {
	b.datasetID = id
}

func (b *Builder) Add(fingerprintID uint32, durationMs uint32, values []uint32) error {
	if len(values) > 0xFFFF {
		return fmt.Errorf("ckaf: fingerprint %d has %d sub-fingerprints, exceeds u16 max (65535)", fingerprintID, len(values))
	}
	compressed, err := b.compress(values)
	if err != nil {
		return err
	}
	return b.addCompressed(fingerprintID, durationMs, compressed, uint16(len(values)), false)
}

func (b *Builder) AddPrecompressed(fingerprintID uint32, durationMs uint32, compressed []byte, rawCount uint16) error {
	return b.addCompressed(fingerprintID, durationMs, compressed, rawCount, true)
}

func (b *Builder) addCompressed(fingerprintID uint32, durationMs uint32, compressed []byte, rawCount uint16, clone bool) error {
	if len(compressed) > 0xFFFF {
		return fmt.Errorf("ckaf: fingerprint %d compressed to %d bytes, exceeds u16 max (65535)", fingerprintID, len(compressed))
	}
	if b.spool != nil {
		if _, err := b.spoolW.Write(compressed); err != nil {
			return fmt.Errorf("writing spool file: %w", err)
		}
		b.spillRecords = append(b.spillRecords, spillRecord{
			fingerprintID: fingerprintID,
			durationMs:    durationMs,
			spoolOffset:   b.spoolOffset,
			length:        uint16(len(compressed)),
			rawCount:      rawCount,
		})
		b.spoolOffset += uint64(len(compressed))
		return nil
	}
	if clone {
		compressed = slices.Clone(compressed)
	}
	b.records = append(b.records, buildRecord{
		fingerprintID: fingerprintID,
		durationMs:    durationMs,
		compressed:    compressed,
		rawCount:      rawCount,
	})
	return nil
}

func (b *Builder) compress(values []uint32) ([]byte, error) {
	switch b.compression {
	case cktype.CompressPFOR:
		return wire.CompressFingerprintPFOR(values)
	default:
		return wire.CompressFingerprint(values), nil
	}
}

func (b *Builder) Finish() error {
	defer b.f.Close()

	if b.spool != nil {
		defer b.removeSpool()
		if err := b.spoolW.Flush(); err != nil {
			return fmt.Errorf("flushing spool file: %w", err)
		}
		sortStart := time.Now()
		slices.SortStableFunc(b.spillRecords, func(x, y spillRecord) int {
			return cmp.Compare(x.fingerprintID, y.fingerprintID)
		})
		b.logf("ckd finish records sorted count=%d elapsed=%s", len(b.spillRecords), time.Since(sortStart).Round(time.Millisecond))
		return b.writeFile(len(b.spillRecords),
			func(i int) recordMeta {
				r := b.spillRecords[i]
				return recordMeta{r.fingerprintID, r.durationMs, r.length, r.rawCount}
			},
			b.writeSpillPayloads)
	}

	slices.SortStableFunc(b.records, func(x, y buildRecord) int {
		return cmp.Compare(x.fingerprintID, y.fingerprintID)
	})
	return b.writeFile(len(b.records),
		func(i int) recordMeta {
			r := b.records[i]
			return recordMeta{r.fingerprintID, r.durationMs, uint16(len(r.compressed)), r.rawCount}
		},
		func(w *bufio.Writer) (uint64, error) {
			var total uint64
			for i := range b.records {
				p := b.records[i].compressed
				if _, err := w.Write(p); err != nil {
					return 0, err
				}
				total += uint64(len(p))
			}
			return total, nil
		})
}

func (b *Builder) removeSpool() {
	b.spool.Close()
	os.Remove(b.spool.Name())
}

func (b *Builder) writeSpillPayloads(w *bufio.Writer) (uint64, error) {
	copyStart := time.Now()
	var total uint64
	var err error
	if b.concurrency > 1 {
		total, err = b.gatherSpillPayloads(w)
	} else {
		total, err = b.copySpillPayloadsSerial(w)
	}
	if err != nil {
		return 0, err
	}
	b.logf("ckd finish payloads copied bytes=%d elapsed=%s", total, time.Since(copyStart).Round(time.Millisecond))
	return total, nil
}

func (b *Builder) copySpillPayloadsSerial(w *bufio.Writer) (uint64, error) {
	payloadBuf := make([]byte, 0xFFFF)
	var total uint64
	for i := range b.spillRecords {
		r := b.spillRecords[i]
		buf := payloadBuf[:r.length]
		if _, err := b.spool.ReadAt(buf, int64(r.spoolOffset)); err != nil {
			return 0, fmt.Errorf("reading spool file: %w", err)
		}
		if _, err := w.Write(buf); err != nil {
			return 0, err
		}
		total += uint64(r.length)
	}
	return total, nil
}

type gatherBatch struct {
	start int
	end   int
	buf   []byte
	err   error
	done  chan struct{}
}

func (b *Builder) gatherSpillPayloads(w *bufio.Writer) (uint64, error) {
	jobs := make(chan *gatherBatch)
	ordered := make(chan *gatherBatch, b.concurrency*gatherQueueFactor)

	go func() {
		defer close(ordered)
		defer close(jobs)
		start := 0
		for start < len(b.spillRecords) {
			end := start
			size := 0
			for end < len(b.spillRecords) && size < gatherBatchBytes {
				size += int(b.spillRecords[end].length)
				end++
			}
			batch := &gatherBatch{start: start, end: end, buf: make([]byte, size), done: make(chan struct{})}
			ordered <- batch
			jobs <- batch
			start = end
		}
	}()

	for i := 0; i < b.concurrency; i++ {
		go func() {
			for batch := range jobs {
				batch.err = b.fillGatherBatch(batch)
				close(batch.done)
			}
		}()
	}

	var total uint64
	var firstErr error
	for batch := range ordered {
		<-batch.done
		if firstErr != nil {
			continue
		}
		if batch.err != nil {
			firstErr = batch.err
			continue
		}
		if _, err := w.Write(batch.buf); err != nil {
			firstErr = err
			continue
		}
		total += uint64(len(batch.buf))
	}
	if firstErr != nil {
		return 0, firstErr
	}
	return total, nil
}

func (b *Builder) fillGatherBatch(batch *gatherBatch) error {
	bufOff := 0
	i := batch.start
	for i < batch.end {
		spoolStart := b.spillRecords[i].spoolOffset
		runLen := int(b.spillRecords[i].length)
		j := i + 1
		for j < batch.end && b.spillRecords[j].spoolOffset == b.spillRecords[j-1].spoolOffset+uint64(b.spillRecords[j-1].length) {
			runLen += int(b.spillRecords[j].length)
			j++
		}
		if _, err := b.spool.ReadAt(batch.buf[bufOff:bufOff+runLen], int64(spoolStart)); err != nil {
			return fmt.Errorf("reading spool file: %w", err)
		}
		bufOff += runLen
		i = j
	}
	return nil
}

func (b *Builder) writeFile(count int, meta func(int) recordMeta, writePayloads func(w *bufio.Writer) (uint64, error)) error {
	w := bufio.NewWriterSize(b.f, 1<<20)

	var placeholder [wire.HeaderSize]byte
	if _, err := w.Write(placeholder[:]); err != nil {
		return err
	}

	section0Offset := uint64(wire.HeaderSize)
	var dataOffset uint64
	for i := 0; i < count; i++ {
		rec := meta(i)
		var buf [RecordSize]byte
		binary.LittleEndian.PutUint32(buf[0:4], rec.fingerprintID)
		binary.LittleEndian.PutUint32(buf[4:8], uint32(dataOffset&0xFFFFFFFF))
		buf[8] = byte(dataOffset >> 32)
		buf[9] = byte(dataOffset >> 40)
		binary.LittleEndian.PutUint16(buf[10:12], rec.length)
		binary.LittleEndian.PutUint32(buf[12:16], rec.durationMs)
		binary.LittleEndian.PutUint16(buf[16:18], rec.rawCount)
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		dataOffset += uint64(rec.length)
	}
	section0Length := uint64(count) * RecordSize

	pos := section0Offset + section0Length
	if pad := pos % 8; pad != 0 {
		padding := make([]byte, 8-pad)
		if _, err := w.Write(padding); err != nil {
			return err
		}
		pos += 8 - pad
	}

	section1Offset := pos
	section1Length, err := writePayloads(w)
	if err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	footerOffset := int64(section1Offset + section1Length)
	footer := cktype.Footer{
		OverflowOffset: 0,
		Magic:          wire.FooterMagicCKD,
	}
	if err := wire.WriteFooter(b.f, footerOffset, footer); err != nil {
		return err
	}

	var flags uint32
	if b.compression == cktype.CompressPFOR {
		flags |= 1
	}

	h := cktype.FileHeader{
		Magic:          wire.MagicCKD,
		VersionMajor:   wire.CurrentVersionMajor,
		VersionMinor:   wire.CurrentVersionMinor,
		Flags:          flags,
		RecordCount:    uint64(count),
		CreatedAt:      uint64(time.Now().Unix()),
		SourceDate:     b.sourceDate,
		DatasetID:      b.datasetID,
		Section0Offset: section0Offset,
		Section0Length: section0Length,
		Section1Offset: section1Offset,
		Section1Length: section1Length,
	}
	return wire.WriteHeader(b.f, h)
}
