package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/v2/internal/cktype"
	"github.com/zephyraoss/libchroma/v2/internal/wire"
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
}

type BuilderOptions struct {
	SpillDir string
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
			rawCount:      uint16(len(values)),
		})
		b.spoolOffset += uint64(len(compressed))
		return nil
	}
	b.records = append(b.records, buildRecord{
		fingerprintID: fingerprintID,
		durationMs:    durationMs,
		compressed:    compressed,
		rawCount:      uint16(len(values)),
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
		sort.SliceStable(b.spillRecords, func(i, j int) bool {
			return b.spillRecords[i].fingerprintID < b.spillRecords[j].fingerprintID
		})
		payloadBuf := make([]byte, 0xFFFF)
		return b.writeFile(len(b.spillRecords),
			func(i int) recordMeta {
				r := b.spillRecords[i]
				return recordMeta{r.fingerprintID, r.durationMs, r.length, r.rawCount}
			},
			func(i int) ([]byte, error) {
				r := b.spillRecords[i]
				buf := payloadBuf[:r.length]
				if _, err := b.spool.ReadAt(buf, int64(r.spoolOffset)); err != nil {
					return nil, fmt.Errorf("reading spool file: %w", err)
				}
				return buf, nil
			})
	}

	sort.SliceStable(b.records, func(i, j int) bool {
		return b.records[i].fingerprintID < b.records[j].fingerprintID
	})
	return b.writeFile(len(b.records),
		func(i int) recordMeta {
			r := b.records[i]
			return recordMeta{r.fingerprintID, r.durationMs, uint16(len(r.compressed)), r.rawCount}
		},
		func(i int) ([]byte, error) {
			return b.records[i].compressed, nil
		})
}

func (b *Builder) removeSpool() {
	b.spool.Close()
	os.Remove(b.spool.Name())
}

func (b *Builder) writeFile(count int, meta func(int) recordMeta, payload func(int) ([]byte, error)) error {
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
	var section1Length uint64
	for i := 0; i < count; i++ {
		p, err := payload(i)
		if err != nil {
			return err
		}
		if _, err := w.Write(p); err != nil {
			return err
		}
		section1Length += uint64(len(p))
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
