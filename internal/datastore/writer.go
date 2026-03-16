package datastore

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/wire"
)

// Builder constructs a .ckd file by accumulating fingerprint records
// and writing them in sorted order.
type Builder struct {
	f           *os.File
	path        string
	compression cktype.CompressionMethod
	datasetID   uuid.UUID
	sourceDate  uint64
	records     []buildRecord
}

type buildRecord struct {
	fingerprintID uint32
	durationMs    uint32
	compressed    []byte
	rawCount      uint16
}

// NewBuilder creates a new Builder that writes to the given path.
func NewBuilder(path string, compression cktype.CompressionMethod) (*Builder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Builder{
		f:           f,
		path:        path,
		compression: compression,
	}, nil
}

// SetSourceDate sets the source_date header field.
func (b *Builder) SetSourceDate(t uint64) {
	b.sourceDate = t
}

// SetDatasetID sets the dataset_id header field.
func (b *Builder) SetDatasetID(id uuid.UUID) {
	b.datasetID = id
}

// Add compresses the fingerprint values and stores the record for later writing.
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

// Finish sorts records, writes all sections, and closes the file.
func (b *Builder) Finish() error {
	defer b.f.Close()

	sort.Slice(b.records, func(i, j int) bool {
		return b.records[i].fingerprintID < b.records[j].fingerprintID
	})

	var placeholder [wire.HeaderSize]byte
	if _, err := b.f.Write(placeholder[:]); err != nil {
		return err
	}

	section0Offset := uint64(wire.HeaderSize)
	var dataOffset uint64
	for _, rec := range b.records {
		var buf [RecordSize]byte
		binary.LittleEndian.PutUint32(buf[0:4], rec.fingerprintID)
		binary.LittleEndian.PutUint32(buf[4:8], uint32(dataOffset&0xFFFFFFFF))
		buf[8] = byte(dataOffset >> 32)
		buf[9] = byte(dataOffset >> 40)
		binary.LittleEndian.PutUint16(buf[10:12], uint16(len(rec.compressed)))
		binary.LittleEndian.PutUint32(buf[12:16], rec.durationMs)
		binary.LittleEndian.PutUint16(buf[16:18], rec.rawCount)
		if _, err := b.f.Write(buf[:]); err != nil {
			return err
		}
		dataOffset += uint64(len(rec.compressed))
	}
	section0Length := uint64(len(b.records)) * RecordSize

	pos := section0Offset + section0Length
	if pad := pos % 8; pad != 0 {
		padding := make([]byte, 8-pad)
		if _, err := b.f.Write(padding); err != nil {
			return err
		}
		pos += 8 - pad
	}

	section1Offset := pos
	var section1Length uint64
	for _, rec := range b.records {
		if _, err := b.f.Write(rec.compressed); err != nil {
			return err
		}
		section1Length += uint64(len(rec.compressed))
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
		RecordCount:    uint64(len(b.records)),
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
