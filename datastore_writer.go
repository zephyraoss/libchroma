package chroma

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

// DataStoreBuilder constructs a .ckd file by accumulating fingerprint records
// and writing them in sorted order.
type DataStoreBuilder struct {
	f           *os.File
	path        string
	compression CompressionMethod
	datasetID   uuid.UUID
	sourceDate  uint64
	records     []datastoreBuildRecord
}

type datastoreBuildRecord struct {
	fingerprintID uint32
	durationMs    uint32
	compressed    []byte
	rawCount      uint16
}

// NewDataStoreBuilder creates a new DataStoreBuilder that writes to the given path.
func NewDataStoreBuilder(path string, compression CompressionMethod) (*DataStoreBuilder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &DataStoreBuilder{
		f:           f,
		path:        path,
		compression: compression,
	}, nil
}

// SetSourceDate sets the source_date header field.
func (b *DataStoreBuilder) SetSourceDate(t uint64) {
	b.sourceDate = t
}

// SetDatasetID sets the dataset_id header field.
func (b *DataStoreBuilder) SetDatasetID(id uuid.UUID) {
	b.datasetID = id
}

// Add compresses the fingerprint values and stores the record for later writing.
func (b *DataStoreBuilder) Add(fingerprintID uint32, durationMs uint32, values []uint32) error {
	if len(values) > 0xFFFF {
		return fmt.Errorf("ckaf: fingerprint %d has %d sub-fingerprints, exceeds u16 max (65535)", fingerprintID, len(values))
	}
	compressed := CompressFingerprint(values)
	if len(compressed) > 0xFFFF {
		return fmt.Errorf("ckaf: fingerprint %d compressed to %d bytes, exceeds u16 max (65535)", fingerprintID, len(compressed))
	}
	b.records = append(b.records, datastoreBuildRecord{
		fingerprintID: fingerprintID,
		durationMs:    durationMs,
		compressed:    compressed,
		rawCount:      uint16(len(values)),
	})
	return nil
}

const recordSize = 20

// Finish sorts records, writes all sections, and closes the file.
func (b *DataStoreBuilder) Finish() error {
	defer b.f.Close()

	// Sort records by fingerprintID.
	sort.Slice(b.records, func(i, j int) bool {
		return b.records[i].fingerprintID < b.records[j].fingerprintID
	})

	// Write placeholder header.
	var placeholder [HeaderSize]byte
	if _, err := b.f.Write(placeholder[:]); err != nil {
		return err
	}

	// Section 0: record table.
	section0Offset := uint64(HeaderSize)
	var dataOffset uint64
	for _, rec := range b.records {
		var buf [recordSize]byte
		binary.LittleEndian.PutUint32(buf[0:4], rec.fingerprintID)
		// u48 data_offset (6 bytes LE)
		binary.LittleEndian.PutUint32(buf[4:8], uint32(dataOffset&0xFFFFFFFF))
		buf[8] = byte(dataOffset >> 32)
		buf[9] = byte(dataOffset >> 40)
		binary.LittleEndian.PutUint16(buf[10:12], uint16(len(rec.compressed)))
		binary.LittleEndian.PutUint32(buf[12:16], rec.durationMs)
		binary.LittleEndian.PutUint16(buf[16:18], rec.rawCount)
		// buf[18:20] reserved, already zero
		if _, err := b.f.Write(buf[:]); err != nil {
			return err
		}
		dataOffset += uint64(len(rec.compressed))
	}
	section0Length := uint64(len(b.records)) * recordSize

	// Pad to 8-byte alignment.
	pos := section0Offset + section0Length
	if pad := pos % 8; pad != 0 {
		padding := make([]byte, 8-pad)
		if _, err := b.f.Write(padding); err != nil {
			return err
		}
		pos += 8 - pad
	}

	// Section 1: fingerprint data blob.
	section1Offset := pos
	var section1Length uint64
	for _, rec := range b.records {
		if _, err := b.f.Write(rec.compressed); err != nil {
			return err
		}
		section1Length += uint64(len(rec.compressed))
	}

	// Footer.
	footerOffset := int64(section1Offset + section1Length)
	footer := Footer{
		OverflowOffset: 0,
		Magic:          FooterMagicCKD,
	}
	if err := WriteFooter(b.f, footerOffset, footer); err != nil {
		return err
	}

	// Build flags: bit 0 = compression method, bit 1 = has_overflow (0).
	var flags uint32
	if b.compression == CompressPFOR {
		flags |= 1
	}

	// Rewrite header with correct offsets.
	h := FileHeader{
		Magic:          MagicCKD,
		VersionMajor:   CurrentVersionMajor,
		VersionMinor:   CurrentVersionMinor,
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
	return WriteHeader(b.f, h)
}
