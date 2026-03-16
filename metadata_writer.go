package chroma

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
)

const mappingRecordSize = 32

// MetadataMapBuilder constructs a .ckm file.
type MetadataMapBuilder struct {
	f           *os.File
	path        string
	includeText bool
	datasetID   uuid.UUID
	records     []metadataBuildRecord
}

type metadataBuildRecord struct {
	fingerprintID uint32
	mbid          uuid.UUID
	trackID       uint32
	metadata      *TrackMetadata
}

// NewMetadataMapBuilder creates a new builder that writes a .ckm file at path.
// If includeText is true, text metadata will be written to the string pool.
func NewMetadataMapBuilder(path string, includeText bool) (*MetadataMapBuilder, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "ckm-build-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	return &MetadataMapBuilder{
		f:           f,
		path:        path,
		includeText: includeText,
	}, nil
}

// SetDatasetID sets the dataset UUID written to the file header.
func (b *MetadataMapBuilder) SetDatasetID(id uuid.UUID) {
	b.datasetID = id
}

// Add accumulates a mapping record. meta may be nil if no text metadata is available.
func (b *MetadataMapBuilder) Add(fingerprintID uint32, mbid uuid.UUID, trackID uint32, meta *TrackMetadata) error {
	b.records = append(b.records, metadataBuildRecord{
		fingerprintID: fingerprintID,
		mbid:          mbid,
		trackID:       trackID,
		metadata:      meta,
	})
	return nil
}

// Finish sorts records, builds the mapping table and string pool, writes
// header and footer, and atomically renames the temp file to the final path.
func (b *MetadataMapBuilder) Finish() error {

	sort.Slice(b.records, func(i, j int) bool {
		return b.records[i].fingerprintID < b.records[j].fingerprintID
	})

	// Build string pool
	var stringPool []byte
	type stringRef struct {
		offset uint32
		length uint32
	}
	refs := make([]stringRef, len(b.records))
	for i, rec := range b.records {
		if !b.includeText || rec.metadata == nil {
			refs[i] = stringRef{offset: 0xFFFFFFFF, length: 0}
			continue
		}
		entry := encodeStringPoolEntry(rec.metadata)
		refs[i] = stringRef{
			offset: uint32(len(stringPool)),
			length: uint32(len(entry)),
		}
		stringPool = append(stringPool, entry...)
	}

	// Compute layout
	section0Offset := uint64(HeaderSize)
	section0Length := uint64(len(b.records)) * mappingRecordSize
	// Align section 1 to 8-byte boundary
	section1Offset := section0Offset + section0Length
	if section1Offset%8 != 0 {
		section1Offset += 8 - (section1Offset % 8)
	}
	section1Length := uint64(len(stringPool))

	// Flags
	var flags uint32
	if b.includeText {
		flags |= 0x1 // bit 0: has_text_metadata
	}

	// Write header
	h := FileHeader{
		Magic:          MagicCKM,
		VersionMajor:   CurrentVersionMajor,
		VersionMinor:   CurrentVersionMinor,
		Flags:          flags,
		RecordCount:    uint64(len(b.records)),
		CreatedAt:      uint64(time.Now().Unix()),
		DatasetID:      b.datasetID,
		Section0Offset: section0Offset,
		Section0Length: section0Length,
		Section1Offset: section1Offset,
		Section1Length: section1Length,
	}
	if err := WriteHeader(b.f, h); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	// Write mapping table
	tableOffset := int64(section0Offset)
	var rec [mappingRecordSize]byte
	for i, r := range b.records {
		binary.LittleEndian.PutUint32(rec[0x00:], r.fingerprintID)
		copy(rec[0x04:0x14], r.mbid[:])
		binary.LittleEndian.PutUint32(rec[0x14:], r.trackID)
		binary.LittleEndian.PutUint32(rec[0x18:], refs[i].offset)
		binary.LittleEndian.PutUint32(rec[0x1C:], refs[i].length)
		if _, err := b.f.WriteAt(rec[:], tableOffset); err != nil {
			return fmt.Errorf("writing mapping record %d: %w", i, err)
		}
		tableOffset += mappingRecordSize
	}

	// Write alignment padding if needed
	if pad := int64(section1Offset) - tableOffset; pad > 0 {
		zeros := make([]byte, pad)
		if _, err := b.f.WriteAt(zeros, tableOffset); err != nil {
			return fmt.Errorf("writing alignment padding: %w", err)
		}
	}

	// Write string pool
	if len(stringPool) > 0 {
		if _, err := b.f.WriteAt(stringPool, int64(section1Offset)); err != nil {
			return fmt.Errorf("writing string pool: %w", err)
		}
	}

	// Write footer
	footerOffset := int64(section1Offset) + int64(section1Length)
	footer := Footer{
		OverflowOffset: 0,
		Magic:          FooterMagicCKM,
	}
	if err := WriteFooter(b.f, footerOffset, footer); err != nil {
		return fmt.Errorf("writing footer: %w", err)
	}

	// Close temp file before rename
	tmpPath := b.f.Name()
	if err := b.f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err := os.Rename(tmpPath, b.path); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}

	return nil
}

// encodeStringPoolEntry encodes track metadata as a key=value string pool entry.
func encodeStringPoolEntry(m *TrackMetadata) []byte {
	var buf []byte
	if m.Title != "" {
		buf = append(buf, "t="...)
		buf = append(buf, m.Title...)
		buf = append(buf, '\n')
	}
	if m.Artist != "" {
		buf = append(buf, "a="...)
		buf = append(buf, m.Artist...)
		buf = append(buf, '\n')
	}
	if m.Release != "" {
		buf = append(buf, "r="...)
		buf = append(buf, m.Release...)
		buf = append(buf, '\n')
	}
	if m.Year != "" {
		buf = append(buf, "y="...)
		buf = append(buf, m.Year...)
		buf = append(buf, '\n')
	}
	return buf
}
