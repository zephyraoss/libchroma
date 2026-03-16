package metadata

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/wire"
)

// Builder constructs a .ckm file.
type Builder struct {
	f           *os.File
	path        string
	includeText bool
	datasetID   uuid.UUID
	records     []buildRecord
}

type buildRecord struct {
	fingerprintID uint32
	mbid          uuid.UUID
	trackID       uint32
	metadata      *cktype.TrackMetadata
}

// NewBuilder creates a new builder that writes a .ckm file at path.
func NewBuilder(path string, includeText bool) (*Builder, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "ckm-build-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	return &Builder{
		f:           f,
		path:        path,
		includeText: includeText,
	}, nil
}

// SetDatasetID sets the dataset UUID written to the file header.
func (b *Builder) SetDatasetID(id uuid.UUID) {
	b.datasetID = id
}

// Add accumulates a mapping record.
func (b *Builder) Add(fingerprintID uint32, mbid uuid.UUID, trackID uint32, meta *cktype.TrackMetadata) error {
	b.records = append(b.records, buildRecord{
		fingerprintID: fingerprintID,
		mbid:          mbid,
		trackID:       trackID,
		metadata:      meta,
	})
	return nil
}

// Finish sorts records, builds the mapping table and string pool, writes
// header and footer, and atomically renames the temp file to the final path.
func (b *Builder) Finish() error {

	sort.Slice(b.records, func(i, j int) bool {
		return b.records[i].fingerprintID < b.records[j].fingerprintID
	})

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
		entry := EncodeStringPoolEntry(rec.metadata)
		refs[i] = stringRef{
			offset: uint32(len(stringPool)),
			length: uint32(len(entry)),
		}
		stringPool = append(stringPool, entry...)
	}

	section0Offset := uint64(wire.HeaderSize)
	section0Length := uint64(len(b.records)) * MappingRecordSize
	section1Offset := section0Offset + section0Length
	if section1Offset%8 != 0 {
		section1Offset += 8 - (section1Offset % 8)
	}
	section1Length := uint64(len(stringPool))

	var flags uint32
	if b.includeText {
		flags |= 0x1
	}

	h := cktype.FileHeader{
		Magic:          wire.MagicCKM,
		VersionMajor:   wire.CurrentVersionMajor,
		VersionMinor:   wire.CurrentVersionMinor,
		Flags:          flags,
		RecordCount:    uint64(len(b.records)),
		CreatedAt:      uint64(time.Now().Unix()),
		DatasetID:      b.datasetID,
		Section0Offset: section0Offset,
		Section0Length: section0Length,
		Section1Offset: section1Offset,
		Section1Length: section1Length,
	}
	if err := wire.WriteHeader(b.f, h); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	tableOffset := int64(section0Offset)
	var rec [MappingRecordSize]byte
	for i, r := range b.records {
		binary.LittleEndian.PutUint32(rec[0x00:], r.fingerprintID)
		copy(rec[0x04:0x14], r.mbid[:])
		binary.LittleEndian.PutUint32(rec[0x14:], r.trackID)
		binary.LittleEndian.PutUint32(rec[0x18:], refs[i].offset)
		binary.LittleEndian.PutUint32(rec[0x1C:], refs[i].length)
		if _, err := b.f.WriteAt(rec[:], tableOffset); err != nil {
			return fmt.Errorf("writing mapping record %d: %w", i, err)
		}
		tableOffset += MappingRecordSize
	}

	if pad := int64(section1Offset) - tableOffset; pad > 0 {
		zeros := make([]byte, pad)
		if _, err := b.f.WriteAt(zeros, tableOffset); err != nil {
			return fmt.Errorf("writing alignment padding: %w", err)
		}
	}

	if len(stringPool) > 0 {
		if _, err := b.f.WriteAt(stringPool, int64(section1Offset)); err != nil {
			return fmt.Errorf("writing string pool: %w", err)
		}
	}

	footerOffset := int64(section1Offset) + int64(section1Length)
	footer := cktype.Footer{
		OverflowOffset: 0,
		Magic:          wire.FooterMagicCKM,
	}
	if err := wire.WriteFooter(b.f, footerOffset, footer); err != nil {
		return fmt.Errorf("writing footer: %w", err)
	}

	tmpPath := b.f.Name()
	if err := b.f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err := os.Rename(tmpPath, b.path); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}

	return nil
}
