package metadata

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/mmap"
	"github.com/zephyraoss/libchroma/internal/wire"
)

const (
	ckmFlagKnownMask = 0x3
	MappingRecordSize = 32
)

var OverflowMagicMO = [8]byte{'C', 'K', 'A', 'F', '-', 'M', 'O', 0}

// MetadataMap provides read access to a .ckm file.
type MetadataMap struct {
	F        *os.File
	Mmap     *mmap.Data
	Header   cktype.FileHeader
	Footer   cktype.Footer
	FileSize int64
}

// Open opens and validates a .ckm file for reading.
func Open(path string) (*MetadataMap, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening metadata map: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat metadata map: %w", err)
	}
	fileSize := fi.Size()

	mm, err := mmap.MapFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap metadata map: %w", err)
	}

	header, err := wire.ReadHeader(mm, wire.MagicCKM, fileSize)
	if err != nil {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	if err := wire.ValidateFlags(header.Flags, ckmFlagKnownMask); err != nil {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	footer, err := wire.ReadFooter(mm, fileSize, wire.FooterMagicCKM)
	if err != nil {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	return &MetadataMap{
		F:        f,
		Mmap:     mm,
		Header:   header,
		Footer:   footer,
		FileSize: fileSize,
	}, nil
}

// Close releases resources associated with the metadata map.
func (m *MetadataMap) Close() error {
	if err := mmap.Unmap(m.Mmap); err != nil {
		m.F.Close()
		return fmt.Errorf("munmap metadata map: %w", err)
	}
	return m.F.Close()
}

// HasTextMetadata returns true if the file contains text metadata (flag bit 0).
func (m *MetadataMap) HasTextMetadata() bool {
	return m.Header.Flags&0x1 != 0
}

// Lookup performs a binary search for fingerprintID in the mapping table.
func (m *MetadataMap) Lookup(fingerprintID uint32) (*cktype.MappingRecord, error) {
	maxCount := m.Header.Section0Length / MappingRecordSize
	count := int(m.Header.RecordCount)
	if uint64(count) > maxCount {
		count = int(maxCount)
	}
	base := m.Header.Section0Offset

	lo, hi := 0, count-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		recOffset := int64(base) + int64(mid)*MappingRecordSize

		if recOffset+MappingRecordSize > int64(m.Mmap.Len()) {
			return nil, fmt.Errorf("%w: mapping record %d", cktype.ErrOffsetOutOfBounds, mid)
		}

		var buf [MappingRecordSize]byte
		if _, err := m.Mmap.ReadAt(buf[:], recOffset); err != nil {
			return nil, fmt.Errorf("reading mapping record: %w", err)
		}

		id := binary.LittleEndian.Uint32(buf[0x00:])
		if id == fingerprintID {
			rec := &cktype.MappingRecord{
				FingerprintID: id,
				TrackID:       binary.LittleEndian.Uint32(buf[0x14:]),
				StringOffset:  binary.LittleEndian.Uint32(buf[0x18:]),
				StringLength:  binary.LittleEndian.Uint32(buf[0x1C:]),
			}
			copy(rec.MBID[:], buf[0x04:0x14])
			return rec, nil
		}
		if id < fingerprintID {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return nil, cktype.ErrRecordNotFound
}

// ReadMetadata reads and parses the string pool entry for a mapping record.
func (m *MetadataMap) ReadMetadata(rec *cktype.MappingRecord) (*cktype.TrackMetadata, error) {
	if rec.StringOffset == 0xFFFFFFFF {
		return nil, nil
	}

	absOffset := int64(m.Header.Section1Offset) + int64(rec.StringOffset)
	end := absOffset + int64(rec.StringLength)
	if end > int64(m.Mmap.Len()) {
		return nil, fmt.Errorf("%w: string pool entry", cktype.ErrOffsetOutOfBounds)
	}

	buf := make([]byte, rec.StringLength)
	if _, err := m.Mmap.ReadAt(buf, absOffset); err != nil {
		return nil, fmt.Errorf("reading string pool: %w", err)
	}

	return ParseStringPoolEntry(buf), nil
}

// ReadMappingRecordAt reads a single mapping record at the given byte offset.
func ReadMappingRecordAt(m *mmap.Data, off int64) (*cktype.MappingRecord, error) {
	var buf [MappingRecordSize]byte
	if _, err := m.ReadAt(buf[:], off); err != nil {
		return nil, err
	}
	rec := &cktype.MappingRecord{
		FingerprintID: binary.LittleEndian.Uint32(buf[0x00:]),
		TrackID:       binary.LittleEndian.Uint32(buf[0x14:]),
		StringOffset:  binary.LittleEndian.Uint32(buf[0x18:]),
		StringLength:  binary.LittleEndian.Uint32(buf[0x1C:]),
	}
	copy(rec.MBID[:], buf[0x04:0x14])
	return rec, nil
}

// ReadOverflowHeader reads the overflow header for a metadata map file.
func ReadOverflowHeader(m *MetadataMap) (count uint32, recordStart int64, err error) {
	off := int64(m.Footer.OverflowOffset)
	var buf [16]byte
	if _, err := m.Mmap.ReadAt(buf[:], off); err != nil {
		return 0, 0, fmt.Errorf("%w: reading metadata overflow header: %v", cktype.ErrCorruptOverflow, err)
	}

	var magic [8]byte
	copy(magic[:], buf[0:8])
	if magic != OverflowMagicMO {
		return 0, 0, fmt.Errorf("%w: bad metadata overflow magic", cktype.ErrCorruptOverflow)
	}

	count = binary.LittleEndian.Uint32(buf[8:12])
	recordStart = off + 16
	return count, recordStart, nil
}

// ReadOverflowMetadata reads metadata from the overflow string pool.
func ReadOverflowMetadata(m *MetadataMap, rec *cktype.MappingRecord) (*cktype.TrackMetadata, error) {
	if rec.StringOffset == 0xFFFFFFFF {
		return nil, nil
	}
	off := int64(m.Footer.OverflowOffset)
	var hdr [16]byte
	if _, err := m.Mmap.ReadAt(hdr[:], off); err != nil {
		return nil, err
	}
	stringsOff := binary.LittleEndian.Uint32(hdr[12:16])
	absOffset := off + int64(stringsOff) + int64(rec.StringOffset)

	data := make([]byte, rec.StringLength)
	if _, err := m.Mmap.ReadAt(data, absOffset); err != nil {
		return nil, err
	}
	return ParseStringPoolEntry(data), nil
}

// ParseStringPoolEntry parses a key=value encoded metadata block.
func ParseStringPoolEntry(data []byte) *cktype.TrackMetadata {
	meta := &cktype.TrackMetadata{}
	for _, line := range strings.Split(string(data), "\n") {
		if len(line) < 2 || line[1] != '=' {
			continue
		}
		val := line[2:]
		switch line[0] {
		case 't':
			meta.Title = val
		case 'a':
			meta.Artist = val
		case 'r':
			meta.Release = val
		case 'y':
			meta.Year = val
		}
	}
	return meta
}

// EncodeStringPoolEntry encodes track metadata as a key=value string pool entry.
func EncodeStringPoolEntry(m *cktype.TrackMetadata) []byte {
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
