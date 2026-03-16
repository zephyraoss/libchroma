package chroma

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
)

const ckmFlagKnownMask = 0x3 // bits 0-1

// MetadataMap provides read access to a .ckm file.
type MetadataMap struct {
	f        *os.File
	mmap     *mmapData
	header   FileHeader
	footer   Footer
	fileSize int64
}

// OpenMetadataMap opens and validates a .ckm file for reading.
func OpenMetadataMap(path string) (*MetadataMap, error) {
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

	mm, err := mmapFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap metadata map: %w", err)
	}

	header, err := ReadHeader(mm, MagicCKM, fileSize)
	if err != nil {
		munmapData(mm)
		f.Close()
		return nil, err
	}

	if err := ValidateFlags(header.Flags, ckmFlagKnownMask); err != nil {
		munmapData(mm)
		f.Close()
		return nil, err
	}

	footer, err := ReadFooter(mm, fileSize, FooterMagicCKM)
	if err != nil {
		munmapData(mm)
		f.Close()
		return nil, err
	}

	return &MetadataMap{
		f:        f,
		mmap:     mm,
		header:   header,
		footer:   footer,
		fileSize: fileSize,
	}, nil
}

// Close releases resources associated with the metadata map.
func (m *MetadataMap) Close() error {
	if err := munmapData(m.mmap); err != nil {
		m.f.Close()
		return fmt.Errorf("munmap metadata map: %w", err)
	}
	return m.f.Close()
}

// Header returns the file header.
func (m *MetadataMap) Header() FileHeader {
	return m.header
}

// HasTextMetadata returns true if the file contains text metadata (flag bit 0).
func (m *MetadataMap) HasTextMetadata() bool {
	return m.header.Flags&0x1 != 0
}

// Lookup performs a binary search for fingerprintID in the mapping table.
func (m *MetadataMap) Lookup(fingerprintID uint32) (*MappingRecord, error) {
	// Validate RecordCount against actual section size.
	maxCount := m.header.Section0Length / mappingRecordSize
	count := int(m.header.RecordCount)
	if uint64(count) > maxCount {
		count = int(maxCount)
	}
	base := m.header.Section0Offset

	lo, hi := 0, count-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		recOffset := int64(base) + int64(mid)*mappingRecordSize

		if recOffset+mappingRecordSize > int64(m.mmap.Len()) {
			return nil, fmt.Errorf("%w: mapping record %d", ErrOffsetOutOfBounds, mid)
		}

		var buf [mappingRecordSize]byte
		if _, err := m.mmap.ReadAt(buf[:], recOffset); err != nil {
			return nil, fmt.Errorf("reading mapping record: %w", err)
		}

		id := binary.LittleEndian.Uint32(buf[0x00:])
		if id == fingerprintID {
			rec := &MappingRecord{
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

	return nil, ErrRecordNotFound
}

// ReadMetadata reads and parses the string pool entry for a mapping record.
func (m *MetadataMap) ReadMetadata(rec *MappingRecord) (*TrackMetadata, error) {
	if rec.StringOffset == 0xFFFFFFFF {
		return nil, nil
	}

	absOffset := int64(m.header.Section1Offset) + int64(rec.StringOffset)
	end := absOffset + int64(rec.StringLength)
	if end > int64(m.mmap.Len()) {
		return nil, fmt.Errorf("%w: string pool entry", ErrOffsetOutOfBounds)
	}

	buf := make([]byte, rec.StringLength)
	if _, err := m.mmap.ReadAt(buf, absOffset); err != nil {
		return nil, fmt.Errorf("reading string pool: %w", err)
	}

	return parseStringPoolEntry(buf), nil
}

// parseStringPoolEntry parses a key=value encoded metadata block.
func parseStringPoolEntry(data []byte) *TrackMetadata {
	meta := &TrackMetadata{}
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
