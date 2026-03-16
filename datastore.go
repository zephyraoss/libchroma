package chroma

import (
	"encoding/binary"
	"fmt"
	"os"
)

var overflowMagicCKD = [8]byte{'C', 'K', 'A', 'F', '-', 'D', 'O', 0}

// DataStore provides read access to a .ckd file via memory-mapping.
type DataStore struct {
	f           *os.File
	mmap        *mmapData
	header      FileHeader
	footer      Footer
	fileSize    int64
	compression CompressionMethod
	hasOverflow bool
	// Overflow fields (populated if hasOverflow).
	overflowCount     uint32
	overflowDataOff   uint32
	overflowStart     int64 // absolute offset of overflow record table
	overflowDataStart int64 // absolute offset of overflow data blob
}

// OpenDataStore opens and validates a .ckd file for reading.
func OpenDataStore(path string) (*DataStore, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := fi.Size()

	m, err := mmapFile(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	header, err := ReadHeader(m, MagicCKD, fileSize)
	if err != nil {
		munmapData(m)
		f.Close()
		return nil, err
	}

	if err := ValidateFlags(header.Flags, 0x3); err != nil {
		munmapData(m)
		f.Close()
		return nil, err
	}

	compression := CompressVarint
	if header.Flags&1 != 0 {
		munmapData(m)
		f.Close()
		return nil, fmt.Errorf("ckaf: PFOR compression (flag bit 0) is not yet supported")
	}
	hasOverflow := header.Flags&2 != 0

	footer, err := ReadFooter(m, fileSize, FooterMagicCKD)
	if err != nil {
		munmapData(m)
		f.Close()
		return nil, err
	}

	ds := &DataStore{
		f:           f,
		mmap:        m,
		header:      header,
		footer:      footer,
		fileSize:    fileSize,
		compression: compression,
		hasOverflow: hasOverflow,
	}

	if hasOverflow && footer.OverflowOffset != 0 {
		if err := ds.readOverflowHeader(); err != nil {
			munmapData(m)
			f.Close()
			return nil, err
		}
	}

	return ds, nil
}

func (ds *DataStore) readOverflowHeader() error {
	off := int64(ds.footer.OverflowOffset)
	var buf [16]byte
	if _, err := ds.mmap.ReadAt(buf[:], off); err != nil {
		return fmt.Errorf("%w: reading overflow header: %v", ErrCorruptOverflow, err)
	}

	var magic [8]byte
	copy(magic[:], buf[0:8])
	if magic != overflowMagicCKD {
		return fmt.Errorf("%w: bad overflow magic", ErrCorruptOverflow)
	}

	ds.overflowCount = binary.LittleEndian.Uint32(buf[8:12])
	ds.overflowDataOff = binary.LittleEndian.Uint32(buf[12:16])

	// Record table starts right after the 16-byte overflow header.
	ds.overflowStart = off + 16
	// Data blob offset is relative to the overflow region start.
	ds.overflowDataStart = off + int64(ds.overflowDataOff)

	if ds.overflowDataStart > ds.fileSize {
		return fmt.Errorf("%w: overflow data start %d beyond file size %d", ErrCorruptOverflow, ds.overflowDataStart, ds.fileSize)
	}

	return nil
}

// Close releases the memory map and closes the file.
func (ds *DataStore) Close() error {
	if err := munmapData(ds.mmap); err != nil {
		ds.f.Close()
		return err
	}
	return ds.f.Close()
}

// Header returns the file header.
func (ds *DataStore) Header() FileHeader {
	return ds.header
}

// Lookup finds a record by fingerprint ID using binary search.
// It checks the overflow table first (if present), then the main table.
func (ds *DataStore) Lookup(id uint32) (*Record, error) {
	// Check overflow first.
	if ds.hasOverflow && ds.overflowCount > 0 {
		rec, err := ds.searchTable(id, ds.overflowStart, int(ds.overflowCount))
		if err == nil {
			rec.fromOverflow = true
			return rec, nil
		}
	}

	// Search main record table.
	count := int(ds.header.Section0Length / recordSize)
	rec, err := ds.searchTable(id, int64(ds.header.Section0Offset), count)
	if err != nil {
		return nil, ErrRecordNotFound
	}
	return rec, nil
}

func (ds *DataStore) searchTable(id uint32, tableStart int64, count int) (*Record, error) {
	lo, hi := 0, count-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		off := tableStart + int64(mid)*recordSize

		var buf [4]byte
		if _, err := ds.mmap.ReadAt(buf[:], off); err != nil {
			return nil, err
		}
		fpID := binary.LittleEndian.Uint32(buf[:])

		if fpID == id {
			return ds.readRecordAt(off)
		} else if fpID < id {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return nil, ErrRecordNotFound
}

func (ds *DataStore) readRecordAt(off int64) (*Record, error) {
	var buf [recordSize]byte
	if _, err := ds.mmap.ReadAt(buf[:], off); err != nil {
		return nil, err
	}

	rec := &Record{
		FingerprintID: binary.LittleEndian.Uint32(buf[0:4]),
		DataOffset:    uint64(binary.LittleEndian.Uint32(buf[4:8])) | uint64(buf[8])<<32 | uint64(buf[9])<<40,
		DataLength:    binary.LittleEndian.Uint16(buf[10:12]),
		DurationMs:    binary.LittleEndian.Uint32(buf[12:16]),
		RawCount:      binary.LittleEndian.Uint16(buf[16:18]),
	}
	return rec, nil
}

// ReadFingerprint reads and decompresses the fingerprint data for a record.
func (ds *DataStore) ReadFingerprint(rec *Record) (*Fingerprint, error) {
	if rec.fromOverflow {
		return ds.readFingerprintFromOverflow(rec)
	}
	absOffset := int64(ds.header.Section1Offset) + int64(rec.DataOffset)
	data := make([]byte, rec.DataLength)
	if _, err := ds.mmap.ReadAt(data, absOffset); err != nil {
		return nil, fmt.Errorf("reading fingerprint data: %w", err)
	}

	values, err := DecompressFingerprint(data, int(rec.RawCount))
	if err != nil {
		return nil, err
	}

	return &Fingerprint{
		ID:         rec.FingerprintID,
		DurationMs: rec.DurationMs,
		Values:     values,
	}, nil
}

// RecordCount returns the number of records in the main table.
func (ds *DataStore) RecordCount() uint64 {
	return ds.header.RecordCount
}

// OverflowCount returns the number of records in the overflow region.
func (ds *DataStore) OverflowCount() uint32 {
	return ds.overflowCount
}

// HasOverflow reports whether the file has an overflow region.
func (ds *DataStore) HasOverflow() bool {
	return ds.hasOverflow
}
