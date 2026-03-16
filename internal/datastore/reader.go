package datastore

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/mmap"
	"github.com/zephyraoss/libchroma/internal/wire"
)

const RecordSize = 20

var overflowMagicCKD = [8]byte{'C', 'K', 'A', 'F', '-', 'D', 'O', 0}

// DataStore provides read access to a .ckd file via memory-mapping.
type DataStore struct {
	F           *os.File
	Mmap        *mmap.Data
	Header      cktype.FileHeader
	Footer      cktype.Footer
	FileSize    int64
	Compression cktype.CompressionMethod
	HasOvfl     bool

	OverflowCount     uint32
	OverflowDataOff   uint32
	OverflowStart     int64 // absolute offset of overflow record table
	OverflowDataStart int64 // absolute offset of overflow data blob
}

// Open opens and validates a .ckd file for reading.
func Open(path string) (*DataStore, error) {
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

	m, err := mmap.MapFile(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	header, err := wire.ReadHeader(m, wire.MagicCKD, fileSize)
	if err != nil {
		mmap.Unmap(m)
		f.Close()
		return nil, err
	}

	if err := wire.ValidateFlags(header.Flags, 0x3); err != nil {
		mmap.Unmap(m)
		f.Close()
		return nil, err
	}

	compression := cktype.CompressVarint
	if header.Flags&1 != 0 {
		mmap.Unmap(m)
		f.Close()
		return nil, fmt.Errorf("ckaf: PFOR compression (flag bit 0) is not yet supported")
	}
	hasOverflow := header.Flags&2 != 0

	footer, err := wire.ReadFooter(m, fileSize, wire.FooterMagicCKD)
	if err != nil {
		mmap.Unmap(m)
		f.Close()
		return nil, err
	}

	ds := &DataStore{
		F:           f,
		Mmap:        m,
		Header:      header,
		Footer:      footer,
		FileSize:    fileSize,
		Compression: compression,
		HasOvfl:     hasOverflow,
	}

	if hasOverflow && footer.OverflowOffset != 0 {
		if err := ds.readOverflowHeader(); err != nil {
			mmap.Unmap(m)
			f.Close()
			return nil, err
		}
	}

	return ds, nil
}

func (ds *DataStore) readOverflowHeader() error {
	off := int64(ds.Footer.OverflowOffset)
	var buf [16]byte
	if _, err := ds.Mmap.ReadAt(buf[:], off); err != nil {
		return fmt.Errorf("%w: reading overflow header: %v", cktype.ErrCorruptOverflow, err)
	}

	var magic [8]byte
	copy(magic[:], buf[0:8])
	if magic != overflowMagicCKD {
		return fmt.Errorf("%w: bad overflow magic", cktype.ErrCorruptOverflow)
	}

	ds.OverflowCount = binary.LittleEndian.Uint32(buf[8:12])
	ds.OverflowDataOff = binary.LittleEndian.Uint32(buf[12:16])
	ds.OverflowStart = off + 16
	ds.OverflowDataStart = off + int64(ds.OverflowDataOff)

	if ds.OverflowDataStart > ds.FileSize {
		return fmt.Errorf("%w: overflow data start %d beyond file size %d", cktype.ErrCorruptOverflow, ds.OverflowDataStart, ds.FileSize)
	}

	return nil
}

// Close releases the memory map and closes the file.
func (ds *DataStore) Close() error {
	if err := mmap.Unmap(ds.Mmap); err != nil {
		ds.F.Close()
		return err
	}
	return ds.F.Close()
}

// Lookup finds a record by fingerprint ID using binary search.
func (ds *DataStore) Lookup(id uint32) (*cktype.Record, error) {
	if ds.HasOvfl && ds.OverflowCount > 0 {
		rec, err := ds.SearchTable(id, ds.OverflowStart, int(ds.OverflowCount))
		if err == nil {
			rec.FromOverflow = true
			return rec, nil
		}
	}

	count := int(ds.Header.Section0Length / RecordSize)
	rec, err := ds.SearchTable(id, int64(ds.Header.Section0Offset), count)
	if err != nil {
		return nil, cktype.ErrRecordNotFound
	}
	return rec, nil
}

// SearchTable performs a binary search within a record table.
func (ds *DataStore) SearchTable(id uint32, tableStart int64, count int) (*cktype.Record, error) {
	lo, hi := 0, count-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		off := tableStart + int64(mid)*RecordSize

		var buf [4]byte
		if _, err := ds.Mmap.ReadAt(buf[:], off); err != nil {
			return nil, err
		}
		fpID := binary.LittleEndian.Uint32(buf[:])

		if fpID == id {
			return ds.ReadRecordAt(off)
		} else if fpID < id {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return nil, cktype.ErrRecordNotFound
}

// ReadRecordAt reads a single record at the given byte offset.
func (ds *DataStore) ReadRecordAt(off int64) (*cktype.Record, error) {
	var buf [RecordSize]byte
	if _, err := ds.Mmap.ReadAt(buf[:], off); err != nil {
		return nil, err
	}

	rec := &cktype.Record{
		FingerprintID: binary.LittleEndian.Uint32(buf[0:4]),
		DataOffset:    uint64(binary.LittleEndian.Uint32(buf[4:8])) | uint64(buf[8])<<32 | uint64(buf[9])<<40,
		DataLength:    binary.LittleEndian.Uint16(buf[10:12]),
		DurationMs:    binary.LittleEndian.Uint32(buf[12:16]),
		RawCount:      binary.LittleEndian.Uint16(buf[16:18]),
	}
	return rec, nil
}

// ReadFingerprint reads and decompresses the fingerprint data for a record.
func (ds *DataStore) ReadFingerprint(rec *cktype.Record) (*cktype.Fingerprint, error) {
	if rec.FromOverflow {
		return ds.ReadFingerprintFromOverflow(rec)
	}
	absOffset := int64(ds.Header.Section1Offset) + int64(rec.DataOffset)
	data := make([]byte, rec.DataLength)
	if _, err := ds.Mmap.ReadAt(data, absOffset); err != nil {
		return nil, fmt.Errorf("reading fingerprint data: %w", err)
	}

	values, err := wire.DecompressFingerprint(data, int(rec.RawCount))
	if err != nil {
		return nil, err
	}

	return &cktype.Fingerprint{
		ID:         rec.FingerprintID,
		DurationMs: rec.DurationMs,
		Values:     values,
	}, nil
}

// ReadFingerprintFromOverflow reads fingerprint data using overflow data offsets.
func (ds *DataStore) ReadFingerprintFromOverflow(rec *cktype.Record) (*cktype.Fingerprint, error) {
	absOffset := ds.OverflowDataStart + int64(rec.DataOffset)
	data := make([]byte, rec.DataLength)
	if _, err := ds.Mmap.ReadAt(data, absOffset); err != nil {
		return nil, fmt.Errorf("reading overflow fingerprint data: %w", err)
	}

	values, err := wire.DecompressFingerprint(data, int(rec.RawCount))
	if err != nil {
		return nil, err
	}

	return &cktype.Fingerprint{
		ID:         rec.FingerprintID,
		DurationMs: rec.DurationMs,
		Values:     values,
	}, nil
}

// RecordCount returns the number of records in the main table.
func (ds *DataStore) RecordCount() uint64 {
	return ds.Header.RecordCount
}
