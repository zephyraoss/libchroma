package chroma

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

var overflowMagicMO = [8]byte{'C', 'K', 'A', 'F', '-', 'M', 'O', 0}

// AppendDataStoreOverflow appends overflow records to a .ckd file.
func AppendDataStoreOverflow(path string, records []OverflowRecord) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening datastore for overflow: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	// Sort records by FingerprintID.
	sort.Slice(records, func(i, j int) bool {
		return records[i].FingerprintID < records[j].FingerprintID
	})

	// Compress each record's values.
	type compressedRec struct {
		fingerprintID uint32
		durationMs    uint32
		compressed    []byte
		rawCount      uint16
	}
	compressed := make([]compressedRec, len(records))
	for i, rec := range records {
		if len(rec.Values) > 0xFFFF {
			return fmt.Errorf("ckaf: overflow fingerprint %d has %d sub-fingerprints, exceeds u16 max", rec.FingerprintID, len(rec.Values))
		}
		comp := CompressFingerprint(rec.Values)
		if len(comp) > 0xFFFF {
			return fmt.Errorf("ckaf: overflow fingerprint %d compressed to %d bytes, exceeds u16 max", rec.FingerprintID, len(comp))
		}
		compressed[i] = compressedRec{
			fingerprintID: rec.FingerprintID,
			durationMs:    rec.DurationMs,
			compressed:    comp,
			rawCount:      uint16(len(rec.Values)),
		}
	}

	// Compute sizes.
	recordTableSize := len(records) * recordSize
	var dataBlobSize int
	for _, c := range compressed {
		dataBlobSize += len(c.compressed)
	}

	// Overflow starts where the current footer is.
	overflowOffset := fileSize - FooterSize

	// overflow_data_offset: from overflow header start to data blob.
	// Layout: 16-byte header + record table + data blob.
	overflowDataOffset := uint32(16 + recordTableSize)

	// Write overflow header (16 bytes).
	var hdr [16]byte
	copy(hdr[0:8], overflowMagicCKD[:])
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(records)))
	binary.LittleEndian.PutUint32(hdr[12:16], overflowDataOffset)
	if _, err := f.WriteAt(hdr[:], overflowOffset); err != nil {
		return err
	}

	// Write overflow record table.
	tableOffset := overflowOffset + 16
	var dataOff uint64
	for i, c := range compressed {
		var buf [recordSize]byte
		binary.LittleEndian.PutUint32(buf[0:4], c.fingerprintID)
		// u48 data_offset
		binary.LittleEndian.PutUint32(buf[4:8], uint32(dataOff&0xFFFFFFFF))
		buf[8] = byte(dataOff >> 32)
		buf[9] = byte(dataOff >> 40)
		binary.LittleEndian.PutUint16(buf[10:12], uint16(len(c.compressed)))
		binary.LittleEndian.PutUint32(buf[12:16], c.durationMs)
		binary.LittleEndian.PutUint16(buf[16:18], c.rawCount)
		if _, err := f.WriteAt(buf[:], tableOffset+int64(i)*recordSize); err != nil {
			return err
		}
		dataOff += uint64(len(c.compressed))
	}

	// Write overflow data blob.
	dataBlobOffset := tableOffset + int64(recordTableSize)
	var pos int64
	for _, c := range compressed {
		if _, err := f.WriteAt(c.compressed, dataBlobOffset+pos); err != nil {
			return err
		}
		pos += int64(len(c.compressed))
	}

	// Write new footer.
	newFooterOffset := dataBlobOffset + int64(dataBlobSize)
	footer := Footer{
		OverflowOffset: uint64(overflowOffset),
		Magic:          FooterMagicCKD,
	}
	if err := WriteFooter(f, newFooterOffset, footer); err != nil {
		return err
	}

	// Update header flags: set bit 1 (has_overflow).
	if err := setHeaderFlagBit(f, 0x2); err != nil {
		return err
	}

	return nil
}

// AppendSearchIndexOverflow appends overflow posting lists to a .ckx file.
func AppendSearchIndexOverflow(path string, ds *DataStore, newFingerprintIDs []uint32) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening search index for overflow: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	// Read header.
	header, err := ReadHeader(f, MagicCKX, fileSize)
	if err != nil {
		return err
	}

	// Read tuning config (64 bytes at offset 96, right after the header).
	var tcBuf [64]byte
	if _, err := f.ReadAt(tcBuf[:], HeaderSize); err != nil {
		return fmt.Errorf("reading tuning config: %w", err)
	}
	numBands := tcBuf[0]
	bitsPerBand := tcBuf[1]
	totalBuckets := binary.LittleEndian.Uint32(tcBuf[0x06:])
	_ = header // header used above

	// Build posting lists for each bucket.
	type posting struct {
		fingerprintID uint32
		position      uint16
	}
	buckets := make(map[uint32][]posting)

	bucketsPerBand := uint32(1) << bitsPerBand

	for _, fpID := range newFingerprintIDs {
		rec, err := ds.Lookup(fpID)
		if err != nil {
			return fmt.Errorf("looking up fingerprint %d: %w", fpID, err)
		}
		fp, err := ds.ReadFingerprint(rec)
		if err != nil {
			return fmt.Errorf("reading fingerprint %d: %w", fpID, err)
		}

		// Extract bands from fingerprint values.
		for pos, val := range fp.Values {
			for band := uint8(0); band < numBands; band++ {
				shift := uint(band) * uint(bitsPerBand)
				mask := uint32(bucketsPerBand - 1)
				bucketLocal := (val >> shift) & mask
				bucketGlobal := uint32(band)*bucketsPerBand + bucketLocal
				buckets[bucketGlobal] = append(buckets[bucketGlobal], posting{
					fingerprintID: fpID,
					position:      uint16(pos),
				})
			}
		}
	}

	// Build overflow bucket directory and posting data.
	// Bucket directory: 12 bytes per bucket (u64 posting_offset + u32 posting_count).
	bucketDirSize := int64(totalBuckets) * 12
	var postingData []byte
	bucketDir := make([]byte, bucketDirSize)

	var postingOffset uint64
	for b := uint32(0); b < totalBuckets; b++ {
		entries := buckets[b]
		dirOff := int64(b) * 12
		binary.LittleEndian.PutUint64(bucketDir[dirOff:], postingOffset)
		binary.LittleEndian.PutUint32(bucketDir[dirOff+8:], uint32(len(entries)))

		// Sort postings by fingerprintID for delta encoding.
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].fingerprintID == entries[j].fingerprintID {
				return entries[i].position < entries[j].position
			}
			return entries[i].fingerprintID < entries[j].fingerprintID
		})

		// Encode postings: first entry raw u32+u16, rest delta varint + raw u16.
		for idx, e := range entries {
			if idx == 0 {
				var tmp [4]byte
				binary.LittleEndian.PutUint32(tmp[:], e.fingerprintID)
				postingData = append(postingData, tmp[:]...)
				var ptmp [2]byte
				binary.LittleEndian.PutUint16(ptmp[:], e.position)
				postingData = append(postingData, ptmp[:]...)
			} else {
				delta := e.fingerprintID - entries[idx-1].fingerprintID
				postingData = appendVarint(postingData, delta)
				var ptmp [2]byte
				binary.LittleEndian.PutUint16(ptmp[:], e.position)
				postingData = append(postingData, ptmp[:]...)
			}
		}
		postingOffset = uint64(len(postingData))
	}

	// Overflow starts where the current footer is.
	overflowOffset := fileSize - FooterSize

	// Write overflow header (16 bytes).
	var hdr [16]byte
	copy(hdr[0:8], overflowMagicCKX[:])
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(newFingerprintIDs)))
	binary.LittleEndian.PutUint32(hdr[12:16], totalBuckets)
	if _, err := f.WriteAt(hdr[:], overflowOffset); err != nil {
		return err
	}

	// Write bucket directory.
	dirOffset := overflowOffset + 16
	if _, err := f.WriteAt(bucketDir, dirOffset); err != nil {
		return err
	}

	// Write posting data.
	postingDataOffset := dirOffset + bucketDirSize
	if _, err := f.WriteAt(postingData, postingDataOffset); err != nil {
		return err
	}

	// Write new footer.
	newFooterOffset := postingDataOffset + int64(len(postingData))
	footer := Footer{
		OverflowOffset: uint64(overflowOffset),
		Magic:          FooterMagicCKX,
	}
	if err := WriteFooter(f, newFooterOffset, footer); err != nil {
		return err
	}

	// Update header flags: set bit 1 (has_overflow).
	if err := setHeaderFlagBit(f, 0x2); err != nil {
		return err
	}

	return nil
}

// AppendMetadataOverflow appends overflow mapping records to a .ckm file.
func AppendMetadataOverflow(path string, records []OverflowMappingRecord) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening metadata map for overflow: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	// Sort records by FingerprintID.
	sort.Slice(records, func(i, j int) bool {
		return records[i].FingerprintID < records[j].FingerprintID
	})

	// Build string pool.
	var stringPool []byte
	type stringRef struct {
		offset uint32
		length uint32
	}
	refs := make([]stringRef, len(records))
	for i, rec := range records {
		if rec.Metadata == nil {
			refs[i] = stringRef{offset: 0xFFFFFFFF, length: 0}
			continue
		}
		entry := encodeStringPoolEntry(rec.Metadata)
		refs[i] = stringRef{
			offset: uint32(len(stringPool)),
			length: uint32(len(entry)),
		}
		stringPool = append(stringPool, entry...)
	}

	// Compute layout.
	mappingTableSize := int64(len(records)) * mappingRecordSize
	overflowStringsOffset := uint32(16 + mappingTableSize)

	// Overflow starts where the current footer is.
	overflowOffset := fileSize - FooterSize

	// Write overflow header (16 bytes).
	var hdr [16]byte
	copy(hdr[0:8], overflowMagicMO[:])
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(records)))
	binary.LittleEndian.PutUint32(hdr[12:16], overflowStringsOffset)
	if _, err := f.WriteAt(hdr[:], overflowOffset); err != nil {
		return err
	}

	// Write overflow mapping table.
	tableOffset := overflowOffset + 16
	for i, rec := range records {
		var buf [mappingRecordSize]byte
		binary.LittleEndian.PutUint32(buf[0x00:], rec.FingerprintID)
		copy(buf[0x04:0x14], rec.MBID[:])
		binary.LittleEndian.PutUint32(buf[0x14:], rec.TrackID)
		binary.LittleEndian.PutUint32(buf[0x18:], refs[i].offset)
		binary.LittleEndian.PutUint32(buf[0x1C:], refs[i].length)
		if _, err := f.WriteAt(buf[:], tableOffset+int64(i)*mappingRecordSize); err != nil {
			return err
		}
	}

	// Write overflow string pool.
	stringPoolOffset := tableOffset + mappingTableSize
	if len(stringPool) > 0 {
		if _, err := f.WriteAt(stringPool, stringPoolOffset); err != nil {
			return err
		}
	}

	// Write new footer.
	newFooterOffset := stringPoolOffset + int64(len(stringPool))
	footer := Footer{
		OverflowOffset: uint64(overflowOffset),
		Magic:          FooterMagicCKM,
	}
	if err := WriteFooter(f, newFooterOffset, footer); err != nil {
		return err
	}

	// Update header flags: set bit 1 (has_overflow).
	if err := setHeaderFlagBit(f, 0x2); err != nil {
		return err
	}

	return nil
}

// setHeaderFlagBit reads the current flags from offset 0x0C, ORs with the given
// bits, and writes them back.
func setHeaderFlagBit(f *os.File, bits uint32) error {
	var buf [4]byte
	if _, err := f.ReadAt(buf[:], 0x0C); err != nil {
		return fmt.Errorf("reading flags: %w", err)
	}
	flags := binary.LittleEndian.Uint32(buf[:])
	flags |= bits
	binary.LittleEndian.PutUint32(buf[:], flags)
	if _, err := f.WriteAt(buf[:], 0x0C); err != nil {
		return fmt.Errorf("writing flags: %w", err)
	}
	return nil
}
