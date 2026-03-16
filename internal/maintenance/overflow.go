package maintenance

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/datastore"
	"github.com/zephyraoss/libchroma/internal/metadata"
	"github.com/zephyraoss/libchroma/internal/wire"
)

var (
	overflowMagicCKD = [8]byte{'C', 'K', 'A', 'F', '-', 'D', 'O', 0}
	overflowMagicCKX = [8]byte{'C', 'K', 'A', 'F', '-', 'X', 'O', 0}
)

// AppendDataStoreOverflow appends overflow records to a .ckd file.
func AppendDataStoreOverflow(path string, records []cktype.OverflowRecord) error {
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

	sort.Slice(records, func(i, j int) bool {
		return records[i].FingerprintID < records[j].FingerprintID
	})

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
		comp := wire.CompressFingerprint(rec.Values)
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

	recordTableSize := len(records) * datastore.RecordSize
	var dataBlobSize int
	for _, c := range compressed {
		dataBlobSize += len(c.compressed)
	}

	overflowOffset := fileSize - wire.FooterSize
	overflowDataOffset := uint32(16 + recordTableSize)

	var hdr [16]byte
	copy(hdr[0:8], overflowMagicCKD[:])
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(records)))
	binary.LittleEndian.PutUint32(hdr[12:16], overflowDataOffset)
	if _, err := f.WriteAt(hdr[:], overflowOffset); err != nil {
		return err
	}

	tableOffset := overflowOffset + 16
	var dataOff uint64
	for i, c := range compressed {
		var buf [datastore.RecordSize]byte
		binary.LittleEndian.PutUint32(buf[0:4], c.fingerprintID)
		binary.LittleEndian.PutUint32(buf[4:8], uint32(dataOff&0xFFFFFFFF))
		buf[8] = byte(dataOff >> 32)
		buf[9] = byte(dataOff >> 40)
		binary.LittleEndian.PutUint16(buf[10:12], uint16(len(c.compressed)))
		binary.LittleEndian.PutUint32(buf[12:16], c.durationMs)
		binary.LittleEndian.PutUint16(buf[16:18], c.rawCount)
		if _, err := f.WriteAt(buf[:], tableOffset+int64(i)*datastore.RecordSize); err != nil {
			return err
		}
		dataOff += uint64(len(c.compressed))
	}

	dataBlobOffset := tableOffset + int64(recordTableSize)
	var pos int64
	for _, c := range compressed {
		if _, err := f.WriteAt(c.compressed, dataBlobOffset+pos); err != nil {
			return err
		}
		pos += int64(len(c.compressed))
	}

	newFooterOffset := dataBlobOffset + int64(dataBlobSize)
	footer := cktype.Footer{
		OverflowOffset: uint64(overflowOffset),
		Magic:          wire.FooterMagicCKD,
	}
	if err := wire.WriteFooter(f, newFooterOffset, footer); err != nil {
		return err
	}

	if err := wire.SetHeaderFlagBit(f, f, 0x2); err != nil {
		return err
	}

	return nil
}

// AppendSearchIndexOverflow appends overflow posting lists to a .ckx file.
func AppendSearchIndexOverflow(path string, ds *datastore.DataStore, newFingerprintIDs []uint32) error {
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

	header, err := wire.ReadHeader(f, wire.MagicCKX, fileSize)
	if err != nil {
		return err
	}

	var tcBuf [64]byte
	if _, err := f.ReadAt(tcBuf[:], wire.HeaderSize); err != nil {
		return fmt.Errorf("reading tuning config: %w", err)
	}
	numBands := tcBuf[0]
	bitsPerBand := tcBuf[1]
	totalBuckets := binary.LittleEndian.Uint32(tcBuf[0x06:])
	_ = header

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

	bucketDirSize := int64(totalBuckets) * 12
	var postingData []byte
	bucketDir := make([]byte, bucketDirSize)

	var postingOffset uint64
	for b := uint32(0); b < totalBuckets; b++ {
		entries := buckets[b]
		dirOff := int64(b) * 12
		binary.LittleEndian.PutUint64(bucketDir[dirOff:], postingOffset)
		binary.LittleEndian.PutUint32(bucketDir[dirOff+8:], uint32(len(entries)))

		sort.Slice(entries, func(i, j int) bool {
			if entries[i].fingerprintID == entries[j].fingerprintID {
				return entries[i].position < entries[j].position
			}
			return entries[i].fingerprintID < entries[j].fingerprintID
		})

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
				postingData = wire.AppendVarint(postingData, delta)
				var ptmp [2]byte
				binary.LittleEndian.PutUint16(ptmp[:], e.position)
				postingData = append(postingData, ptmp[:]...)
			}
		}
		postingOffset = uint64(len(postingData))
	}

	overflowOffset := fileSize - wire.FooterSize

	var hdr [16]byte
	copy(hdr[0:8], overflowMagicCKX[:])
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(newFingerprintIDs)))
	binary.LittleEndian.PutUint32(hdr[12:16], totalBuckets)
	if _, err := f.WriteAt(hdr[:], overflowOffset); err != nil {
		return err
	}

	dirOffset := overflowOffset + 16
	if _, err := f.WriteAt(bucketDir, dirOffset); err != nil {
		return err
	}

	postingDataOffset := dirOffset + bucketDirSize
	if _, err := f.WriteAt(postingData, postingDataOffset); err != nil {
		return err
	}

	newFooterOffset := postingDataOffset + int64(len(postingData))
	footer := cktype.Footer{
		OverflowOffset: uint64(overflowOffset),
		Magic:          wire.FooterMagicCKX,
	}
	if err := wire.WriteFooter(f, newFooterOffset, footer); err != nil {
		return err
	}

	if err := wire.SetHeaderFlagBit(f, f, 0x2); err != nil {
		return err
	}

	return nil
}

// AppendMetadataOverflow appends overflow mapping records to a .ckm file.
func AppendMetadataOverflow(path string, records []cktype.OverflowMappingRecord) error {
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

	sort.Slice(records, func(i, j int) bool {
		return records[i].FingerprintID < records[j].FingerprintID
	})

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
		entry := metadata.EncodeStringPoolEntry(rec.Metadata)
		refs[i] = stringRef{
			offset: uint32(len(stringPool)),
			length: uint32(len(entry)),
		}
		stringPool = append(stringPool, entry...)
	}

	mappingTableSize := int64(len(records)) * metadata.MappingRecordSize
	overflowStringsOffset := uint32(16 + mappingTableSize)

	overflowOffset := fileSize - wire.FooterSize

	var hdr [16]byte
	copy(hdr[0:8], metadata.OverflowMagicMO[:])
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(records)))
	binary.LittleEndian.PutUint32(hdr[12:16], overflowStringsOffset)
	if _, err := f.WriteAt(hdr[:], overflowOffset); err != nil {
		return err
	}

	tableOffset := overflowOffset + 16
	for i, rec := range records {
		var buf [metadata.MappingRecordSize]byte
		binary.LittleEndian.PutUint32(buf[0x00:], rec.FingerprintID)
		copy(buf[0x04:0x14], rec.MBID[:])
		binary.LittleEndian.PutUint32(buf[0x14:], rec.TrackID)
		binary.LittleEndian.PutUint32(buf[0x18:], refs[i].offset)
		binary.LittleEndian.PutUint32(buf[0x1C:], refs[i].length)
		if _, err := f.WriteAt(buf[:], tableOffset+int64(i)*metadata.MappingRecordSize); err != nil {
			return err
		}
	}

	stringPoolOffset := tableOffset + mappingTableSize
	if len(stringPool) > 0 {
		if _, err := f.WriteAt(stringPool, stringPoolOffset); err != nil {
			return err
		}
	}

	newFooterOffset := stringPoolOffset + int64(len(stringPool))
	footer := cktype.Footer{
		OverflowOffset: uint64(overflowOffset),
		Magic:          wire.FooterMagicCKM,
	}
	if err := wire.WriteFooter(f, newFooterOffset, footer); err != nil {
		return err
	}

	if err := wire.SetHeaderFlagBit(f, f, 0x2); err != nil {
		return err
	}

	return nil
}
