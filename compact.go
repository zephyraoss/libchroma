package chroma

import (
	"encoding/binary"
	"fmt"
	"os"
)

// CompactDataStore merges main and overflow records into a new clean .ckd file.
func CompactDataStore(srcPath, dstPath string) error {
	src, err := OpenDataStore(srcPath)
	if err != nil {
		return fmt.Errorf("opening source datastore: %w", err)
	}
	defer src.Close()

	dst, err := NewDataStoreBuilder(dstPath, src.compression)
	if err != nil {
		return fmt.Errorf("creating destination datastore: %w", err)
	}
	dst.SetDatasetID(src.header.DatasetID)
	dst.SetSourceDate(src.header.SourceDate)

	// Read all main records.
	mainCount := int(src.header.Section0Length / recordSize)
	mainRecs := make([]Record, 0, mainCount)
	for i := 0; i < mainCount; i++ {
		off := int64(src.header.Section0Offset) + int64(i)*recordSize
		rec, err := src.readRecordAt(off)
		if err != nil {
			return fmt.Errorf("reading main record %d: %w", i, err)
		}
		mainRecs = append(mainRecs, *rec)
	}

	// Read all overflow records.
	var overflowRecs []Record
	if src.hasOverflow && src.overflowCount > 0 {
		overflowRecs = make([]Record, 0, src.overflowCount)
		for i := 0; i < int(src.overflowCount); i++ {
			off := src.overflowStart + int64(i)*recordSize
			rec, err := src.readRecordAt(off)
			if err != nil {
				return fmt.Errorf("reading overflow record %d: %w", i, err)
			}
			overflowRecs = append(overflowRecs, *rec)
		}
	}

	// Merge-sort by fingerprint_id, overflow wins on duplicates.
	overflowSet := make(map[uint32]struct{}, len(overflowRecs))
	for _, rec := range overflowRecs {
		overflowSet[rec.FingerprintID] = struct{}{}
	}

	// Add overflow records first (they take priority).
	for i := range overflowRecs {
		fp, err := src.readFingerprintFromOverflow(&overflowRecs[i])
		if err != nil {
			return fmt.Errorf("reading overflow fingerprint %d: %w", overflowRecs[i].FingerprintID, err)
		}
		if err := dst.Add(fp.ID, fp.DurationMs, fp.Values); err != nil {
			return err
		}
	}

	// Add main records, skipping duplicates.
	for i := range mainRecs {
		if _, dup := overflowSet[mainRecs[i].FingerprintID]; dup {
			continue
		}
		fp, err := src.ReadFingerprint(&mainRecs[i])
		if err != nil {
			return fmt.Errorf("reading main fingerprint %d: %w", mainRecs[i].FingerprintID, err)
		}
		if err := dst.Add(fp.ID, fp.DurationMs, fp.Values); err != nil {
			return err
		}
	}

	return dst.Finish()
}

// readFingerprintFromOverflow reads fingerprint data using overflow data offsets.
func (ds *DataStore) readFingerprintFromOverflow(rec *Record) (*Fingerprint, error) {
	absOffset := ds.overflowDataStart + int64(rec.DataOffset)
	data := make([]byte, rec.DataLength)
	if _, err := ds.mmap.ReadAt(data, absOffset); err != nil {
		return nil, fmt.Errorf("reading overflow fingerprint data: %w", err)
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

// CompactSearchIndex rebuilds a search index from a compacted datastore.
func CompactSearchIndex(srcPath, dstPath string, ds *DataStore) error {
	src, err := OpenSearchIndex(srcPath)
	if err != nil {
		return fmt.Errorf("opening source search index: %w", err)
	}
	tuning := src.Tuning()
	datasetID := src.Header().DatasetID
	src.Close()

	builder, err := NewSearchIndexBuilder(dstPath, CompressVarint)
	if err != nil {
		return fmt.Errorf("creating search index builder: %w", err)
	}
	builder.SetDatasetID(datasetID)
	builder.SetTuningConfig(tuning)

	if err := builder.BuildFrom(ds); err != nil {
		return fmt.Errorf("building search index: %w", err)
	}

	return builder.Finish()
}

// CompactMetadata merges main and overflow mapping records into a new .ckm file.
func CompactMetadata(srcPath, dstPath string) error {
	src, err := OpenMetadataMap(srcPath)
	if err != nil {
		return fmt.Errorf("opening source metadata: %w", err)
	}
	defer src.Close()

	includeText := src.HasTextMetadata()
	builder, err := NewMetadataMapBuilder(dstPath, includeText)
	if err != nil {
		return fmt.Errorf("creating metadata builder: %w", err)
	}
	builder.SetDatasetID(src.header.DatasetID)

	// Read main mapping records.
	mainCount := int(src.header.RecordCount)
	type mappingEntry struct {
		rec  MappingRecord
		meta *TrackMetadata
	}

	// Build overflow set for dedup.
	overflowSet := make(map[uint32]struct{})
	if src.footer.OverflowOffset != 0 {
		overflowCount, overflowStart, err := readMetadataOverflowHeader(src)
		if err != nil {
			return err
		}

		// Add overflow records.
		for i := 0; i < int(overflowCount); i++ {
			rec, err := readMappingRecordAt(src.mmap, overflowStart+int64(i)*mappingRecordSize)
			if err != nil {
				return fmt.Errorf("reading overflow mapping %d: %w", i, err)
			}
			overflowSet[rec.FingerprintID] = struct{}{}
			var meta *TrackMetadata
			if includeText {
				meta, _ = readOverflowMetadata(src, rec)
			}
			if err := builder.Add(rec.FingerprintID, rec.MBID, rec.TrackID, meta); err != nil {
				return err
			}
		}
	}

	// Add main records, skipping duplicates.
	for i := 0; i < mainCount; i++ {
		off := int64(src.header.Section0Offset) + int64(i)*mappingRecordSize
		rec, err := readMappingRecordAt(src.mmap, off)
		if err != nil {
			return fmt.Errorf("reading main mapping %d: %w", i, err)
		}
		if _, dup := overflowSet[rec.FingerprintID]; dup {
			continue
		}
		var meta *TrackMetadata
		if includeText {
			meta, _ = src.ReadMetadata(rec)
		}
		if err := builder.Add(rec.FingerprintID, rec.MBID, rec.TrackID, meta); err != nil {
			return err
		}
	}

	return builder.Finish()
}

func readMappingRecordAt(m *mmapData, off int64) (*MappingRecord, error) {
	var buf [mappingRecordSize]byte
	if _, err := m.ReadAt(buf[:], off); err != nil {
		return nil, err
	}
	rec := &MappingRecord{
		FingerprintID: binary.LittleEndian.Uint32(buf[0x00:]),
		TrackID:       binary.LittleEndian.Uint32(buf[0x14:]),
		StringOffset:  binary.LittleEndian.Uint32(buf[0x18:]),
		StringLength:  binary.LittleEndian.Uint32(buf[0x1C:]),
	}
	copy(rec.MBID[:], buf[0x04:0x14])
	return rec, nil
}

func readMetadataOverflowHeader(m *MetadataMap) (count uint32, recordStart int64, err error) {
	off := int64(m.footer.OverflowOffset)
	var buf [16]byte
	if _, err := m.mmap.ReadAt(buf[:], off); err != nil {
		return 0, 0, fmt.Errorf("%w: reading metadata overflow header: %v", ErrCorruptOverflow, err)
	}

	var magic [8]byte
	copy(magic[:], buf[0:8])
	if magic != overflowMagicMO {
		return 0, 0, fmt.Errorf("%w: bad metadata overflow magic", ErrCorruptOverflow)
	}

	count = binary.LittleEndian.Uint32(buf[8:12])
	recordStart = off + 16
	return count, recordStart, nil
}

func readOverflowMetadata(m *MetadataMap, rec *MappingRecord) (*TrackMetadata, error) {
	if rec.StringOffset == 0xFFFFFFFF {
		return nil, nil
	}
	// The overflow strings_offset is relative to overflow string pool start,
	// which we compute from the overflow header.
	off := int64(m.footer.OverflowOffset)
	var hdr [16]byte
	if _, err := m.mmap.ReadAt(hdr[:], off); err != nil {
		return nil, err
	}
	stringsOff := binary.LittleEndian.Uint32(hdr[12:16])
	absOffset := off + int64(stringsOff) + int64(rec.StringOffset)

	data := make([]byte, rec.StringLength)
	if _, err := m.mmap.ReadAt(data, absOffset); err != nil {
		return nil, err
	}
	return parseStringPoolEntry(data), nil
}

// CompactDataset performs a full compaction of all dataset files.
func CompactDataset(prefix string) error {
	// Compact datastore.
	if err := CompactDataStore(prefix+".ckd", prefix+".ckd.tmp"); err != nil {
		return fmt.Errorf("compacting datastore: %w", err)
	}

	// Open the new compacted datastore for index rebuilding.
	newDS, err := OpenDataStore(prefix + ".ckd.tmp")
	if err != nil {
		return fmt.Errorf("opening compacted datastore: %w", err)
	}

	// Compact search index.
	if err := CompactSearchIndex(prefix+".ckx", prefix+".ckx.tmp", newDS); err != nil {
		newDS.Close()
		return fmt.Errorf("compacting search index: %w", err)
	}
	newDS.Close()

	// Compact metadata (optional file).
	if _, err := os.Stat(prefix + ".ckm"); err == nil {
		if err := CompactMetadata(prefix+".ckm", prefix+".ckm.tmp"); err != nil {
			return fmt.Errorf("compacting metadata: %w", err)
		}
	}

	// Atomically replace originals.
	if err := os.Rename(prefix+".ckd.tmp", prefix+".ckd"); err != nil {
		return fmt.Errorf("renaming compacted datastore: %w", err)
	}
	if err := os.Rename(prefix+".ckx.tmp", prefix+".ckx"); err != nil {
		return fmt.Errorf("renaming compacted search index: %w", err)
	}
	if _, err := os.Stat(prefix + ".ckm.tmp"); err == nil {
		if err := os.Rename(prefix+".ckm.tmp", prefix+".ckm"); err != nil {
			return fmt.Errorf("renaming compacted metadata: %w", err)
		}
	}

	return nil
}
