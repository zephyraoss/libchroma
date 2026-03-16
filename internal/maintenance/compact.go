package maintenance

import (
	"fmt"
	"os"

	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/datastore"
	"github.com/zephyraoss/libchroma/internal/metadata"
	"github.com/zephyraoss/libchroma/internal/searchindex"
)

// CompactDataStore merges main and overflow records into a new clean .ckd file.
func CompactDataStore(srcPath, dstPath string) error {
	src, err := datastore.Open(srcPath)
	if err != nil {
		return fmt.Errorf("opening source datastore: %w", err)
	}
	defer src.Close()

	dst, err := datastore.NewBuilder(dstPath, src.Compression)
	if err != nil {
		return fmt.Errorf("creating destination datastore: %w", err)
	}
	dst.SetDatasetID(src.Header.DatasetID)
	dst.SetSourceDate(src.Header.SourceDate)

	mainCount := int(src.Header.Section0Length / datastore.RecordSize)
	mainRecs := make([]cktype.Record, 0, mainCount)
	for i := 0; i < mainCount; i++ {
		off := int64(src.Header.Section0Offset) + int64(i)*datastore.RecordSize
		rec, err := src.ReadRecordAt(off)
		if err != nil {
			return fmt.Errorf("reading main record %d: %w", i, err)
		}
		mainRecs = append(mainRecs, *rec)
	}

	var overflowRecs []cktype.Record
	if src.HasOvfl && src.OverflowCount > 0 {
		overflowRecs = make([]cktype.Record, 0, src.OverflowCount)
		for i := 0; i < int(src.OverflowCount); i++ {
			off := src.OverflowStart + int64(i)*datastore.RecordSize
			rec, err := src.ReadRecordAt(off)
			if err != nil {
				return fmt.Errorf("reading overflow record %d: %w", i, err)
			}
			overflowRecs = append(overflowRecs, *rec)
		}
	}

	overflowSet := make(map[uint32]struct{}, len(overflowRecs))
	for _, rec := range overflowRecs {
		overflowSet[rec.FingerprintID] = struct{}{}
	}

	for i := range overflowRecs {
		fp, err := src.ReadFingerprintFromOverflow(&overflowRecs[i])
		if err != nil {
			return fmt.Errorf("reading overflow fingerprint %d: %w", overflowRecs[i].FingerprintID, err)
		}
		if err := dst.Add(fp.ID, fp.DurationMs, fp.Values); err != nil {
			return err
		}
	}

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

// CompactSearchIndex rebuilds a search index from a compacted datastore.
func CompactSearchIndex(srcPath, dstPath string, ds *datastore.DataStore) error {
	src, err := searchindex.Open(srcPath)
	if err != nil {
		return fmt.Errorf("opening source search index: %w", err)
	}
	tuning := src.Tuning
	datasetID := src.Header.DatasetID
	src.Close()

	builder, err := searchindex.NewBuilder(dstPath, cktype.CompressVarint)
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
	src, err := metadata.Open(srcPath)
	if err != nil {
		return fmt.Errorf("opening source metadata: %w", err)
	}
	defer src.Close()

	includeText := src.HasTextMetadata()
	builder, err := metadata.NewBuilder(dstPath, includeText)
	if err != nil {
		return fmt.Errorf("creating metadata builder: %w", err)
	}
	builder.SetDatasetID(src.Header.DatasetID)

	mainCount := int(src.Header.RecordCount)

	overflowSet := make(map[uint32]struct{})
	if src.Footer.OverflowOffset != 0 {
		overflowCount, overflowStart, err := metadata.ReadOverflowHeader(src)
		if err != nil {
			return err
		}

		for i := 0; i < int(overflowCount); i++ {
			rec, err := metadata.ReadMappingRecordAt(src.Mmap, overflowStart+int64(i)*metadata.MappingRecordSize)
			if err != nil {
				return fmt.Errorf("reading overflow mapping %d: %w", i, err)
			}
			overflowSet[rec.FingerprintID] = struct{}{}
			var meta *cktype.TrackMetadata
			if includeText {
				meta, _ = metadata.ReadOverflowMetadata(src, rec)
			}
			if err := builder.Add(rec.FingerprintID, rec.MBID, rec.TrackID, meta); err != nil {
				return err
			}
		}
	}

	for i := 0; i < mainCount; i++ {
		off := int64(src.Header.Section0Offset) + int64(i)*metadata.MappingRecordSize
		rec, err := metadata.ReadMappingRecordAt(src.Mmap, off)
		if err != nil {
			return fmt.Errorf("reading main mapping %d: %w", i, err)
		}
		if _, dup := overflowSet[rec.FingerprintID]; dup {
			continue
		}
		var meta *cktype.TrackMetadata
		if includeText {
			meta, _ = src.ReadMetadata(rec)
		}
		if err := builder.Add(rec.FingerprintID, rec.MBID, rec.TrackID, meta); err != nil {
			return err
		}
	}

	return builder.Finish()
}

// CompactDataset performs a full compaction of all dataset files.
func CompactDataset(prefix string) error {
	if err := CompactDataStore(prefix+".ckd", prefix+".ckd.tmp"); err != nil {
		return fmt.Errorf("compacting datastore: %w", err)
	}

	newDS, err := datastore.Open(prefix + ".ckd.tmp")
	if err != nil {
		return fmt.Errorf("opening compacted datastore: %w", err)
	}

	if err := CompactSearchIndex(prefix+".ckx", prefix+".ckx.tmp", newDS); err != nil {
		newDS.Close()
		return fmt.Errorf("compacting search index: %w", err)
	}
	newDS.Close()

	if _, err := os.Stat(prefix + ".ckm"); err == nil {
		if err := CompactMetadata(prefix+".ckm", prefix+".ckm.tmp"); err != nil {
			return fmt.Errorf("compacting metadata: %w", err)
		}
	}

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
