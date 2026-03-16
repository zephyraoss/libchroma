package chroma

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

// Dataset provides unified access to a CKAF dataset consisting of
// a datastore (.ckd), search index (.ckx), and optional metadata map (.ckm).
type Dataset struct {
	ds     *DataStore
	si     *SearchIndex
	mm     *MetadataMap
	prefix string
}

// Open opens a dataset with default options.
func Open(prefix string) (*Dataset, error) {
	return OpenWithOptions(prefix, DatasetOptions{})
}

// OpenWithOptions opens a dataset with the given options.
func OpenWithOptions(prefix string, opts DatasetOptions) (*Dataset, error) {
	ds, err := OpenDataStore(prefix + ".ckd")
	if err != nil {
		return nil, fmt.Errorf("open datastore: %w", err)
	}

	si, err := OpenSearchIndex(prefix + ".ckx")
	if err != nil {
		ds.Close()
		return nil, fmt.Errorf("open search index: %w", err)
	}

	// Metadata file is optional.
	var mm *MetadataMap
	if _, err := os.Stat(prefix + ".ckm"); err == nil {
		mm, err = OpenMetadataMap(prefix + ".ckm")
		if err != nil {
			si.Close()
			ds.Close()
			return nil, fmt.Errorf("open metadata: %w", err)
		}
	}

	// Verify dataset_id consistency across files.
	dsID := ds.Header().DatasetID
	if siID := si.Header().DatasetID; siID != dsID {
		si.Close()
		ds.Close()
		if mm != nil {
			mm.Close()
		}
		return nil, fmt.Errorf("%w: datastore %x vs search index %x", ErrDatasetMismatch, dsID, siID)
	}
	if mm != nil {
		if mmID := mm.Header().DatasetID; mmID != dsID {
			si.Close()
			ds.Close()
			mm.Close()
			return nil, fmt.Errorf("%w: datastore %x vs metadata %x", ErrDatasetMismatch, dsID, mmID)
		}
	}

	return &Dataset{ds: ds, si: si, mm: mm, prefix: prefix}, nil
}

// Close releases all resources.
func (d *Dataset) Close() error {
	var firstErr error
	if d.mm != nil {
		if err := d.mm.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := d.si.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := d.ds.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Query performs a similarity search against the dataset.
func (d *Dataset) Query(fingerprint []uint32, durationMs uint32, opts *QueryOptions) ([]MatchResult, error) {
	return QueryDataset(d.ds, d.si, d.mm, fingerprint, durationMs, opts)
}

// Lookup retrieves a fingerprint by ID.
func (d *Dataset) Lookup(id uint32) (*Fingerprint, error) {
	rec, err := d.ds.Lookup(id)
	if err != nil {
		return nil, err
	}
	return d.ds.ReadFingerprint(rec)
}

// LookupMetadata retrieves metadata for a fingerprint ID.
func (d *Dataset) LookupMetadata(id uint32) (*TrackMetadata, *uuid.UUID, error) {
	if d.mm == nil {
		return nil, nil, nil
	}
	mr, err := d.mm.Lookup(id)
	if err != nil {
		return nil, nil, err
	}
	mbid := mr.MBID
	meta, err := d.mm.ReadMetadata(mr)
	if err != nil {
		return nil, &mbid, err
	}
	return meta, &mbid, nil
}

// NeedsCompaction returns true if the overflow region exceeds the given
// threshold as a percentage of the main record count.
func (d *Dataset) NeedsCompaction(thresholdPct float64) bool {
	mainCount := d.ds.RecordCount()
	if mainCount == 0 {
		return d.ds.HasOverflow()
	}
	overflowCount := float64(d.ds.OverflowCount())
	return overflowCount/float64(mainCount) >= thresholdPct/100.0
}

// Stats returns summary statistics about the dataset.
func (d *Dataset) Stats() DatasetStats {
	stats := DatasetStats{
		RecordCount:   d.ds.RecordCount(),
		HasOverflow:   d.ds.HasOverflow(),
		OverflowCount: d.ds.OverflowCount(),
		TuningConfig:  d.si.Tuning(),
	}
	if d.mm != nil {
		stats.HasMetadata = true
		stats.MetadataCount = d.mm.header.RecordCount
	}
	return stats
}
