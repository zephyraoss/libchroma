package chroma

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/internal/datastore"
	"github.com/zephyraoss/libchroma/internal/maintenance"
	"github.com/zephyraoss/libchroma/internal/metadata"
	"github.com/zephyraoss/libchroma/internal/searchindex"
	"github.com/zephyraoss/libchroma/internal/wire"
)

// Dataset provides unified access to a CKAF dataset consisting of
// a datastore (.ckd), search index (.ckx), and optional metadata map (.ckm).
type Dataset struct {
	ds     *datastore.DataStore
	si     *searchindex.SearchIndex
	mm     *metadata.MetadataMap
	prefix string
}

// Open opens a dataset with default options.
func Open(prefix string) (*Dataset, error) {
	return OpenWithOptions(prefix, DatasetOptions{})
}

// OpenWithOptions opens a dataset with the given options.
func OpenWithOptions(prefix string, opts DatasetOptions) (*Dataset, error) {
	ds, err := datastore.Open(prefix + ".ckd")
	if err != nil {
		return nil, fmt.Errorf("open datastore: %w", err)
	}

	si, err := searchindex.Open(prefix + ".ckx")
	if err != nil {
		ds.Close()
		return nil, fmt.Errorf("open search index: %w", err)
	}

	var mm *metadata.MetadataMap
	if _, err := os.Stat(prefix + ".ckm"); err == nil {
		mm, err = metadata.Open(prefix + ".ckm")
		if err != nil {
			si.Close()
			ds.Close()
			return nil, fmt.Errorf("open metadata: %w", err)
		}
	}

	dsID := ds.Header.DatasetID
	if siID := si.Header.DatasetID; siID != dsID {
		si.Close()
		ds.Close()
		if mm != nil {
			mm.Close()
		}
		return nil, fmt.Errorf("%w: datastore %x vs search index %x", ErrDatasetMismatch, dsID, siID)
	}
	if mm != nil {
		if mmID := mm.Header.DatasetID; mmID != dsID {
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
	return queryDataset(d.ds, d.si, d.mm, fingerprint, durationMs, opts)
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
		return d.ds.HasOvfl
	}
	overflowCount := float64(d.ds.OverflowCount)
	return overflowCount/float64(mainCount) >= thresholdPct/100.0
}

// Stats returns summary statistics about the dataset.
func (d *Dataset) Stats() DatasetStats {
	stats := DatasetStats{
		RecordCount:   d.ds.RecordCount(),
		HasOverflow:   d.ds.HasOvfl,
		OverflowCount: d.ds.OverflowCount,
		TuningConfig:  d.si.Tuning,
	}
	if d.mm != nil {
		stats.HasMetadata = true
		stats.MetadataCount = d.mm.Header.RecordCount
	}
	return stats
}

// --- Delegated constructors for sub-components ---

// DataStore provides read access to a .ckd file via memory-mapping.
type DataStore = datastore.DataStore

// SearchIndex provides read access to a .ckx file via memory-mapping.
type SearchIndex = searchindex.SearchIndex

// MetadataMap provides read access to a .ckm file.
type MetadataMap = metadata.MetadataMap

// DataStoreBuilder constructs a .ckd file.
type DataStoreBuilder = datastore.Builder

// SearchIndexBuilder constructs a .ckx file.
type SearchIndexBuilder = searchindex.Builder

// MetadataMapBuilder constructs a .ckm file.
type MetadataMapBuilder = metadata.Builder

// OpenDataStore opens and validates a .ckd file for reading.
func OpenDataStore(path string) (*DataStore, error) {
	return datastore.Open(path)
}

// OpenSearchIndex opens and validates a .ckx file for reading.
func OpenSearchIndex(path string) (*SearchIndex, error) {
	return searchindex.Open(path)
}

// OpenMetadataMap opens and validates a .ckm file for reading.
func OpenMetadataMap(path string) (*MetadataMap, error) {
	return metadata.Open(path)
}

// NewDataStoreBuilder creates a new DataStoreBuilder that writes to the given path.
func NewDataStoreBuilder(path string, compression CompressionMethod) (*DataStoreBuilder, error) {
	return datastore.NewBuilder(path, compression)
}

// NewSearchIndexBuilder creates a new builder that writes a .ckx file at path.
func NewSearchIndexBuilder(path string, compression CompressionMethod) (*SearchIndexBuilder, error) {
	return searchindex.NewBuilder(path, compression)
}

// NewMetadataMapBuilder creates a new builder that writes a .ckm file at path.
func NewMetadataMapBuilder(path string, includeText bool) (*MetadataMapBuilder, error) {
	return metadata.NewBuilder(path, includeText)
}

// CompressFingerprint compresses sub-fingerprint values using XOR-delta + varint encoding.
func CompressFingerprint(values []uint32) []byte {
	return wire.CompressFingerprint(values)
}

// DecompressFingerprint decompresses XOR-delta + varint encoded fingerprint data.
func DecompressFingerprint(data []byte, rawCount int) ([]uint32, error) {
	return wire.DecompressFingerprint(data, rawCount)
}

// CompressFingerprintPFOR compresses using XOR-delta + PFOR bitpacking.
func CompressFingerprintPFOR(values []uint32) ([]byte, error) {
	return wire.CompressFingerprintPFOR(values)
}

// DecompressFingerprintPFOR decompresses XOR-delta + PFOR encoded data.
func DecompressFingerprintPFOR(data []byte, rawCount int) ([]uint32, error) {
	return wire.DecompressFingerprintPFOR(data, rawCount)
}

// AutoTuneParams selects LSH parameters based on dataset size and constraints.
func AutoTuneParams(recordCount uint64, strategy TuningStrategy, availableRAM, storageBudget uint64) TuningConfig {
	return maintenance.AutoTuneParams(recordCount, strategy, availableRAM, storageBudget)
}

// AppendDataStoreOverflow appends overflow records to a .ckd file.
func AppendDataStoreOverflow(path string, records []OverflowRecord) error {
	return maintenance.AppendDataStoreOverflow(path, records)
}

// AppendSearchIndexOverflow appends overflow posting lists to a .ckx file.
func AppendSearchIndexOverflow(path string, ds *DataStore, newFingerprintIDs []uint32) error {
	return maintenance.AppendSearchIndexOverflow(path, ds, newFingerprintIDs)
}

// AppendMetadataOverflow appends overflow mapping records to a .ckm file.
func AppendMetadataOverflow(path string, records []OverflowMappingRecord) error {
	return maintenance.AppendMetadataOverflow(path, records)
}

// CompactDataStore merges main and overflow records into a new clean .ckd file.
func CompactDataStore(srcPath, dstPath string) error {
	return maintenance.CompactDataStore(srcPath, dstPath)
}

// CompactSearchIndex rebuilds a search index from a compacted datastore.
func CompactSearchIndex(srcPath, dstPath string, ds *DataStore) error {
	return maintenance.CompactSearchIndex(srcPath, dstPath, ds)
}

// CompactMetadata merges main and overflow mapping records into a new .ckm file.
func CompactMetadata(srcPath, dstPath string) error {
	return maintenance.CompactMetadata(srcPath, dstPath)
}

// CompactDataset performs a full compaction of all dataset files.
func CompactDataset(prefix string) error {
	return maintenance.CompactDataset(prefix)
}
