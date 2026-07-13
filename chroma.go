package chroma

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/v2/internal/datastore"
	"github.com/zephyraoss/libchroma/v2/internal/maintenance"
	"github.com/zephyraoss/libchroma/v2/internal/metadata"
	"github.com/zephyraoss/libchroma/v2/internal/postingindex"
	"github.com/zephyraoss/libchroma/v2/internal/searchindex"
	"github.com/zephyraoss/libchroma/v2/internal/wire"
)

type Dataset struct {
	ds     *datastore.DataStore
	si     *searchindex.SearchIndex
	mm     *metadata.MetadataMap
	pi     *postingindex.PostingIndex
	prefix string
}

func Open(prefix string) (*Dataset, error) {
	return OpenWithOptions(prefix, DatasetOptions{})
}

func OpenWithOptions(prefix string, opts DatasetOptions) (*Dataset, error) {
	var (
		ds *datastore.DataStore
		si *searchindex.SearchIndex
		mm *metadata.MetadataMap
		pi *postingindex.PostingIndex
	)
	closeAll := func() {
		if pi != nil {
			pi.Close()
		}
		if mm != nil {
			mm.Close()
		}
		if si != nil {
			si.Close()
		}
		if ds != nil {
			ds.Close()
		}
	}

	if _, err := os.Stat(prefix + ".ckd"); err == nil {
		ds, err = datastore.Open(prefix + ".ckd")
		if err != nil {
			return nil, fmt.Errorf("open datastore: %w", err)
		}
	}

	if _, err := os.Stat(prefix + ".ckx"); err == nil {
		si, err = searchindex.Open(prefix + ".ckx")
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("open search index: %w", err)
		}
	}

	if _, err := os.Stat(prefix + ".ckm"); err == nil {
		mm, err = metadata.Open(prefix + ".ckm")
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("open metadata: %w", err)
		}
	}

	if _, err := os.Stat(prefix + ".cki"); err == nil {
		pi, err = postingindex.Open(prefix + ".cki")
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("open posting index: %w", err)
		}
	}

	if si == nil && pi == nil {
		closeAll()
		return nil, fmt.Errorf("%w: %s", ErrNoIndex, prefix)
	}
	if ds == nil && pi == nil {
		closeAll()
		return nil, fmt.Errorf("%w: %s (posting index required when datastore is absent)", ErrNoDataStore, prefix)
	}

	var refID uuid.UUID
	refName := ""
	check := func(name string, id uuid.UUID) error {
		if refName == "" {
			refID, refName = id, name
			return nil
		}
		if id != refID {
			return fmt.Errorf("%w: %s %x vs %s %x", ErrDatasetMismatch, refName, refID, name, id)
		}
		return nil
	}
	if ds != nil {
		if err := check("datastore", ds.Header.DatasetID); err != nil {
			closeAll()
			return nil, err
		}
	}
	if si != nil {
		if err := check("search index", si.Header.DatasetID); err != nil {
			closeAll()
			return nil, err
		}
	}
	if mm != nil {
		if err := check("metadata", mm.Header.DatasetID); err != nil {
			closeAll()
			return nil, err
		}
	}
	if pi != nil {
		if err := check("posting index", pi.Header.DatasetID); err != nil {
			closeAll()
			return nil, err
		}
	}

	return &Dataset{ds: ds, si: si, mm: mm, pi: pi, prefix: prefix}, nil
}

func (d *Dataset) Close() error {
	var firstErr error
	if d.pi != nil {
		if err := d.pi.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if d.mm != nil {
		if err := d.mm.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if d.si != nil {
		if err := d.si.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if d.ds != nil {
		if err := d.ds.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (d *Dataset) Query(fingerprint []uint32, durationMs uint32, opts *QueryOptions) ([]MatchResult, error) {
	if d.si == nil {
		return nil, ErrNoSearchIndex
	}
	if d.ds == nil {
		return nil, ErrNoDataStore
	}
	return queryDataset(d.ds, d.si, d.mm, fingerprint, durationMs, opts)
}

func (d *Dataset) QueryFull(values []uint32, opts *PostingQueryOptions) ([]PostingHit, error) {
	if d.pi == nil {
		return nil, ErrNoPostingIndex
	}
	return d.pi.QueryFull(values, opts)
}

func (d *Dataset) Lookup(id uint32) (*Fingerprint, error) {
	if d.ds == nil {
		return nil, ErrNoDataStore
	}
	rec, err := d.ds.Lookup(id)
	if err != nil {
		return nil, err
	}
	return d.ds.ReadFingerprint(rec)
}

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

func (d *Dataset) NeedsCompaction(thresholdPct float64) bool {
	if d.ds == nil {
		return false
	}
	mainCount := d.ds.RecordCount()
	if mainCount == 0 {
		return d.ds.HasOvfl
	}
	overflowCount := float64(d.ds.OverflowCount)
	return overflowCount/float64(mainCount) >= thresholdPct/100.0
}

func (d *Dataset) Stats() DatasetStats {
	var stats DatasetStats
	if d.ds != nil {
		stats.HasDataStore = true
		stats.RecordCount = d.ds.RecordCount()
		stats.HasOverflow = d.ds.HasOvfl
		stats.OverflowCount = d.ds.OverflowCount
	}
	if d.si != nil {
		stats.HasSearchIndex = true
		stats.TuningConfig = d.si.Tuning
	}
	if d.mm != nil {
		stats.HasMetadata = true
		stats.MetadataCount = d.mm.Header.RecordCount
	}
	if d.pi != nil {
		stats.HasPostingIndex = true
		stats.PostingIndexTuning = d.pi.Tuning
	}
	return stats
}

type DataStore = datastore.DataStore

type SearchIndex = searchindex.SearchIndex

type MetadataMap = metadata.MetadataMap

type PostingIndex = postingindex.PostingIndex

type DataStoreBuilder = datastore.Builder

type SearchIndexBuilder = searchindex.Builder

type MetadataMapBuilder = metadata.Builder

type PostingIndexBuilder = postingindex.Builder

func OpenDataStore(path string) (*DataStore, error) {
	return datastore.Open(path)
}

func OpenSearchIndex(path string) (*SearchIndex, error) {
	return searchindex.Open(path)
}

func OpenMetadataMap(path string) (*MetadataMap, error) {
	return metadata.Open(path)
}

func OpenPostingIndex(path string) (*PostingIndex, error) {
	return postingindex.Open(path)
}

type BuilderOptions struct {
	SpillDir string

	SpillBufferBytes int64
}

func NewDataStoreBuilder(path string, compression CompressionMethod) (*DataStoreBuilder, error) {
	return datastore.NewBuilder(path, compression)
}

func NewDataStoreBuilderWithOptions(path string, compression CompressionMethod, opts BuilderOptions) (*DataStoreBuilder, error) {
	return datastore.NewBuilderWithOptions(path, compression, datastore.BuilderOptions{
		SpillDir: opts.SpillDir,
	})
}

func NewSearchIndexBuilder(path string, compression CompressionMethod) (*SearchIndexBuilder, error) {
	return searchindex.NewBuilder(path, compression)
}

func NewMetadataMapBuilder(path string, includeText bool) (*MetadataMapBuilder, error) {
	return metadata.NewBuilder(path, includeText)
}

func NewPostingIndexBuilder(path string) (*PostingIndexBuilder, error) {
	return postingindex.NewBuilder(path)
}

func NewPostingIndexBuilderWithOptions(path string, opts BuilderOptions) (*PostingIndexBuilder, error) {
	return postingindex.NewBuilderWithOptions(path, postingindex.BuilderOptions{
		SpillDir:         opts.SpillDir,
		SpillBufferBytes: opts.SpillBufferBytes,
	})
}

func CompressFingerprint(values []uint32) []byte {
	return wire.CompressFingerprint(values)
}

func DecompressFingerprint(data []byte, rawCount int) ([]uint32, error) {
	return wire.DecompressFingerprint(data, rawCount)
}

func CompressFingerprintPFOR(values []uint32) ([]byte, error) {
	return wire.CompressFingerprintPFOR(values)
}

func DecompressFingerprintPFOR(data []byte, rawCount int) ([]uint32, error) {
	return wire.DecompressFingerprintPFOR(data, rawCount)
}

func AutoTuneParams(recordCount uint64, strategy TuningStrategy, availableRAM, storageBudget uint64) TuningConfig {
	return maintenance.AutoTuneParams(recordCount, strategy, availableRAM, storageBudget)
}

func AppendDataStoreOverflow(path string, records []OverflowRecord) error {
	return maintenance.AppendDataStoreOverflow(path, records)
}

func AppendSearchIndexOverflow(path string, ds *DataStore, newFingerprintIDs []uint32) error {
	return maintenance.AppendSearchIndexOverflow(path, ds, newFingerprintIDs)
}

func AppendMetadataOverflow(path string, records []OverflowMappingRecord) error {
	return maintenance.AppendMetadataOverflow(path, records)
}

func AppendPostingIndexOverflow(path string, ds *DataStore, newFingerprintIDs []uint32) error {
	return maintenance.AppendPostingIndexOverflow(path, ds, newFingerprintIDs)
}

func AppendPostingIndexOverflowValues(path string, records []OverflowRecord) error {
	return maintenance.AppendPostingIndexOverflowValues(path, records)
}

func TruncateOverflow(path string, baseSize int64) error {
	return maintenance.TruncateOverflow(path, baseSize)
}

func CompactDataStore(srcPath, dstPath string) error {
	return maintenance.CompactDataStore(srcPath, dstPath)
}

func CompactSearchIndex(srcPath, dstPath string, ds *DataStore) error {
	return maintenance.CompactSearchIndex(srcPath, dstPath, ds)
}

func CompactMetadata(srcPath, dstPath string) error {
	return maintenance.CompactMetadata(srcPath, dstPath)
}

func CompactPostingIndex(srcPath, dstPath string, ds *DataStore) error {
	return maintenance.CompactPostingIndex(srcPath, dstPath, ds)
}

func CompactDataset(prefix string) error {
	return maintenance.CompactDataset(prefix)
}
