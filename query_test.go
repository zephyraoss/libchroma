package chroma

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func buildTestDataset(t *testing.T, dir string, numFPs, fpCount int) (dsPath, idxPath, metaPath string, allValues [][]uint32, allIDs []uint32) {
	t.Helper()

	dsPath = filepath.Join(dir, "test.ckd")
	idxPath = filepath.Join(dir, "test.ckx")
	metaPath = filepath.Join(dir, "test.ckm")
	datasetID := uuid.New()

	// Build datastore.
	db, err := NewDataStoreBuilder(dsPath, CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}
	db.SetDatasetID(datasetID)

	allValues = make([][]uint32, numFPs)
	allIDs = make([]uint32, numFPs)
	for i := 0; i < numFPs; i++ {
		id, dur, vals := generateTestFingerprint(uint32(i+1), fpCount)
		allValues[i] = vals
		allIDs[i] = id
		if err := db.Add(id, dur, vals); err != nil {
			t.Fatalf("Add datastore: %v", err)
		}
	}
	if err := db.Finish(); err != nil {
		t.Fatalf("Finish datastore: %v", err)
	}

	// Build search index.
	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}

	sb, err := NewSearchIndexBuilder(idxPath, CompressVarint)
	if err != nil {
		ds.Close()
		t.Fatalf("NewSearchIndexBuilder: %v", err)
	}
	sb.SetDatasetID(datasetID)
	sb.SetTuningConfig(TuningConfig{
		NumBands:       4,
		BitsPerBand:    8,
		BucketsPerBand: 256,
		TotalBuckets:   1024,
		Strategy:       TuneBalanced,
	})
	if err := sb.BuildFrom(ds); err != nil {
		ds.Close()
		t.Fatalf("BuildFrom: %v", err)
	}
	if err := sb.Finish(); err != nil {
		ds.Close()
		t.Fatalf("Finish search index: %v", err)
	}
	ds.Close()

	// Build metadata.
	mb, err := NewMetadataMapBuilder(metaPath, true)
	if err != nil {
		t.Fatalf("NewMetadataMapBuilder: %v", err)
	}
	mb.SetDatasetID(datasetID)
	for i := 0; i < numFPs; i++ {
		meta := &TrackMetadata{
			Title:  "Track " + string(rune('A'+i)),
			Artist: "Artist " + string(rune('A'+i)),
		}
		if err := mb.Add(allIDs[i], uuid.New(), uint32(i+1), meta); err != nil {
			t.Fatalf("Add metadata: %v", err)
		}
	}
	if err := mb.Finish(); err != nil {
		t.Fatalf("Finish metadata: %v", err)
	}

	return
}

func TestQueryExactMatch(t *testing.T) {
	dir := t.TempDir()
	dsPath, idxPath, metaPath, allValues, allIDs := buildTestDataset(t, dir, 8, 50)

	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	defer ds.Close()

	si, err := OpenSearchIndex(idxPath)
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	defer si.Close()

	mm, err := OpenMetadataMap(metaPath)
	if err != nil {
		t.Fatalf("OpenMetadataMap: %v", err)
	}
	defer mm.Close()

	// Query with exact copy of first fingerprint.
	results, err := QueryDataset(ds, si, mm, allValues[0], uint32(len(allValues[0]))*100, nil)
	if err != nil {
		t.Fatalf("QueryDataset: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("QueryDataset returned no results")
	}

	// The exact match should be the top result.
	found := false
	for _, r := range results {
		if r.Match.FingerprintID == allIDs[0] {
			found = true
			if r.Match.Score != MatchStrong {
				t.Errorf("expected MatchStrong, got %d (BER=%.4f)", r.Match.Score, r.Match.BitErrorRate)
			}
			if r.Match.BitErrorRate > 0.01 {
				t.Errorf("exact match BER too high: %.4f", r.Match.BitErrorRate)
			}
			break
		}
	}
	if !found {
		t.Errorf("exact match fingerprint ID %d not found in results", allIDs[0])
	}
}

func TestQuerySimilarMatch(t *testing.T) {
	dir := t.TempDir()
	dsPath, idxPath, _, allValues, allIDs := buildTestDataset(t, dir, 8, 50)

	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	defer ds.Close()

	si, err := OpenSearchIndex(idxPath)
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	defer si.Close()

	// Create a slightly modified fingerprint (flip some bits).
	query := make([]uint32, len(allValues[0]))
	copy(query, allValues[0])
	for i := 0; i < len(query); i += 5 {
		query[i] ^= 0x01 // flip 1 bit every 5th value
	}

	opts := &QueryOptions{
		MaxBitErrorRate: 0.35,
		IncludeMetadata: false,
	}
	results, err := QueryDataset(ds, si, nil, query, uint32(len(query))*100, opts)
	if err != nil {
		t.Fatalf("QueryDataset: %v", err)
	}

	found := false
	for _, r := range results {
		if r.Match.FingerprintID == allIDs[0] {
			found = true
			if r.Match.BitErrorRate >= 0.35 {
				t.Errorf("similar match BER too high: %.4f", r.Match.BitErrorRate)
			}
			break
		}
	}
	if !found {
		t.Errorf("similar match fingerprint ID %d not found in results", allIDs[0])
	}
}
