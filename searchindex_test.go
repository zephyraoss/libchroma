package chroma

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func buildTestDataStore(t *testing.T, dir string, numFPs, fpCount int) (*DataStore, string, [][]uint32) {
	t.Helper()
	path := filepath.Join(dir, "test.ckd")
	b, err := NewDataStoreBuilder(path, CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}
	b.SetDatasetID(uuid.New())

	allValues := make([][]uint32, numFPs)
	for i := 0; i < numFPs; i++ {
		id, dur, vals := generateTestFingerprint(uint32(i+1), fpCount)
		allValues[i] = vals
		if err := b.Add(id, dur, vals); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	ds, err := OpenDataStore(path)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	return ds, path, allValues
}

func TestSearchIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ds, _, allValues := buildTestDataStore(t, dir, 15, 30)
	defer ds.Close()

	idxPath := filepath.Join(dir, "test.ckx")
	sb, err := NewSearchIndexBuilder(idxPath, CompressVarint)
	if err != nil {
		t.Fatalf("NewSearchIndexBuilder: %v", err)
	}
	sb.SetDatasetID(ds.Header.DatasetID)
	sb.SetTuningConfig(TuningConfig{
		NumBands:       4,
		BitsPerBand:    8,
		BucketsPerBand: 256,
		TotalBuckets:   1024,
		Strategy:       TuneBalanced,
	})

	if err := sb.BuildFrom(ds); err != nil {
		t.Fatalf("BuildFrom: %v", err)
	}
	if err := sb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	si, err := OpenSearchIndex(idxPath)
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	defer si.Close()

	tuning := si.Tuning
	if tuning.NumBands != 4 {
		t.Errorf("NumBands: got %d, want 4", tuning.NumBands)
	}
	if tuning.BitsPerBand != 8 {
		t.Errorf("BitsPerBand: got %d, want 8", tuning.BitsPerBand)
	}
	if tuning.BucketsPerBand != 256 {
		t.Errorf("BucketsPerBand: got %d, want 256", tuning.BucketsPerBand)
	}
	if tuning.TotalBuckets != 1024 {
		t.Errorf("TotalBuckets: got %d, want 1024", tuning.TotalBuckets)
	}

	results, err := si.Search(allValues[0])
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	fpID := uint32(1)
	found := false
	for _, entry := range results {
		if entry.FingerprintID == fpID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Search did not find fingerprint ID %d in %d results", fpID, len(results))
	}
}

func TestExtractBands(t *testing.T) {
	dir := t.TempDir()
	ds, _, _ := buildTestDataStore(t, dir, 5, 10)
	defer ds.Close()

	idxPath := filepath.Join(dir, "test.ckx")
	sb, err := NewSearchIndexBuilder(idxPath, CompressVarint)
	if err != nil {
		t.Fatalf("NewSearchIndexBuilder: %v", err)
	}
	sb.SetTuningConfig(TuningConfig{
		NumBands:       4,
		BitsPerBand:    8,
		BucketsPerBand: 256,
		TotalBuckets:   1024,
	})
	if err := sb.BuildFrom(ds); err != nil {
		t.Fatalf("BuildFrom: %v", err)
	}
	if err := sb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	si, err := OpenSearchIndex(idxPath)
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	defer si.Close()

	bands := si.ExtractBands(0xAABBCCDD)
	expected := []uint32{0xDD, 0xCC, 0xBB, 0xAA}
	if len(bands) != len(expected) {
		t.Fatalf("ExtractBands: got %d bands, want %d", len(bands), len(expected))
	}
	for i, v := range expected {
		if bands[i] != v {
			t.Errorf("band[%d]: got 0x%02X, want 0x%02X", i, bands[i], v)
		}
	}
}
