package chroma

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestDatasetOpenClose(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "test")
	dsPath := prefix + ".ckd"
	idxPath := prefix + ".ckx"
	metaPath := prefix + ".ckm"

	datasetID := uuid.New()
	const numFPs = 8
	const fpCount = 50

	db, err := NewDataStoreBuilder(dsPath, CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}
	db.SetDatasetID(datasetID)

	type fpData struct {
		id     uint32
		dur    uint32
		values []uint32
	}
	fps := make([]fpData, numFPs)
	for i := 0; i < numFPs; i++ {
		id, dur, vals := generateTestFingerprint(uint32(i+1), fpCount)
		fps[i] = fpData{id: id, dur: dur, values: vals}
		if err := db.Add(id, dur, vals); err != nil {
			t.Fatalf("Add datastore: %v", err)
		}
	}
	if err := db.Finish(); err != nil {
		t.Fatalf("Finish datastore: %v", err)
	}

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

	mb, err := NewMetadataMapBuilder(metaPath, true)
	if err != nil {
		t.Fatalf("NewMetadataMapBuilder: %v", err)
	}
	mb.SetDatasetID(datasetID)
	for i, fp := range fps {
		meta := &TrackMetadata{
			Title:   "Track " + string(rune('A'+i)),
			Artist:  "Artist " + string(rune('A'+i)),
			Release: "Album " + string(rune('A'+i)),
			Year:    "2024",
		}
		if err := mb.Add(fp.id, uuid.New(), uint32(i+1), meta); err != nil {
			t.Fatalf("Add metadata: %v", err)
		}
	}
	if err := mb.Finish(); err != nil {
		t.Fatalf("Finish metadata: %v", err)
	}

	dataset, err := Open(prefix)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dataset.Close()

	stats := dataset.Stats()
	if stats.RecordCount != numFPs {
		t.Errorf("RecordCount: got %d, want %d", stats.RecordCount, numFPs)
	}
	if !stats.HasMetadata {
		t.Error("expected HasMetadata to be true")
	}
	if stats.MetadataCount != numFPs {
		t.Errorf("MetadataCount: got %d, want %d", stats.MetadataCount, numFPs)
	}
	if stats.HasOverflow {
		t.Error("did not expect overflow")
	}
	if stats.TuningConfig.NumBands != 4 {
		t.Errorf("TuningConfig.NumBands: got %d, want 4", stats.TuningConfig.NumBands)
	}

	results, err := dataset.Query(fps[0].values, fps[0].dur, nil)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("Query returned no results")
	}
	found := false
	for _, r := range results {
		if r.Match.FingerprintID == fps[0].id {
			found = true
			if r.Match.Score != MatchStrong {
				t.Errorf("expected MatchStrong, got %d", r.Match.Score)
			}
			break
		}
	}
	if !found {
		t.Errorf("query did not find fingerprint ID %d", fps[0].id)
	}

	fp, err := dataset.Lookup(fps[2].id)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if fp.ID != fps[2].id {
		t.Errorf("Lookup ID: got %d, want %d", fp.ID, fps[2].id)
	}
	if len(fp.Values) != len(fps[2].values) {
		t.Fatalf("Lookup values length: got %d, want %d", len(fp.Values), len(fps[2].values))
	}

	meta, mbid, err := dataset.LookupMetadata(fps[0].id)
	if err != nil {
		t.Fatalf("LookupMetadata: %v", err)
	}
	if mbid == nil {
		t.Fatal("LookupMetadata: mbid is nil")
	}
	if meta == nil {
		t.Fatal("LookupMetadata: meta is nil")
	}
	if meta.Title != "Track A" {
		t.Errorf("Title: got %q, want %q", meta.Title, "Track A")
	}
	if meta.Year != "2024" {
		t.Errorf("Year: got %q, want %q", meta.Year, "2024")
	}

	if dataset.NeedsCompaction(5.0) {
		t.Error("NeedsCompaction should be false for fresh dataset")
	}
}
