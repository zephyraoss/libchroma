package chroma

import (
	"errors"
	"os"
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
	if !stats.HasSearchIndex {
		t.Error("expected HasSearchIndex to be true")
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

func buildNoCKXDataset(t *testing.T, dir string, fps []struct {
	id     uint32
	dur    uint32
	values []uint32
	meta   *TrackMetadata
}) string {
	t.Helper()
	prefix := filepath.Join(dir, "test")
	datasetID := uuid.New()

	db, err := NewDataStoreBuilder(prefix+".ckd", CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}
	db.SetDatasetID(datasetID)
	for _, fp := range fps {
		if err := db.Add(fp.id, fp.dur, fp.values); err != nil {
			t.Fatalf("Add datastore: %v", err)
		}
	}
	if err := db.Finish(); err != nil {
		t.Fatalf("Finish datastore: %v", err)
	}

	ds, err := OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	buildTestPostingIndexAt(t, prefix+".cki", ds)
	ds.Close()

	mb, err := NewMetadataMapBuilder(prefix+".ckm", true)
	if err != nil {
		t.Fatalf("NewMetadataMapBuilder: %v", err)
	}
	mb.SetDatasetID(datasetID)
	for i, fp := range fps {
		if err := mb.Add(fp.id, uuid.New(), uint32(i+1), fp.meta); err != nil {
			t.Fatalf("Add metadata: %v", err)
		}
	}
	if err := mb.Finish(); err != nil {
		t.Fatalf("Finish metadata: %v", err)
	}

	return prefix
}

func TestDatasetOpenNoSearchIndex(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 64

	fps := make([]struct {
		id     uint32
		dur    uint32
		values []uint32
		meta   *TrackMetadata
	}, 4)
	for i := range fps {
		id, dur, vals := generateTestFingerprint(uint32(i+1)*10, fpCount)
		fps[i].id = id
		fps[i].dur = dur
		fps[i].values = vals
		fps[i].meta = &TrackMetadata{Title: "T"}
	}

	prefix := buildNoCKXDataset(t, dir, fps)

	dataset, err := Open(prefix)
	if err != nil {
		t.Fatalf("Open without .ckx: %v", err)
	}
	defer dataset.Close()

	stats := dataset.Stats()
	if stats.HasSearchIndex {
		t.Error("Stats().HasSearchIndex: got true without .ckx")
	}
	if !stats.HasPostingIndex {
		t.Error("Stats().HasPostingIndex: got false with .cki")
	}
	if !stats.HasMetadata {
		t.Error("Stats().HasMetadata: got false with .ckm")
	}
	if stats.RecordCount != uint64(len(fps)) {
		t.Errorf("RecordCount: got %d, want %d", stats.RecordCount, len(fps))
	}

	if _, err := dataset.Query(fps[0].values, fps[0].dur, nil); !errors.Is(err, ErrNoSearchIndex) {
		t.Fatalf("Query without .ckx: got %v, want ErrNoSearchIndex", err)
	}

	hits, err := dataset.QueryFull(fps[0].values, nil)
	if err != nil {
		t.Fatalf("QueryFull: %v", err)
	}
	if len(hits) == 0 || hits[0].FingerprintID != fps[0].id {
		t.Fatalf("QueryFull: got %+v, want top hit fp %d", hits, fps[0].id)
	}

	fp, err := dataset.Lookup(fps[2].id)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if fp.ID != fps[2].id {
		t.Errorf("Lookup ID: got %d, want %d", fp.ID, fps[2].id)
	}

	meta, mbid, err := dataset.LookupMetadata(fps[0].id)
	if err != nil {
		t.Fatalf("LookupMetadata: %v", err)
	}
	if mbid == nil || meta == nil {
		t.Fatal("LookupMetadata: nil meta or mbid")
	}
}

func TestDatasetOpenBareDatastoreFails(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "test")

	db, err := NewDataStoreBuilder(prefix+".ckd", CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}
	db.SetDatasetID(uuid.New())
	id, dur, vals := generateTestFingerprint(1, 64)
	if err := db.Add(id, dur, vals); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := db.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if _, err := Open(prefix); !errors.Is(err, ErrNoIndex) {
		t.Fatalf("Open with bare .ckd: got %v, want ErrNoIndex", err)
	}
}

func TestCompactDatasetNoSearchIndex(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 64

	fps := make([]struct {
		id     uint32
		dur    uint32
		values []uint32
		meta   *TrackMetadata
	}, 5)
	for i := range fps {
		id, dur, vals := generateTestFingerprint(uint32(i+1)*10, fpCount)
		fps[i].id = id
		fps[i].dur = dur
		fps[i].values = vals
		fps[i].meta = &TrackMetadata{Title: "Initial"}
	}

	prefix := buildNoCKXDataset(t, dir, fps)

	overflowRecs := make([]OverflowRecord, 3)
	for i := range overflowRecs {
		id, dur, vals := generateTestFingerprint(uint32(i+100)*10, fpCount)
		overflowRecs[i] = OverflowRecord{FingerprintID: id, DurationMs: dur, Values: vals}
	}
	if err := AppendDataStoreOverflow(prefix+".ckd", overflowRecs); err != nil {
		t.Fatalf("AppendDataStoreOverflow: %v", err)
	}

	ds, err := OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatalf("OpenDataStore after overflow: %v", err)
	}
	newIDs := make([]uint32, len(overflowRecs))
	for i, r := range overflowRecs {
		newIDs[i] = r.FingerprintID
	}
	if err := AppendPostingIndexOverflow(prefix+".cki", ds, newIDs); err != nil {
		ds.Close()
		t.Fatalf("AppendPostingIndexOverflow: %v", err)
	}
	ds.Close()

	if err := CompactDataset(prefix); err != nil {
		t.Fatalf("CompactDataset without .ckx: %v", err)
	}

	if _, err := os.Stat(prefix + ".ckx"); !os.IsNotExist(err) {
		t.Errorf("compaction should not create a .ckx file, stat: %v", err)
	}

	dataset, err := Open(prefix)
	if err != nil {
		t.Fatalf("Open after compaction: %v", err)
	}
	defer dataset.Close()

	stats := dataset.Stats()
	if stats.HasOverflow {
		t.Error("expected no overflow after compaction")
	}
	if stats.RecordCount != uint64(len(fps)+len(overflowRecs)) {
		t.Errorf("RecordCount: got %d, want %d", stats.RecordCount, len(fps)+len(overflowRecs))
	}
	if stats.HasSearchIndex {
		t.Error("Stats().HasSearchIndex: got true without .ckx")
	}

	for _, fp := range fps {
		hits, err := dataset.QueryFull(fp.values, nil)
		if err != nil {
			t.Fatalf("QueryFull main %d: %v", fp.id, err)
		}
		if len(hits) == 0 || hits[0].FingerprintID != fp.id {
			t.Errorf("main fp %d not top hit after compaction: %+v", fp.id, hits)
		}
	}
	for _, r := range overflowRecs {
		hits, err := dataset.QueryFull(r.Values, nil)
		if err != nil {
			t.Fatalf("QueryFull overflow %d: %v", r.FingerprintID, err)
		}
		if len(hits) == 0 || hits[0].FingerprintID != r.FingerprintID {
			t.Errorf("overflow fp %d not top hit after compaction: %+v", r.FingerprintID, hits)
		}
	}
}
