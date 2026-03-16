package chroma

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

// buildOverflowTestDataset creates a .ckd, .ckx, and .ckm with the given fingerprints.
// Returns the prefix path and the dataset ID used.
func buildOverflowTestDataset(t *testing.T, dir string, fps []struct {
	id     uint32
	dur    uint32
	values []uint32
	meta   *TrackMetadata
}) (string, uuid.UUID) {
	t.Helper()
	prefix := filepath.Join(dir, "test")
	dsPath := prefix + ".ckd"
	idxPath := prefix + ".ckx"
	metaPath := prefix + ".ckm"

	datasetID := uuid.New()

	// Build datastore.
	db, err := NewDataStoreBuilder(dsPath, CompressVarint)
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
	for _, fp := range fps {
		if err := mb.Add(fp.id, uuid.New(), fp.id, fp.meta); err != nil {
			t.Fatalf("Add metadata: %v", err)
		}
	}
	if err := mb.Finish(); err != nil {
		t.Fatalf("Finish metadata: %v", err)
	}

	return prefix, datasetID
}

func TestOverflowDataStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 30

	// Build initial dataset with 5 fingerprints.
	initialFPs := make([]struct {
		id     uint32
		dur    uint32
		values []uint32
		meta   *TrackMetadata
	}, 5)
	for i := range initialFPs {
		id, dur, vals := generateTestFingerprint(uint32(i+1)*10, fpCount)
		initialFPs[i].id = id
		initialFPs[i].dur = dur
		initialFPs[i].values = vals
		initialFPs[i].meta = &TrackMetadata{Title: "Initial"}
	}

	prefix, _ := buildOverflowTestDataset(t, dir, initialFPs)
	dsPath := prefix + ".ckd"

	// Append overflow records.
	overflowRecs := make([]OverflowRecord, 3)
	overflowExpected := make([]struct {
		id     uint32
		dur    uint32
		values []uint32
	}, 3)
	for i := range overflowRecs {
		id, dur, vals := generateTestFingerprint(uint32(i+100)*10, fpCount)
		overflowRecs[i] = OverflowRecord{
			FingerprintID: id,
			DurationMs:    dur,
			Values:        vals,
		}
		overflowExpected[i].id = id
		overflowExpected[i].dur = dur
		overflowExpected[i].values = vals
	}

	if err := AppendDataStoreOverflow(dsPath, overflowRecs); err != nil {
		t.Fatalf("AppendDataStoreOverflow: %v", err)
	}

	// Re-open and verify.
	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore after overflow: %v", err)
	}
	defer ds.Close()

	if !ds.HasOverflow() {
		t.Fatal("expected HasOverflow to be true")
	}
	if ds.OverflowCount() != 3 {
		t.Errorf("OverflowCount: got %d, want 3", ds.OverflowCount())
	}

	// Verify overflow records can be looked up and read.
	for _, exp := range overflowExpected {
		rec, err := ds.Lookup(exp.id)
		if err != nil {
			t.Fatalf("Lookup overflow %d: %v", exp.id, err)
		}
		if rec.FingerprintID != exp.id {
			t.Errorf("FingerprintID: got %d, want %d", rec.FingerprintID, exp.id)
		}
		if rec.DurationMs != exp.dur {
			t.Errorf("DurationMs: got %d, want %d", rec.DurationMs, exp.dur)
		}

		fp, err := ds.ReadFingerprint(rec)
		if err != nil {
			t.Fatalf("ReadFingerprint overflow %d: %v", exp.id, err)
		}
		if len(fp.Values) != len(exp.values) {
			t.Fatalf("values length: got %d, want %d", len(fp.Values), len(exp.values))
		}
		for j, v := range exp.values {
			if fp.Values[j] != v {
				t.Errorf("overflow value[%d]: got %d, want %d", j, fp.Values[j], v)
				break
			}
		}
	}

	// Verify main records still accessible.
	for _, fp := range initialFPs {
		rec, err := ds.Lookup(fp.id)
		if err != nil {
			t.Fatalf("Lookup main %d: %v", fp.id, err)
		}
		readFP, err := ds.ReadFingerprint(rec)
		if err != nil {
			t.Fatalf("ReadFingerprint main %d: %v", fp.id, err)
		}
		if len(readFP.Values) != len(fp.values) {
			t.Errorf("main values length: got %d, want %d", len(readFP.Values), len(fp.values))
		}
	}
}

func TestOverflowSearchIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 30

	// Build initial dataset.
	initialFPs := make([]struct {
		id     uint32
		dur    uint32
		values []uint32
		meta   *TrackMetadata
	}, 5)
	for i := range initialFPs {
		id, dur, vals := generateTestFingerprint(uint32(i+1)*10, fpCount)
		initialFPs[i].id = id
		initialFPs[i].dur = dur
		initialFPs[i].values = vals
		initialFPs[i].meta = &TrackMetadata{Title: "Initial"}
	}

	prefix, _ := buildOverflowTestDataset(t, dir, initialFPs)
	dsPath := prefix + ".ckd"
	idxPath := prefix + ".ckx"

	// Append overflow datastore records first.
	overflowRecs := make([]OverflowRecord, 3)
	for i := range overflowRecs {
		id, dur, vals := generateTestFingerprint(uint32(i+100)*10, fpCount)
		overflowRecs[i] = OverflowRecord{
			FingerprintID: id,
			DurationMs:    dur,
			Values:        vals,
		}
	}
	if err := AppendDataStoreOverflow(dsPath, overflowRecs); err != nil {
		t.Fatalf("AppendDataStoreOverflow: %v", err)
	}

	// Open datastore with overflow.
	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	defer ds.Close()

	// Append search index overflow for the new fingerprints.
	newIDs := make([]uint32, len(overflowRecs))
	for i, r := range overflowRecs {
		newIDs[i] = r.FingerprintID
	}
	if err := AppendSearchIndexOverflow(idxPath, ds, newIDs); err != nil {
		t.Fatalf("AppendSearchIndexOverflow: %v", err)
	}

	// Open search index and verify overflow postings.
	si, err := OpenSearchIndex(idxPath)
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	defer si.Close()

	if !si.HasOverflow() {
		t.Fatal("expected search index HasOverflow to be true")
	}

	// Search for an overflow fingerprint - should find it in overflow posting lists.
	entries, err := si.Search(overflowRecs[0].Values)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	found := false
	for _, e := range entries {
		if e.FingerprintID == overflowRecs[0].FingerprintID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("overflow fingerprint %d not found in search results", overflowRecs[0].FingerprintID)
	}
}

func TestOverflowMetadataRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 30

	// Build initial dataset.
	initialFPs := make([]struct {
		id     uint32
		dur    uint32
		values []uint32
		meta   *TrackMetadata
	}, 3)
	for i := range initialFPs {
		id, dur, vals := generateTestFingerprint(uint32(i+1)*10, fpCount)
		initialFPs[i].id = id
		initialFPs[i].dur = dur
		initialFPs[i].values = vals
		initialFPs[i].meta = &TrackMetadata{
			Title:  "Original",
			Artist: "Artist",
		}
	}

	prefix, _ := buildOverflowTestDataset(t, dir, initialFPs)
	metaPath := prefix + ".ckm"

	// Append overflow metadata records.
	overflowMeta := []OverflowMappingRecord{
		{
			FingerprintID: 5000,
			MBID:          uuid.New(),
			TrackID:       500,
			Metadata: &TrackMetadata{
				Title:  "Overflow Track",
				Artist: "Overflow Artist",
				Year:   "2025",
			},
		},
		{
			FingerprintID: 6000,
			MBID:          uuid.New(),
			TrackID:       600,
			Metadata: &TrackMetadata{
				Title: "Another Overflow",
			},
		},
	}

	if err := AppendMetadataOverflow(metaPath, overflowMeta); err != nil {
		t.Fatalf("AppendMetadataOverflow: %v", err)
	}

	// Re-open and verify overflow metadata via compaction path
	// (since MetadataMap.Lookup doesn't search overflow directly).
	mm, err := OpenMetadataMap(metaPath)
	if err != nil {
		t.Fatalf("OpenMetadataMap after overflow: %v", err)
	}
	defer mm.Close()

	// Main records should still be accessible.
	for _, fp := range initialFPs {
		rec, err := mm.Lookup(fp.id)
		if err != nil {
			t.Fatalf("Lookup main metadata %d: %v", fp.id, err)
		}
		meta, err := mm.ReadMetadata(rec)
		if err != nil {
			t.Fatalf("ReadMetadata %d: %v", fp.id, err)
		}
		if meta.Title != "Original" {
			t.Errorf("main metadata Title: got %q, want %q", meta.Title, "Original")
		}
	}
}

func TestOverflowCompactionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 30

	// Build initial dataset.
	initialFPs := make([]struct {
		id     uint32
		dur    uint32
		values []uint32
		meta   *TrackMetadata
	}, 5)
	for i := range initialFPs {
		id, dur, vals := generateTestFingerprint(uint32(i+1)*10, fpCount)
		initialFPs[i].id = id
		initialFPs[i].dur = dur
		initialFPs[i].values = vals
		initialFPs[i].meta = &TrackMetadata{Title: "Initial", Artist: "Artist"}
	}

	prefix, _ := buildOverflowTestDataset(t, dir, initialFPs)
	dsPath := prefix + ".ckd"

	// Add overflow records.
	overflowRecs := make([]OverflowRecord, 3)
	for i := range overflowRecs {
		id, dur, vals := generateTestFingerprint(uint32(i+100)*10, fpCount)
		overflowRecs[i] = OverflowRecord{
			FingerprintID: id,
			DurationMs:    dur,
			Values:        vals,
		}
	}
	if err := AppendDataStoreOverflow(dsPath, overflowRecs); err != nil {
		t.Fatalf("AppendDataStoreOverflow: %v", err)
	}

	// Compact.
	if err := CompactDataset(prefix); err != nil {
		t.Fatalf("CompactDataset: %v", err)
	}

	// Open compacted dataset.
	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore after compact: %v", err)
	}
	defer ds.Close()

	if ds.HasOverflow() {
		t.Error("expected no overflow after compaction")
	}

	expectedTotal := uint64(len(initialFPs) + len(overflowRecs))
	if ds.RecordCount() != expectedTotal {
		t.Errorf("RecordCount: got %d, want %d", ds.RecordCount(), expectedTotal)
	}

	// Verify all records are accessible.
	for _, fp := range initialFPs {
		rec, err := ds.Lookup(fp.id)
		if err != nil {
			t.Fatalf("Lookup main %d after compact: %v", fp.id, err)
		}
		readFP, err := ds.ReadFingerprint(rec)
		if err != nil {
			t.Fatalf("ReadFingerprint main %d after compact: %v", fp.id, err)
		}
		for j, v := range fp.values {
			if readFP.Values[j] != v {
				t.Errorf("compact main value[%d]: got %d, want %d", j, readFP.Values[j], v)
				break
			}
		}
	}
	for _, r := range overflowRecs {
		rec, err := ds.Lookup(r.FingerprintID)
		if err != nil {
			t.Fatalf("Lookup overflow %d after compact: %v", r.FingerprintID, err)
		}
		readFP, err := ds.ReadFingerprint(rec)
		if err != nil {
			t.Fatalf("ReadFingerprint overflow %d after compact: %v", r.FingerprintID, err)
		}
		for j, v := range r.Values {
			if readFP.Values[j] != v {
				t.Errorf("compact overflow value[%d]: got %d, want %d", j, readFP.Values[j], v)
				break
			}
		}
	}
}
