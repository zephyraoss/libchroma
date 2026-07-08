package chroma

import (
	"errors"
	"path/filepath"
	"testing"
)

func buildTestPostingIndex(t *testing.T, dir string, ds *DataStore, tuning TuningConfig) string {
	t.Helper()
	path := filepath.Join(dir, "test.cki")
	pb, err := NewPostingIndexBuilder(path)
	if err != nil {
		t.Fatalf("NewPostingIndexBuilder: %v", err)
	}
	pb.SetDatasetID(ds.Header.DatasetID)
	pb.SetTuningConfig(tuning)
	if err := pb.BuildFrom(ds); err != nil {
		t.Fatalf("BuildFrom: %v", err)
	}
	if err := pb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return path
}

func TestPostingIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ds, _, allValues := buildTestDataStore(t, dir, 15, 64)
	defer ds.Close()

	path := buildTestPostingIndex(t, dir, ds, TuningConfig{
		Stride:       8,
		QBits:        2,
		SkipInterval: 64,
		Strategy:     TuneBalanced,
	})

	pi, err := OpenPostingIndex(path)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	tuning := pi.Tuning
	if tuning.Stride != 8 {
		t.Errorf("Stride: got %d, want 8", tuning.Stride)
	}
	if tuning.QBits != 2 {
		t.Errorf("QBits: got %d, want 2", tuning.QBits)
	}
	if tuning.SkipInterval != 64 {
		t.Errorf("SkipInterval: got %d, want 64", tuning.SkipInterval)
	}
	if tuning.TotalPostings != 120 {
		t.Errorf("TotalPostings: got %d, want 120", tuning.TotalPostings)
	}
	if tuning.BucketCount == 0 {
		t.Error("BucketCount: got 0, want > 0")
	}
	if pi.Header.DatasetID != ds.Header.DatasetID {
		t.Error("DatasetID mismatch between datastore and posting index")
	}

	hits, err := pi.QueryFull(allValues[0], nil)
	if err != nil {
		t.Fatalf("QueryFull: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("QueryFull returned no hits for indexed fingerprint")
	}
	if hits[0].FingerprintID != 1 {
		t.Errorf("top hit: got fp %d, want 1", hits[0].FingerprintID)
	}
	if hits[0].Hits != 8 {
		t.Errorf("top hit count: got %d, want 8", hits[0].Hits)
	}
	if hits[0].Delta != 0 {
		t.Errorf("top hit delta: got %d, want 0", hits[0].Delta)
	}

	shifted, err := pi.QueryFull(allValues[0][16:], nil)
	if err != nil {
		t.Fatalf("QueryFull shifted: %v", err)
	}
	if len(shifted) == 0 {
		t.Fatal("shifted QueryFull returned no hits")
	}
	if shifted[0].FingerprintID != 1 {
		t.Errorf("shifted top hit: got fp %d, want 1", shifted[0].FingerprintID)
	}
	if shifted[0].Delta != 16 {
		t.Errorf("shifted top hit delta: got %d, want 16", shifted[0].Delta)
	}
	if shifted[0].Hits != 6 {
		t.Errorf("shifted top hit count: got %d, want 6", shifted[0].Hits)
	}
}

func TestPostingIndexQuantization(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.cki")

	pb, err := NewPostingIndexBuilder(path)
	if err != nil {
		t.Fatalf("NewPostingIndexBuilder: %v", err)
	}
	if err := pb.Add(7, []uint32{0x1001, 0x2002, 0x3003}, []uint8{0, 1, 2}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := pb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	pi, err := OpenPostingIndex(path)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	query := make([]uint32, 24)
	for i := range query {
		query[i] = 0xDEAD0000 + uint32(i)*8
	}
	query[0] = 0x1002
	query[8] = 0x2001
	query[16] = 0x3000

	hits, err := pi.QueryFull(query, nil)
	if err != nil {
		t.Fatalf("QueryFull: %v", err)
	}
	if len(hits) != 1 || hits[0].FingerprintID != 7 || hits[0].Hits != 3 || hits[0].Delta != 0 {
		t.Fatalf("quantized query: got %+v, want fp 7 with 3 hits at delta 0", hits)
	}
}

func TestPostingIndexVotingSemantics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.cki")

	pb, err := NewPostingIndexBuilder(path)
	if err != nil {
		t.Fatalf("NewPostingIndexBuilder: %v", err)
	}

	h := []uint32{0x1100, 0x2200, 0x3300, 0x4400, 0x5500}

	if err := pb.Add(10, h, []uint8{0, 1, 2, 3, 4}); err != nil {
		t.Fatalf("Add fpA: %v", err)
	}
	if err := pb.Add(20, h[:4], []uint8{0, 1, 2, 3}); err != nil {
		t.Fatalf("Add fpB: %v", err)
	}
	if err := pb.Add(30, h[:3], []uint8{0, 3, 7}); err != nil {
		t.Fatalf("Add fpC: %v", err)
	}
	if err := pb.Add(40, h[:3], []uint8{1, 2, 3}); err != nil {
		t.Fatalf("Add fpD: %v", err)
	}
	if err := pb.Add(50, h[:3], []uint8{0, 1, 2}); err != nil {
		t.Fatalf("Add fpE: %v", err)
	}
	if err := pb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	pi, err := OpenPostingIndex(path)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	query := make([]uint32, 40)
	for i := range query {
		query[i] = 0xDEAD0000 + uint32(i)*8
	}
	for i, hv := range h {
		query[i*8] = hv | 3
	}

	hits, err := pi.QueryFull(query, nil)
	if err != nil {
		t.Fatalf("QueryFull: %v", err)
	}

	want := []PostingHit{
		{FingerprintID: 10, Hits: 5, Delta: 0},
		{FingerprintID: 20, Hits: 4, Delta: 0},
		{FingerprintID: 50, Hits: 3, Delta: 0},
		{FingerprintID: 40, Hits: 3, Delta: 8},
	}
	if len(hits) != len(want) {
		t.Fatalf("QueryFull: got %d hits %+v, want %d", len(hits), hits, len(want))
	}
	for i, w := range want {
		if hits[i] != w {
			t.Errorf("hit[%d]: got %+v, want %+v", i, hits[i], w)
		}
	}

	strict, err := pi.QueryFull(query, &PostingQueryOptions{MinHits: 4})
	if err != nil {
		t.Fatalf("QueryFull MinHits=4: %v", err)
	}
	if len(strict) != 2 || strict[0].FingerprintID != 10 || strict[1].FingerprintID != 20 {
		t.Errorf("MinHits=4: got %+v, want fps 10, 20", strict)
	}

	top1, err := pi.QueryFull(query, &PostingQueryOptions{TopK: 1})
	if err != nil {
		t.Fatalf("QueryFull TopK=1: %v", err)
	}
	if len(top1) != 1 || top1[0].FingerprintID != 10 {
		t.Errorf("TopK=1: got %+v, want fp 10 only", top1)
	}

	postings, err := pi.LookupHash(h[0])
	if err != nil {
		t.Fatalf("LookupHash: %v", err)
	}
	wantFPs := []uint32{10, 20, 30, 40, 50}
	if len(postings) != len(wantFPs) {
		t.Fatalf("LookupHash: got %d postings, want %d", len(postings), len(wantFPs))
	}
	for i, fp := range wantFPs {
		if postings[i].FingerprintID != fp {
			t.Errorf("posting[%d]: got fp %d, want %d", i, postings[i].FingerprintID, fp)
		}
	}
}

func TestPostingIndexSkipDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.cki")

	pb, err := NewPostingIndexBuilder(path)
	if err != nil {
		t.Fatalf("NewPostingIndexBuilder: %v", err)
	}
	pb.SetTuningConfig(TuningConfig{Stride: 8, QBits: 2, SkipInterval: 4})

	const numHashes = 200
	hashes := make([]uint32, numHashes)
	ords := make([]uint8, numHashes)
	for i := range hashes {
		hashes[i] = uint32(i) * 256
		ords[i] = uint8(i)
	}
	if err := pb.Add(1, hashes, ords); err != nil {
		t.Fatalf("Add fp1: %v", err)
	}
	if err := pb.Add(2, hashes, ords); err != nil {
		t.Fatalf("Add fp2: %v", err)
	}
	if err := pb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	pi, err := OpenPostingIndex(path)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	if pi.Tuning.BucketCount != numHashes {
		t.Errorf("BucketCount: got %d, want %d", pi.Tuning.BucketCount, numHashes)
	}
	if pi.Tuning.TotalPostings != 2*numHashes {
		t.Errorf("TotalPostings: got %d, want %d", pi.Tuning.TotalPostings, 2*numHashes)
	}
	wantSkip := uint32((numHashes + 3) / 4)
	if pi.Tuning.SkipEntryCount != wantSkip {
		t.Errorf("SkipEntryCount: got %d, want %d", pi.Tuning.SkipEntryCount, wantSkip)
	}

	for i := 0; i < numHashes; i++ {
		postings, err := pi.LookupHash(uint32(i) * 256)
		if err != nil {
			t.Fatalf("LookupHash(%d): %v", i, err)
		}
		if len(postings) != 2 {
			t.Fatalf("LookupHash(%d): got %d postings, want 2", i, len(postings))
		}
		if postings[0].FingerprintID != 1 || postings[1].FingerprintID != 2 {
			t.Errorf("LookupHash(%d): got fps %d, %d, want 1, 2", i, postings[0].FingerprintID, postings[1].FingerprintID)
		}
		if postings[0].Ordinal != uint8(i) {
			t.Errorf("LookupHash(%d): got ordinal %d, want %d", i, postings[0].Ordinal, uint8(i))
		}
	}

	for _, h := range []uint32{128, 5*256 + 128, numHashes * 256} {
		postings, err := pi.LookupHash(h)
		if err != nil {
			t.Fatalf("LookupHash(absent %d): %v", h, err)
		}
		if len(postings) != 0 {
			t.Errorf("LookupHash(absent %d): got %d postings, want 0", h, len(postings))
		}
	}
}

func TestPostingIndexEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.cki")

	pb, err := NewPostingIndexBuilder(path)
	if err != nil {
		t.Fatalf("NewPostingIndexBuilder: %v", err)
	}
	if err := pb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	pi, err := OpenPostingIndex(path)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	hits, err := pi.QueryFull([]uint32{1, 2, 3}, nil)
	if err != nil {
		t.Fatalf("QueryFull: %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("QueryFull on empty index: got %d hits, want 0", len(hits))
	}
}

func TestOverflowPostingIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 64

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
	ckiPath := prefix + ".cki"

	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	buildTestPostingIndexAt(t, ckiPath, ds)
	ds.Close()

	overflowRecs := make([]OverflowRecord, 3)
	for i := range overflowRecs {
		id, dur, vals := generateTestFingerprint(uint32(i+100)*10, fpCount)
		overflowRecs[i] = OverflowRecord{FingerprintID: id, DurationMs: dur, Values: vals}
	}
	if err := AppendDataStoreOverflow(dsPath, overflowRecs); err != nil {
		t.Fatalf("AppendDataStoreOverflow: %v", err)
	}

	ds, err = OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore after overflow: %v", err)
	}
	defer ds.Close()

	newIDs := make([]uint32, len(overflowRecs))
	for i, r := range overflowRecs {
		newIDs[i] = r.FingerprintID
	}
	if err := AppendPostingIndexOverflow(ckiPath, ds, newIDs); err != nil {
		t.Fatalf("AppendPostingIndexOverflow: %v", err)
	}

	pi, err := OpenPostingIndex(ckiPath)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	if !pi.HasOvfl {
		t.Fatal("expected posting index HasOverflow to be true")
	}

	hits, err := pi.QueryFull(overflowRecs[0].Values, nil)
	if err != nil {
		t.Fatalf("QueryFull overflow: %v", err)
	}
	if len(hits) == 0 || hits[0].FingerprintID != overflowRecs[0].FingerprintID {
		t.Fatalf("overflow query: got %+v, want top hit fp %d", hits, overflowRecs[0].FingerprintID)
	}
	if hits[0].Hits != 8 || hits[0].Delta != 0 {
		t.Errorf("overflow top hit: got %d hits at delta %d, want 8 at 0", hits[0].Hits, hits[0].Delta)
	}

	mainHits, err := pi.QueryFull(initialFPs[0].values, nil)
	if err != nil {
		t.Fatalf("QueryFull main: %v", err)
	}
	if len(mainHits) == 0 || mainHits[0].FingerprintID != initialFPs[0].id {
		t.Fatalf("main query after overflow: got %+v, want top hit fp %d", mainHits, initialFPs[0].id)
	}
}

func TestOverflowPostingIndexFromValues(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 64

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
	ckiPath := prefix + ".cki"

	ds, err := OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	buildTestPostingIndexAt(t, ckiPath, ds)
	ds.Close()

	overflowRecs := make([]OverflowRecord, 3)
	for i := range overflowRecs {
		id, dur, vals := generateTestFingerprint(uint32(i+100)*10, fpCount)
		overflowRecs[i] = OverflowRecord{FingerprintID: id, DurationMs: dur, Values: vals}
	}
	if err := AppendPostingIndexOverflowValues(ckiPath, overflowRecs); err != nil {
		t.Fatalf("AppendPostingIndexOverflowValues: %v", err)
	}

	pi, err := OpenPostingIndex(ckiPath)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	if !pi.HasOvfl {
		t.Fatal("expected posting index HasOverflow to be true")
	}

	hits, err := pi.QueryFull(overflowRecs[0].Values, nil)
	if err != nil {
		t.Fatalf("QueryFull overflow: %v", err)
	}
	if len(hits) == 0 || hits[0].FingerprintID != overflowRecs[0].FingerprintID {
		t.Fatalf("overflow query: got %+v, want top hit fp %d", hits, overflowRecs[0].FingerprintID)
	}

	mainHits, err := pi.QueryFull(initialFPs[0].values, nil)
	if err != nil {
		t.Fatalf("QueryFull main: %v", err)
	}
	if len(mainHits) == 0 || mainHits[0].FingerprintID != initialFPs[0].id {
		t.Fatalf("main query after overflow: got %+v, want top hit fp %d", mainHits, initialFPs[0].id)
	}
}

func buildTestPostingIndexAt(t *testing.T, path string, ds *DataStore) {
	t.Helper()
	pb, err := NewPostingIndexBuilder(path)
	if err != nil {
		t.Fatalf("NewPostingIndexBuilder: %v", err)
	}
	pb.SetDatasetID(ds.Header.DatasetID)
	pb.SetTuningConfig(TuningConfig{Stride: 8, QBits: 2, SkipInterval: 64, Strategy: TuneBalanced})
	if err := pb.BuildFrom(ds); err != nil {
		t.Fatalf("BuildFrom: %v", err)
	}
	if err := pb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

func TestPostingIndexCompaction(t *testing.T) {
	dir := t.TempDir()
	const fpCount = 64

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
	ckiPath := prefix + ".cki"

	ds, err := OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	buildTestPostingIndexAt(t, ckiPath, ds)
	ds.Close()

	overflowRecs := make([]OverflowRecord, 3)
	for i := range overflowRecs {
		id, dur, vals := generateTestFingerprint(uint32(i+100)*10, fpCount)
		overflowRecs[i] = OverflowRecord{FingerprintID: id, DurationMs: dur, Values: vals}
	}
	if err := AppendDataStoreOverflow(dsPath, overflowRecs); err != nil {
		t.Fatalf("AppendDataStoreOverflow: %v", err)
	}

	ds, err = OpenDataStore(dsPath)
	if err != nil {
		t.Fatalf("OpenDataStore after overflow: %v", err)
	}
	newIDs := make([]uint32, len(overflowRecs))
	for i, r := range overflowRecs {
		newIDs[i] = r.FingerprintID
	}
	if err := AppendPostingIndexOverflow(ckiPath, ds, newIDs); err != nil {
		ds.Close()
		t.Fatalf("AppendPostingIndexOverflow: %v", err)
	}
	ds.Close()

	if err := CompactDataset(prefix); err != nil {
		t.Fatalf("CompactDataset: %v", err)
	}

	pi, err := OpenPostingIndex(ckiPath)
	if err != nil {
		t.Fatalf("OpenPostingIndex after compact: %v", err)
	}
	defer pi.Close()

	if pi.HasOvfl {
		t.Error("expected no overflow after compaction")
	}
	if pi.Tuning.Stride != 8 || pi.Tuning.QBits != 2 {
		t.Errorf("tuning not preserved: stride %d qbits %d", pi.Tuning.Stride, pi.Tuning.QBits)
	}
	if pi.Tuning.TotalPostings != 64 {
		t.Errorf("TotalPostings after compact: got %d, want 64", pi.Tuning.TotalPostings)
	}

	for _, fp := range initialFPs {
		hits, err := pi.QueryFull(fp.values, nil)
		if err != nil {
			t.Fatalf("QueryFull main %d: %v", fp.id, err)
		}
		if len(hits) == 0 || hits[0].FingerprintID != fp.id {
			t.Errorf("main fp %d not top hit after compaction: %+v", fp.id, hits)
		}
	}
	for _, r := range overflowRecs {
		hits, err := pi.QueryFull(r.Values, nil)
		if err != nil {
			t.Fatalf("QueryFull overflow %d: %v", r.FingerprintID, err)
		}
		if len(hits) == 0 || hits[0].FingerprintID != r.FingerprintID {
			t.Errorf("overflow fp %d not top hit after compaction: %+v", r.FingerprintID, hits)
		}
	}
}

func TestDatasetQueryFull(t *testing.T) {
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

	prefix, _ := buildOverflowTestDataset(t, dir, fps)

	d, err := Open(prefix)
	if err != nil {
		t.Fatalf("Open without .cki: %v", err)
	}
	if _, err := d.QueryFull(fps[0].values, nil); !errors.Is(err, ErrNoPostingIndex) {
		d.Close()
		t.Fatalf("QueryFull without .cki: got %v, want ErrNoPostingIndex", err)
	}
	if d.Stats().HasPostingIndex {
		d.Close()
		t.Fatal("Stats().HasPostingIndex: got true without .cki")
	}
	d.Close()

	ds, err := OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	buildTestPostingIndexAt(t, prefix+".cki", ds)
	ds.Close()

	d, err = Open(prefix)
	if err != nil {
		t.Fatalf("Open with .cki: %v", err)
	}
	defer d.Close()

	stats := d.Stats()
	if !stats.HasPostingIndex {
		t.Error("Stats().HasPostingIndex: got false, want true")
	}
	if stats.PostingIndexTuning.Stride != 8 {
		t.Errorf("Stats().PostingIndexTuning.Stride: got %d, want 8", stats.PostingIndexTuning.Stride)
	}

	hits, err := d.QueryFull(fps[2].values, nil)
	if err != nil {
		t.Fatalf("QueryFull: %v", err)
	}
	if len(hits) == 0 || hits[0].FingerprintID != fps[2].id {
		t.Fatalf("QueryFull: got %+v, want top hit fp %d", hits, fps[2].id)
	}
}
