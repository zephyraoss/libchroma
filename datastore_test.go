package chroma

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestDataStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ckd")

	datasetID := uuid.New()
	const numRecords = 100
	const fpCount = 50

	// Build datastore.
	b, err := NewDataStoreBuilder(path, CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}
	b.SetDatasetID(datasetID)

	type testRecord struct {
		id         uint32
		durationMs uint32
		values     []uint32
	}
	records := make([]testRecord, numRecords)
	for i := range records {
		id, dur, vals := generateTestFingerprint(uint32(i+1)*10, fpCount)
		records[i] = testRecord{id: id, durationMs: dur, values: vals}
		if err := b.Add(id, dur, vals); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Read datastore.
	ds, err := OpenDataStore(path)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	defer ds.Close()

	if ds.RecordCount() != numRecords {
		t.Errorf("RecordCount: got %d, want %d", ds.RecordCount(), numRecords)
	}

	if ds.Header().DatasetID != datasetID {
		t.Errorf("DatasetID mismatch")
	}

	// Verify each record.
	for _, tr := range records {
		rec, err := ds.Lookup(tr.id)
		if err != nil {
			t.Fatalf("Lookup(%d): %v", tr.id, err)
		}
		if rec.FingerprintID != tr.id {
			t.Errorf("FingerprintID: got %d, want %d", rec.FingerprintID, tr.id)
		}
		if rec.DurationMs != tr.durationMs {
			t.Errorf("DurationMs: got %d, want %d", rec.DurationMs, tr.durationMs)
		}
		if rec.RawCount != uint16(len(tr.values)) {
			t.Errorf("RawCount: got %d, want %d", rec.RawCount, len(tr.values))
		}

		fp, err := ds.ReadFingerprint(rec)
		if err != nil {
			t.Fatalf("ReadFingerprint(%d): %v", tr.id, err)
		}
		if len(fp.Values) != len(tr.values) {
			t.Fatalf("values length: got %d, want %d", len(fp.Values), len(tr.values))
		}
		for j, v := range tr.values {
			if fp.Values[j] != v {
				t.Errorf("value[%d]: got %d, want %d", j, fp.Values[j], v)
				break
			}
		}
	}
}

func TestDataStoreLookupNotFound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ckd")

	b, err := NewDataStoreBuilder(path, CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}
	if err := b.Add(100, 5000, []uint32{1, 2, 3}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	ds, err := OpenDataStore(path)
	if err != nil {
		t.Fatalf("OpenDataStore: %v", err)
	}
	defer ds.Close()

	_, err = ds.Lookup(999)
	if !errors.Is(err, ErrRecordNotFound) {
		t.Errorf("expected ErrRecordNotFound, got %v", err)
	}
}

func TestDataStoreBinarySearch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ckd")

	b, err := NewDataStoreBuilder(path, CompressVarint)
	if err != nil {
		t.Fatalf("NewDataStoreBuilder: %v", err)
	}

	// Add non-sequential IDs (builder sorts them).
	ids := []uint32{500, 10, 999, 42, 7, 300, 1}
	for _, id := range ids {
		if err := b.Add(id, 1000, []uint32{id, id + 1, id + 2}); err != nil {
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
	defer ds.Close()

	for _, id := range ids {
		rec, err := ds.Lookup(id)
		if err != nil {
			t.Errorf("Lookup(%d): %v", id, err)
			continue
		}
		if rec.FingerprintID != id {
			t.Errorf("got ID %d, want %d", rec.FingerprintID, id)
		}
	}
}
