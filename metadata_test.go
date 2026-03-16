package chroma

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestMetadataRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ckm")

	datasetID := uuid.New()

	type testMapping struct {
		fpID    uint32
		mbid    uuid.UUID
		trackID uint32
		meta    *TrackMetadata
	}

	mappings := []testMapping{
		{10, uuid.New(), 1, &TrackMetadata{Title: "Song A", Artist: "Artist A", Release: "Album A", Year: "2020"}},
		{20, uuid.New(), 2, &TrackMetadata{Title: "Song B", Artist: "Artist B", Release: "Album B", Year: "2021"}},
		{30, uuid.New(), 3, &TrackMetadata{Title: "Song C", Artist: "Artist C", Release: "Album C", Year: "2022"}},
	}

	mb, err := NewMetadataMapBuilder(path, true)
	if err != nil {
		t.Fatalf("NewMetadataMapBuilder: %v", err)
	}
	mb.SetDatasetID(datasetID)

	for _, m := range mappings {
		if err := mb.Add(m.fpID, m.mbid, m.trackID, m.meta); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := mb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	mm, err := OpenMetadataMap(path)
	if err != nil {
		t.Fatalf("OpenMetadataMap: %v", err)
	}
	defer mm.Close()

	if mm.Header().DatasetID != datasetID {
		t.Errorf("DatasetID mismatch")
	}
	if !mm.HasTextMetadata() {
		t.Errorf("expected HasTextMetadata() to be true")
	}

	for _, m := range mappings {
		rec, err := mm.Lookup(m.fpID)
		if err != nil {
			t.Fatalf("Lookup(%d): %v", m.fpID, err)
		}
		if rec.FingerprintID != m.fpID {
			t.Errorf("FingerprintID: got %d, want %d", rec.FingerprintID, m.fpID)
		}
		if rec.MBID != m.mbid {
			t.Errorf("MBID mismatch for fpID %d", m.fpID)
		}
		if rec.TrackID != m.trackID {
			t.Errorf("TrackID: got %d, want %d", rec.TrackID, m.trackID)
		}

		meta, err := mm.ReadMetadata(rec)
		if err != nil {
			t.Fatalf("ReadMetadata(%d): %v", m.fpID, err)
		}
		if meta == nil {
			t.Fatalf("ReadMetadata(%d): got nil", m.fpID)
		}
		if meta.Title != m.meta.Title {
			t.Errorf("Title: got %q, want %q", meta.Title, m.meta.Title)
		}
		if meta.Artist != m.meta.Artist {
			t.Errorf("Artist: got %q, want %q", meta.Artist, m.meta.Artist)
		}
		if meta.Release != m.meta.Release {
			t.Errorf("Release: got %q, want %q", meta.Release, m.meta.Release)
		}
		if meta.Year != m.meta.Year {
			t.Errorf("Year: got %q, want %q", meta.Year, m.meta.Year)
		}
	}
}

func TestMetadataNoText(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ckm")

	mb, err := NewMetadataMapBuilder(path, false)
	if err != nil {
		t.Fatalf("NewMetadataMapBuilder: %v", err)
	}
	if err := mb.Add(1, uuid.New(), 1, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := mb.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	mm, err := OpenMetadataMap(path)
	if err != nil {
		t.Fatalf("OpenMetadataMap: %v", err)
	}
	defer mm.Close()

	if mm.HasTextMetadata() {
		t.Errorf("expected HasTextMetadata() to be false")
	}
}
