package postingindex

import (
	"os"
	"path/filepath"
	"testing"
)

func addTestPostings(t *testing.T, b *Builder, numFingerprints int) {
	t.Helper()
	for i := 0; i < numFingerprints; i++ {
		hashes := make([]uint32, 16)
		ordinals := make([]uint8, 16)
		rng := uint32(i + 1)
		for j := range hashes {
			rng = rng*1103515245 + 12345
			hashes[j] = rng % 4096
			ordinals[j] = uint8(j)
		}
		if err := b.Add(uint32(i+1), hashes, ordinals); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
}

func TestSpillBufferStaysBounded(t *testing.T) {
	dir := t.TempDir()

	b, err := NewBuilderWithOptions(filepath.Join(dir, "test.cki"), BuilderOptions{
		SpillDir:         dir,
		SpillBufferBytes: 480,
	})
	if err != nil {
		t.Fatalf("NewBuilderWithOptions: %v", err)
	}
	const numFingerprints = 100
	for i := 0; i < numFingerprints; i++ {
		hashes := make([]uint32, 16)
		ordinals := make([]uint8, 16)
		rng := uint32(i + 1)
		for j := range hashes {
			rng = rng*1103515245 + 12345
			hashes[j] = rng % 4096
			ordinals[j] = uint8(j)
		}
		if err := b.Add(uint32(i+1), hashes, ordinals); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if len(b.postings) >= b.runLimit+16 {
			t.Fatalf("posting buffer grew to %d, limit %d", len(b.postings), b.runLimit)
		}
	}
	if b.runCount < 2 {
		t.Fatalf("expected multiple run files, got %d", b.runCount)
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if _, err := os.Stat(b.runDir); !os.IsNotExist(err) {
		t.Errorf("run directory not removed after Finish: %v", err)
	}
}

func TestSpillRunsRemovedOnFinishError(t *testing.T) {
	dir := t.TempDir()
	spillDir := filepath.Join(dir, "scratch")
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	b, err := NewBuilderWithOptions(filepath.Join(dir, "test.cki"), BuilderOptions{
		SpillDir:         spillDir,
		SpillBufferBytes: 480,
	})
	if err != nil {
		t.Fatalf("NewBuilderWithOptions: %v", err)
	}
	addTestPostings(t, b, 50)
	if b.runCount == 0 {
		t.Fatalf("expected at least one run file before Finish")
	}
	tmpPath := b.f.Name()

	b.f.Close()
	if err := b.Finish(); err == nil {
		t.Fatalf("Finish succeeded on closed output file")
	}
	if _, err := os.Stat(b.runDir); !os.IsNotExist(err) {
		t.Errorf("run directory not removed after failed Finish: %v", err)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("temp output file not removed after failed Finish: %v", err)
	}
	entries, err := os.ReadDir(spillDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("leftover spill file: %s", e.Name())
	}
}
