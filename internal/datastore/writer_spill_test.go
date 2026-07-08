package datastore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/libchroma/internal/cktype"
)

func TestSpillSpoolRemovedOnFinishError(t *testing.T) {
	dir := t.TempDir()
	spillDir := filepath.Join(dir, "scratch")
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	b, err := NewBuilderWithOptions(filepath.Join(dir, "test.ckd"), cktype.CompressVarint, BuilderOptions{SpillDir: spillDir})
	if err != nil {
		t.Fatalf("NewBuilderWithOptions: %v", err)
	}
	if err := b.Add(1, 1000, []uint32{1, 2, 3}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	spoolPath := b.spool.Name()
	if _, err := os.Stat(spoolPath); err != nil {
		t.Fatalf("spool file missing before Finish: %v", err)
	}

	b.f.Close()
	if err := b.Finish(); err == nil {
		t.Fatalf("Finish succeeded on closed output file")
	}
	if _, err := os.Stat(spoolPath); !os.IsNotExist(err) {
		t.Errorf("spool file not removed after failed Finish: %v", err)
	}
}

func TestSpillSpoolRemovedOnSuccess(t *testing.T) {
	dir := t.TempDir()

	b, err := NewBuilderWithOptions(filepath.Join(dir, "test.ckd"), cktype.CompressVarint, BuilderOptions{SpillDir: dir})
	if err != nil {
		t.Fatalf("NewBuilderWithOptions: %v", err)
	}
	if err := b.Add(1, 1000, []uint32{1, 2, 3}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	spoolPath := b.spool.Name()
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if _, err := os.Stat(spoolPath); !os.IsNotExist(err) {
		t.Errorf("spool file not removed after Finish: %v", err)
	}
}
