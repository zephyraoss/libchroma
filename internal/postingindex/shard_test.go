package postingindex

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/libchroma/v2/internal/cktype"
)

func maskCreatedAt(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if len(data) < 0x20 {
		t.Fatalf("file %s too small for header: %d bytes", path, len(data))
	}
	for i := 0x18; i < 0x20; i++ {
		data[i] = 0
	}
	return data
}

func buildIndex(t *testing.T, path string, opts BuilderOptions, hashFor func(fp, j int) uint32, numFingerprints, hashesPerFP int) {
	t.Helper()
	b, err := NewBuilderWithOptions(path, opts)
	if err != nil {
		t.Fatalf("NewBuilderWithOptions: %v", err)
	}
	b.SetTuningConfig(cktype.TuningConfig{Stride: 8, QBits: 0, SkipInterval: 4})
	hashes := make([]uint32, hashesPerFP)
	ordinals := make([]uint8, hashesPerFP)
	for i := 0; i < numFingerprints; i++ {
		for j := range hashes {
			hashes[j] = hashFor(i, j)
			ordinals[j] = uint8(j)
		}
		if err := b.Add(uint32(i+1), hashes, ordinals); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

func TestShardedSpillEquivalence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hashFor func(fp, j int) uint32
	}{
		{"uniform", func(fp, j int) uint32 {
			rng := uint32(fp*31 + j + 1)
			rng = rng*1103515245 + 12345
			return rng
		}},
		{"narrow-range", func(fp, j int) uint32 {
			rng := uint32(fp*31 + j + 1)
			rng = rng*1103515245 + 12345
			return rng % 97
		}},
		{"single-hash", func(fp, j int) uint32 { return 0xDEADBEEF }},
		{"skewed", func(fp, j int) uint32 {
			rng := uint32(fp*31 + j + 1)
			rng = rng*1103515245 + 12345
			if rng%10 != 0 {
				return rng % 16
			}
			return rng
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			const numFingerprints = 500
			const hashesPerFP = 24

			serialPath := filepath.Join(dir, "serial.cki")
			buildIndex(t, serialPath, BuilderOptions{SpillDir: dir, SpillBufferBytes: 1200}, tc.hashFor, numFingerprints, hashesPerFP)

			for _, concurrency := range []int{2, 8} {
				shardedPath := filepath.Join(dir, "sharded.cki")
				buildIndex(t, shardedPath, BuilderOptions{SpillDir: dir, SpillBufferBytes: 1200, Concurrency: concurrency}, tc.hashFor, numFingerprints, hashesPerFP)
				if !bytes.Equal(maskCreatedAt(t, serialPath), maskCreatedAt(t, shardedPath)) {
					t.Errorf("concurrency=%d sharded .cki differs from serial spill .cki", concurrency)
				}
			}
		})
	}
}

func TestShardedSpillEmpty(t *testing.T) {
	dir := t.TempDir()
	serialPath := filepath.Join(dir, "serial.cki")
	shardedPath := filepath.Join(dir, "sharded.cki")
	buildIndex(t, serialPath, BuilderOptions{SpillDir: dir}, nil, 0, 0)
	buildIndex(t, shardedPath, BuilderOptions{SpillDir: dir, Concurrency: 8}, nil, 0, 0)
	if !bytes.Equal(maskCreatedAt(t, serialPath), maskCreatedAt(t, shardedPath)) {
		t.Errorf("empty sharded .cki differs from empty serial spill .cki")
	}
}
