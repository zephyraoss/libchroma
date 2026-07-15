package chroma

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func readMaskedFile(t *testing.T, path string) []byte {
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

func requireEmptyDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	for _, e := range entries {
		t.Errorf("leftover spill file: %s", e.Name())
	}
}

func TestDataStoreBuilderSpillEquivalence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		compression CompressionMethod
		numRecords  int
	}{
		{"varint", CompressVarint, 500},
		{"pfor", CompressPFOR, 500},
		{"empty", CompressPFOR, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			spillDir := filepath.Join(dir, "scratch")
			if err := os.MkdirAll(spillDir, 0o755); err != nil {
				t.Fatalf("MkdirAll: %v", err)
			}

			datasetID := uuid.New()
			build := func(path string, opts BuilderOptions) {
				t.Helper()
				b, err := NewDataStoreBuilderWithOptions(path, tc.compression, opts)
				if err != nil {
					t.Fatalf("NewDataStoreBuilderWithOptions: %v", err)
				}
				b.SetDatasetID(datasetID)
				b.SetSourceDate(1234567890)
				for i := 0; i < tc.numRecords; i++ {
					id := uint32(tc.numRecords - i)
					if i%7 == 0 {
						id = 42
					}
					_, dur, vals := generateTestFingerprint(uint32(i+1), 40+i%20)
					if err := b.Add(id, dur, vals); err != nil {
						t.Fatalf("Add: %v", err)
					}
				}
				if err := b.Finish(); err != nil {
					t.Fatalf("Finish: %v", err)
				}
			}

			memPath := filepath.Join(dir, "mem.ckd")
			spillPath := filepath.Join(dir, "spill.ckd")
			concurrentPath := filepath.Join(dir, "concurrent.ckd")
			build(memPath, BuilderOptions{})
			build(spillPath, BuilderOptions{SpillDir: spillDir})
			build(concurrentPath, BuilderOptions{SpillDir: spillDir, Concurrency: 8})

			want := readMaskedFile(t, memPath)
			if !bytes.Equal(want, readMaskedFile(t, spillPath)) {
				t.Errorf("spill-built .ckd differs from in-memory-built .ckd")
			}
			if !bytes.Equal(want, readMaskedFile(t, concurrentPath)) {
				t.Errorf("concurrent spill-built .ckd differs from in-memory-built .ckd")
			}
			requireEmptyDir(t, spillDir)
		})
	}
}

func TestPostingIndexBuilderSpillEquivalence(t *testing.T) {
	for _, tc := range []struct {
		name            string
		numFingerprints int
	}{
		{"multiple-runs", 400},
		{"empty", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			spillDir := filepath.Join(dir, "scratch")
			if err := os.MkdirAll(spillDir, 0o755); err != nil {
				t.Fatalf("MkdirAll: %v", err)
			}

			datasetID := uuid.New()
			build := func(path string, opts BuilderOptions) {
				t.Helper()
				b, err := NewPostingIndexBuilderWithOptions(path, opts)
				if err != nil {
					t.Fatalf("NewPostingIndexBuilderWithOptions: %v", err)
				}
				b.SetDatasetID(datasetID)
				b.SetTuningConfig(TuningConfig{Stride: 8, QBits: 2, SkipInterval: 16})
				for i := 0; i < tc.numFingerprints; i++ {
					_, _, vals := generateTestFingerprint(uint32(i+1), 30)
					hashes := make([]uint32, len(vals))
					ordinals := make([]uint8, len(vals))
					for j, v := range vals {
						hashes[j] = v % 5000
						ordinals[j] = uint8(j)
					}
					for rep := 0; rep < 2; rep++ {
						if err := b.Add(uint32(i+1), hashes, ordinals); err != nil {
							t.Fatalf("Add: %v", err)
						}
					}
				}
				if err := b.Finish(); err != nil {
					t.Fatalf("Finish: %v", err)
				}
			}

			memPath := filepath.Join(dir, "mem.cki")
			spillPath := filepath.Join(dir, "spill.cki")
			concurrentPath := filepath.Join(dir, "concurrent.cki")
			build(memPath, BuilderOptions{})
			build(spillPath, BuilderOptions{SpillDir: spillDir, SpillBufferBytes: 1024})
			build(concurrentPath, BuilderOptions{SpillDir: spillDir, SpillBufferBytes: 1024, Concurrency: 8})

			want := readMaskedFile(t, memPath)
			if !bytes.Equal(want, readMaskedFile(t, spillPath)) {
				t.Errorf("spill-built .cki differs from in-memory-built .cki")
			}
			if !bytes.Equal(want, readMaskedFile(t, concurrentPath)) {
				t.Errorf("concurrent spill-built .cki differs from in-memory-built .cki")
			}
			requireEmptyDir(t, spillDir)
		})
	}
}

func TestDataStoreBuilderAddPrecompressedEquivalence(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts func(dir string) BuilderOptions
	}{
		{"memory", func(string) BuilderOptions { return BuilderOptions{} }},
		{"spill", func(dir string) BuilderOptions { return BuilderOptions{SpillDir: dir} }},
		{"spill-concurrent", func(dir string) BuilderOptions { return BuilderOptions{SpillDir: dir, Concurrency: 4} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			datasetID := uuid.New()
			const numRecords = 200

			build := func(path string, add func(b *DataStoreBuilder, id, dur uint32, vals []uint32) error) {
				t.Helper()
				b, err := NewDataStoreBuilderWithOptions(path, CompressPFOR, tc.opts(dir))
				if err != nil {
					t.Fatalf("NewDataStoreBuilderWithOptions: %v", err)
				}
				b.SetDatasetID(datasetID)
				for i := 0; i < numRecords; i++ {
					id, dur, vals := generateTestFingerprint(uint32(i+1), 20+i%30)
					if err := add(b, id, dur, vals); err != nil {
						t.Fatalf("add: %v", err)
					}
				}
				if err := b.Finish(); err != nil {
					t.Fatalf("Finish: %v", err)
				}
			}

			plainPath := filepath.Join(dir, "plain.ckd")
			prePath := filepath.Join(dir, "pre.ckd")
			build(plainPath, func(b *DataStoreBuilder, id, dur uint32, vals []uint32) error {
				return b.Add(id, dur, vals)
			})
			build(prePath, func(b *DataStoreBuilder, id, dur uint32, vals []uint32) error {
				compressed, err := CompressFingerprintPFOR(vals)
				if err != nil {
					return err
				}
				return b.AddPrecompressed(id, dur, compressed, uint16(len(vals)))
			})

			if !bytes.Equal(readMaskedFile(t, plainPath), readMaskedFile(t, prePath)) {
				t.Errorf("AddPrecompressed-built .ckd differs from Add-built .ckd")
			}
		})
	}
}

func TestPostingIndexSpillQueryable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.cki")

	b, err := NewPostingIndexBuilderWithOptions(path, BuilderOptions{SpillDir: dir, SpillBufferBytes: 512})
	if err != nil {
		t.Fatalf("NewPostingIndexBuilderWithOptions: %v", err)
	}
	b.SetTuningConfig(TuningConfig{Stride: 8, QBits: 2, SkipInterval: 4})
	const numFingerprints = 100
	for i := 0; i < numFingerprints; i++ {
		_, _, vals := generateTestFingerprint(uint32(i+1), 24)
		ordinals := make([]uint8, len(vals))
		for j := range vals {
			ordinals[j] = uint8(j)
		}
		if err := b.Add(uint32(i+1), vals, ordinals); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	pi, err := OpenPostingIndex(path)
	if err != nil {
		t.Fatalf("OpenPostingIndex: %v", err)
	}
	defer pi.Close()

	if pi.Tuning.TotalPostings == 0 {
		t.Fatalf("TotalPostings is 0")
	}

	_, _, vals := generateTestFingerprint(7, 24)
	found := false
	postings, err := pi.LookupHash(vals[0])
	if err != nil {
		t.Fatalf("LookupHash: %v", err)
	}
	for _, p := range postings {
		if p.FingerprintID == 7 && p.Ordinal == 0 {
			found = true
		}
	}
	if !found {
		t.Errorf("posting for fingerprint 7 ordinal 0 not found")
	}
}
