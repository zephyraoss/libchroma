package chroma

import (
	"path/filepath"
	"runtime"
	"testing"
)

const (
	benchFingerprints = 100_000
	benchValuesPerFP  = 128
)

func benchmarkPostingIndexSpill(b *testing.B, concurrency int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		builder, err := NewPostingIndexBuilderWithOptions(filepath.Join(dir, "bench.cki"), BuilderOptions{
			SpillDir:         dir,
			SpillBufferBytes: 4 << 20,
			Concurrency:      concurrency,
		})
		if err != nil {
			b.Fatal(err)
		}
		builder.SetTuningConfig(TuningConfig{Stride: 8, QBits: 2, SkipInterval: 64})
		for fp := 0; fp < benchFingerprints; fp++ {
			_, _, vals := generateTestFingerprint(uint32(fp+1), benchValuesPerFP/8)
			ordinals := make([]uint8, len(vals))
			for j := range vals {
				ordinals[j] = uint8(j)
			}
			if err := builder.Add(uint32(fp+1), vals, ordinals); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()

		if err := builder.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPostingIndexSpillSerial(b *testing.B) {
	benchmarkPostingIndexSpill(b, 1)
}

func BenchmarkPostingIndexSpillConcurrent(b *testing.B) {
	benchmarkPostingIndexSpill(b, runtime.GOMAXPROCS(0))
}

func benchmarkDataStoreSpill(b *testing.B, concurrency int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		builder, err := NewDataStoreBuilderWithOptions(filepath.Join(dir, "bench.ckd"), CompressPFOR, BuilderOptions{
			SpillDir:    dir,
			Concurrency: concurrency,
		})
		if err != nil {
			b.Fatal(err)
		}
		for fp := 0; fp < benchFingerprints; fp++ {
			id, dur, vals := generateTestFingerprint(uint32(benchFingerprints-fp), benchValuesPerFP)
			if err := builder.Add(id, dur, vals); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()

		if err := builder.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDataStoreSpillSerial(b *testing.B) {
	benchmarkDataStoreSpill(b, 1)
}

func BenchmarkDataStoreSpillConcurrent(b *testing.B) {
	benchmarkDataStoreSpill(b, runtime.GOMAXPROCS(0))
}
