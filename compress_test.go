package chroma

import (
	"testing"

	"github.com/zephyraoss/libchroma/internal/wire"
)

func TestVarintRoundTrip(t *testing.T) {
	values := []uint32{0, 1, 127, 128, 255, 256, 16383, 16384, 0x0FFFFFFF, 0xFFFFFFFF}
	for _, v := range values {
		buf := wire.AppendVarint(nil, v)
		decoded, n, err := wire.DecodeVarint(buf, 0)
		if err != nil {
			t.Fatalf("DecodeVarint(%d): %v", v, err)
		}
		if n != len(buf) {
			t.Errorf("DecodeVarint(%d): consumed %d bytes, encoded %d", v, n, len(buf))
		}
		if decoded != v {
			t.Errorf("DecodeVarint(%d): got %d", v, decoded)
		}
	}
}

func TestVarintLen(t *testing.T) {
	values := []uint32{0, 1, 127, 128, 255, 16383, 16384, 0xFFFFFFFF}
	for _, v := range values {
		buf := wire.AppendVarint(nil, v)
		if wire.VarintLen(v) != len(buf) {
			t.Errorf("VarintLen(%d) = %d, want %d", v, wire.VarintLen(v), len(buf))
		}
	}
}

func TestCompressFingerprintRoundTrip(t *testing.T) {
	for _, count := range []int{10, 100, 500} {
		_, _, values := generateTestFingerprint(42, count)
		compressed := CompressFingerprint(values)
		decompressed, err := DecompressFingerprint(compressed, len(values))
		if err != nil {
			t.Fatalf("DecompressFingerprint (count=%d): %v", count, err)
		}
		if len(decompressed) != len(values) {
			t.Fatalf("length mismatch: got %d, want %d", len(decompressed), len(values))
		}
		for i, v := range values {
			if decompressed[i] != v {
				t.Errorf("mismatch at index %d: got %d, want %d", i, decompressed[i], v)
				break
			}
		}
	}
}

func TestCompressFingerprintEmpty(t *testing.T) {
	compressed := CompressFingerprint(nil)
	if compressed != nil {
		t.Errorf("expected nil for empty input, got %v", compressed)
	}
	decompressed, err := DecompressFingerprint(nil, 0)
	if err != nil {
		t.Fatalf("DecompressFingerprint(nil, 0): %v", err)
	}
	if decompressed != nil {
		t.Errorf("expected nil, got %v", decompressed)
	}
}

func TestCompressFingerprintSingle(t *testing.T) {
	values := []uint32{0xDEADBEEF}
	compressed := CompressFingerprint(values)
	decompressed, err := DecompressFingerprint(compressed, 1)
	if err != nil {
		t.Fatalf("DecompressFingerprint: %v", err)
	}
	if len(decompressed) != 1 || decompressed[0] != values[0] {
		t.Errorf("got %v, want %v", decompressed, values)
	}
}

func TestCompressFingerprintIdentical(t *testing.T) {
	values := make([]uint32, 50)
	for i := range values {
		values[i] = 0x12345678
	}
	compressed := CompressFingerprint(values)
	decompressed, err := DecompressFingerprint(compressed, len(values))
	if err != nil {
		t.Fatalf("DecompressFingerprint: %v", err)
	}
	for i, v := range values {
		if decompressed[i] != v {
			t.Errorf("mismatch at %d: got %d, want %d", i, decompressed[i], v)
			break
		}
	}
	expectedMax := 4 + len(values) - 1
	if len(compressed) > expectedMax {
		t.Errorf("compressed size %d > expected max %d for identical values", len(compressed), expectedMax)
	}
}

func TestPFORFingerprintRoundTrip(t *testing.T) {
	for _, count := range []int{1, 10, 50, 127, 128, 129, 256, 500} {
		_, _, values := generateTestFingerprint(42, count)
		compressed, err := CompressFingerprintPFOR(values)
		if err != nil {
			t.Fatalf("CompressFingerprintPFOR (count=%d): %v", count, err)
		}
		decompressed, err := DecompressFingerprintPFOR(compressed, len(values))
		if err != nil {
			t.Fatalf("DecompressFingerprintPFOR (count=%d): %v", count, err)
		}
		if len(decompressed) != len(values) {
			t.Fatalf("length mismatch (count=%d): got %d, want %d", count, len(decompressed), len(values))
		}
		for i, v := range values {
			if decompressed[i] != v {
				t.Errorf("mismatch at index %d (count=%d): got %08x, want %08x", i, count, decompressed[i], v)
				break
			}
		}
	}
}

func TestPFORFingerprintEmpty(t *testing.T) {
	compressed, err := CompressFingerprintPFOR(nil)
	if err != nil {
		t.Fatalf("CompressFingerprintPFOR(nil): %v", err)
	}
	if compressed != nil {
		t.Errorf("expected nil for empty input, got %v", compressed)
	}
	decompressed, err := DecompressFingerprintPFOR(nil, 0)
	if err != nil {
		t.Fatalf("DecompressFingerprintPFOR(nil, 0): %v", err)
	}
	if decompressed != nil {
		t.Errorf("expected nil, got %v", decompressed)
	}
}

func TestPFORFingerprintSingle(t *testing.T) {
	values := []uint32{0xDEADBEEF}
	compressed, err := CompressFingerprintPFOR(values)
	if err != nil {
		t.Fatalf("CompressFingerprintPFOR: %v", err)
	}
	decompressed, err := DecompressFingerprintPFOR(compressed, 1)
	if err != nil {
		t.Fatalf("DecompressFingerprintPFOR: %v", err)
	}
	if len(decompressed) != 1 || decompressed[0] != values[0] {
		t.Errorf("got %v, want %v", decompressed, values)
	}
}

func TestPFORFingerprintIdentical(t *testing.T) {
	values := make([]uint32, 200)
	for i := range values {
		values[i] = 0x12345678
	}
	compressed, err := CompressFingerprintPFOR(values)
	if err != nil {
		t.Fatalf("CompressFingerprintPFOR: %v", err)
	}
	decompressed, err := DecompressFingerprintPFOR(compressed, len(values))
	if err != nil {
		t.Fatalf("DecompressFingerprintPFOR: %v", err)
	}
	for i, v := range values {
		if decompressed[i] != v {
			t.Errorf("mismatch at %d: got %d, want %d", i, decompressed[i], v)
			break
		}
	}
}

func TestPFORPostingsRoundTrip(t *testing.T) {
	entries := []PostingEntry{
		{FingerprintID: 10, Position: 0},
		{FingerprintID: 10, Position: 5},
		{FingerprintID: 20, Position: 3},
		{FingerprintID: 50, Position: 1},
		{FingerprintID: 100, Position: 7},
	}
	encoded := wire.CompressPostingsPFOR(entries)
	decoded, err := wire.DecompressPostingsPFOR(encoded, len(entries))
	if err != nil {
		t.Fatalf("DecompressPostingsPFOR: %v", err)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(entries))
	}
	for i, e := range entries {
		if decoded[i].FingerprintID != e.FingerprintID || decoded[i].Position != e.Position {
			t.Errorf("entry %d: got {%d, %d}, want {%d, %d}",
				i, decoded[i].FingerprintID, decoded[i].Position, e.FingerprintID, e.Position)
		}
	}
}

func TestPFORPostingsLargeRoundTrip(t *testing.T) {
	// Create 300 entries to exercise multiple PFOR blocks.
	entries := make([]PostingEntry, 300)
	id := uint32(1)
	for i := range entries {
		entries[i] = PostingEntry{FingerprintID: id, Position: uint16(i % 100)}
		id += uint32(i%5 + 1)
	}
	encoded := wire.CompressPostingsPFOR(entries)
	decoded, err := wire.DecompressPostingsPFOR(encoded, len(entries))
	if err != nil {
		t.Fatalf("DecompressPostingsPFOR: %v", err)
	}
	for i, e := range entries {
		if decoded[i].FingerprintID != e.FingerprintID || decoded[i].Position != e.Position {
			t.Errorf("entry %d: got {%d, %d}, want {%d, %d}",
				i, decoded[i].FingerprintID, decoded[i].Position, e.FingerprintID, e.Position)
			break
		}
	}
}
