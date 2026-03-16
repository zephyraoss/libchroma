package chroma

import (
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	values := []uint32{0, 1, 127, 128, 255, 256, 16383, 16384, 0x0FFFFFFF, 0xFFFFFFFF}
	for _, v := range values {
		buf := appendVarint(nil, v)
		decoded, n, err := decodeVarint(buf, 0)
		if err != nil {
			t.Fatalf("decodeVarint(%d): %v", v, err)
		}
		if n != len(buf) {
			t.Errorf("decodeVarint(%d): consumed %d bytes, encoded %d", v, n, len(buf))
		}
		if decoded != v {
			t.Errorf("decodeVarint(%d): got %d", v, decoded)
		}
	}
}

func TestVarintLen(t *testing.T) {
	values := []uint32{0, 1, 127, 128, 255, 16383, 16384, 0xFFFFFFFF}
	for _, v := range values {
		buf := appendVarint(nil, v)
		if varintLen(v) != len(buf) {
			t.Errorf("varintLen(%d) = %d, want %d", v, varintLen(v), len(buf))
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
	// All-identical values should compress well (XOR deltas are 0, varint 0 = 1 byte each).
	expectedMax := 4 + len(values) - 1 // 4 bytes for first value + 1 byte per zero delta
	if len(compressed) > expectedMax {
		t.Errorf("compressed size %d > expected max %d for identical values", len(compressed), expectedMax)
	}
}

func generateTestFingerprint(id uint32, count int) (uint32, uint32, []uint32) {
	values := make([]uint32, count)
	rng := id
	for i := range values {
		rng = rng*1103515245 + 12345
		values[i] = rng
	}
	return id, uint32(count) * 100, values
}
