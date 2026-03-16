package chroma

import "errors"

// CompressFingerprintPFOR compresses using XOR-delta + PFOR bitpacking.
// This is an optimization for later implementation.
func CompressFingerprintPFOR(values []uint32) ([]byte, error) {
	return nil, errors.New("ckaf: PFOR compression not yet implemented")
}

// DecompressFingerprintPFOR decompresses XOR-delta + PFOR encoded data.
func DecompressFingerprintPFOR(data []byte, rawCount int) ([]uint32, error) {
	return nil, errors.New("ckaf: PFOR decompression not yet implemented")
}
