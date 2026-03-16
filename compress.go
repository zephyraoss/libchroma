package chroma

import "encoding/binary"

// CompressFingerprint compresses sub-fingerprint values using XOR-delta + varint encoding.
// The first value is written as a raw little-endian u32, then each subsequent
// value is XORed with the previous and the delta is varint-encoded.
func CompressFingerprint(values []uint32) []byte {
	if len(values) == 0 {
		return nil
	}
	// Estimate capacity: 4 bytes for first value + ~2 bytes avg per delta
	buf := make([]byte, 4, 4+len(values)*2)
	binary.LittleEndian.PutUint32(buf, values[0])

	prev := values[0]
	for _, v := range values[1:] {
		delta := v ^ prev
		buf = appendVarint(buf, delta)
		prev = v
	}
	return buf
}

// DecompressFingerprint decompresses XOR-delta + varint encoded fingerprint data.
// rawCount is the number of u32 values to decode.
func DecompressFingerprint(data []byte, rawCount int) ([]uint32, error) {
	if rawCount == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, ErrInvalidCompression
	}

	values := make([]uint32, 0, rawCount)
	first := binary.LittleEndian.Uint32(data[:4])
	values = append(values, first)

	prev := first
	offset := 4
	for i := 1; i < rawCount; i++ {
		delta, n, err := decodeVarint(data, offset)
		if err != nil {
			return nil, ErrInvalidCompression
		}
		offset += n
		v := prev ^ delta
		values = append(values, v)
		prev = v
	}
	return values, nil
}
