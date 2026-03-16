package wire

import (
	"encoding/binary"

	"github.com/zephyraoss/libchroma/internal/cktype"
)

// CompressFingerprint compresses sub-fingerprint values using XOR-delta + varint encoding.
func CompressFingerprint(values []uint32) []byte {
	if len(values) == 0 {
		return nil
	}
	buf := make([]byte, 4, 4+len(values)*2)
	binary.LittleEndian.PutUint32(buf, values[0])

	prev := values[0]
	for _, v := range values[1:] {
		delta := v ^ prev
		buf = AppendVarint(buf, delta)
		prev = v
	}
	return buf
}

// DecompressFingerprint decompresses XOR-delta + varint encoded fingerprint data.
func DecompressFingerprint(data []byte, rawCount int) ([]uint32, error) {
	if rawCount == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, cktype.ErrInvalidCompression
	}

	values := make([]uint32, 0, rawCount)
	first := binary.LittleEndian.Uint32(data[:4])
	values = append(values, first)

	prev := first
	offset := 4
	for i := 1; i < rawCount; i++ {
		delta, n, err := DecodeVarint(data, offset)
		if err != nil {
			return nil, cktype.ErrInvalidCompression
		}
		offset += n
		v := prev ^ delta
		values = append(values, v)
		prev = v
	}
	return values, nil
}
