package wire

import "github.com/zephyraoss/libchroma/internal/cktype"

func AppendVarint(buf []byte, v uint32) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v&0x7F)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

func DecodeVarint(data []byte, offset int) (uint32, int, error) {
	var result uint32
	var shift uint
	for i := 0; i < 5; i++ {
		if offset+i >= len(data) {
			return 0, 0, cktype.ErrTruncatedVarint
		}
		b := data[offset+i]
		result |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, i + 1, nil
		}
		shift += 7
	}
	return 0, 0, cktype.ErrTruncatedVarint
}

func VarintLen(v uint32) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
