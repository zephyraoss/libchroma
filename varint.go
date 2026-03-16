package chroma

// appendVarint appends the LEB128 encoding of v to buf and returns the extended buffer.
func appendVarint(buf []byte, v uint32) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v&0x7F)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

// decodeVarint reads a LEB128 varint from data at the given offset.
// Returns the decoded value and the number of bytes consumed.
// Returns ErrTruncatedVarint if data is exhausted before the varint ends.
// Cap at 5 bytes for u32 values (35 bits max).
func decodeVarint(data []byte, offset int) (uint32, int, error) {
	var result uint32
	var shift uint
	for i := 0; i < 5; i++ {
		if offset+i >= len(data) {
			return 0, 0, ErrTruncatedVarint
		}
		b := data[offset+i]
		result |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, i + 1, nil
		}
		shift += 7
	}
	return 0, 0, ErrTruncatedVarint
}

// varintLen returns the encoded length of v in bytes.
func varintLen(v uint32) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
