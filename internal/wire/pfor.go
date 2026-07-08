package wire

import (
	"encoding/binary"
	"math/bits"
	"sort"

	"github.com/zephyraoss/libchroma/v2/internal/cktype"
)

const PFORBlockSize = 128

func CompressFingerprintPFOR(values []uint32) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	buf := make([]byte, 4, 4+len(values)*2)
	binary.LittleEndian.PutUint32(buf, values[0])

	if len(values) == 1 {
		return buf, nil
	}

	deltas := make([]uint32, len(values)-1)
	prev := values[0]
	for i := 1; i < len(values); i++ {
		deltas[i-1] = values[i] ^ prev
		prev = values[i]
	}

	for start := 0; start < len(deltas); start += PFORBlockSize {
		end := start + PFORBlockSize
		if end > len(deltas) {
			end = len(deltas)
		}
		buf = appendPFORBlock(buf, deltas[start:end])
	}

	return buf, nil
}

func DecompressFingerprintPFOR(data []byte, rawCount int) ([]uint32, error) {
	if rawCount == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, cktype.ErrInvalidCompression
	}

	values := make([]uint32, 0, rawCount)
	first := binary.LittleEndian.Uint32(data[:4])
	values = append(values, first)

	offset := 4
	remaining := rawCount - 1
	prev := first

	for remaining > 0 {
		blockCount := PFORBlockSize
		if remaining < blockCount {
			blockCount = remaining
		}

		deltas, consumed, err := decodePFORBlock(data, offset, blockCount)
		if err != nil {
			return nil, err
		}
		offset += consumed

		for _, d := range deltas {
			v := prev ^ d
			values = append(values, v)
			prev = v
		}
		remaining -= blockCount
	}

	return values, nil
}

func CompressPostingsPFOR(entries []cktype.PostingEntry) []byte {
	if len(entries) == 0 {
		return nil
	}

	var buf []byte
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], entries[0].FingerprintID)
	buf = append(buf, tmp[:]...)

	if len(entries) > 1 {
		deltas := make([]uint32, len(entries)-1)
		prevID := entries[0].FingerprintID
		for i := 1; i < len(entries); i++ {
			deltas[i-1] = entries[i].FingerprintID - prevID
			prevID = entries[i].FingerprintID
		}

		for start := 0; start < len(deltas); start += PFORBlockSize {
			end := start + PFORBlockSize
			if end > len(deltas) {
				end = len(deltas)
			}
			buf = appendPFORBlock(buf, deltas[start:end])
		}
	}

	var ptmp [2]byte
	for _, e := range entries {
		binary.LittleEndian.PutUint16(ptmp[:], e.Position)
		buf = append(buf, ptmp[:]...)
	}

	return buf
}

func DecompressPostingsPFOR(data []byte, count int) ([]cktype.PostingEntry, error) {
	if count == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, cktype.ErrInvalidCompression
	}

	entries := make([]cktype.PostingEntry, count)

	firstID := binary.LittleEndian.Uint32(data[:4])
	entries[0].FingerprintID = firstID

	offset := 4
	prevID := firstID

	remaining := count - 1
	idx := 1
	for remaining > 0 {
		blockCount := PFORBlockSize
		if remaining < blockCount {
			blockCount = remaining
		}

		deltas, consumed, err := decodePFORBlock(data, offset, blockCount)
		if err != nil {
			return nil, err
		}
		offset += consumed

		for _, d := range deltas {
			prevID += d
			entries[idx].FingerprintID = prevID
			idx++
		}
		remaining -= blockCount
	}

	posBytes := count * 2
	if offset+posBytes > len(data) {
		return nil, cktype.ErrInvalidCompression
	}
	for i := 0; i < count; i++ {
		entries[i].Position = binary.LittleEndian.Uint16(data[offset:])
		offset += 2
	}

	return entries, nil
}

func chooseBitWidth(values []uint32) uint8 {
	if len(values) == 0 {
		return 0
	}

	widths := make([]int, len(values))
	for i, v := range values {
		if v == 0 {
			widths[i] = 0
		} else {
			widths[i] = bits.Len32(v)
		}
	}
	sort.Ints(widths)

	idx := len(values)*9/10 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return uint8(widths[idx])
}

type pforException struct {
	index uint8
	value uint32
}

func appendPFORBlock(buf []byte, values []uint32) []byte {
	b := chooseBitWidth(values)

	var exceptions []pforException
	var mask uint32
	if b >= 32 {
		mask = 0xFFFFFFFF
	} else {
		mask = (1 << b) - 1
	}

	for i, v := range values {
		if v > mask {
			exceptions = append(exceptions, pforException{index: uint8(i), value: v})
		}
	}

	buf = append(buf, b, uint8(len(exceptions)))

	if b > 0 {
		packed := packBits(values, b)
		buf = append(buf, packed...)
	}

	var etmp [4]byte
	for _, exc := range exceptions {
		buf = append(buf, exc.index)
		binary.LittleEndian.PutUint32(etmp[:], exc.value)
		buf = append(buf, etmp[:]...)
	}

	return buf
}

func decodePFORBlock(data []byte, offset int, count int) ([]uint32, int, error) {
	if offset+2 > len(data) {
		return nil, 0, cktype.ErrInvalidCompression
	}

	b := data[offset]
	numExceptions := int(data[offset+1])
	consumed := 2

	var packedSize int
	if b > 0 {
		packedSize = (count*int(b) + 7) / 8
	}

	if offset+consumed+packedSize > len(data) {
		return nil, 0, cktype.ErrInvalidCompression
	}

	var values []uint32
	if b > 0 {
		values = unpackBits(data[offset+consumed:], b, count)
	} else {
		values = make([]uint32, count)
	}
	consumed += packedSize

	excSize := numExceptions * 5
	if offset+consumed+excSize > len(data) {
		return nil, 0, cktype.ErrInvalidCompression
	}

	for i := 0; i < numExceptions; i++ {
		excOff := offset + consumed + i*5
		idx := int(data[excOff])
		if idx >= count {
			return nil, 0, cktype.ErrInvalidCompression
		}
		val := binary.LittleEndian.Uint32(data[excOff+1:])
		values[idx] = val
	}
	consumed += excSize

	return values, consumed, nil
}

func packBits(values []uint32, b uint8) []byte {
	totalBits := len(values) * int(b)
	numBytes := (totalBits + 7) / 8
	out := make([]byte, numBytes)

	bitPos := 0
	for _, v := range values {
		masked := v
		if b < 32 {
			masked = v & ((1 << b) - 1)
		}

		remaining := int(b)
		pos := bitPos
		for remaining > 0 {
			byteIdx := pos / 8
			bitIdx := pos % 8
			bitsAvail := 8 - bitIdx
			if bitsAvail > remaining {
				bitsAvail = remaining
			}

			chunk := byte(masked) & ((1 << bitsAvail) - 1)
			out[byteIdx] |= chunk << uint(bitIdx)

			masked >>= uint(bitsAvail)
			pos += bitsAvail
			remaining -= bitsAvail
		}

		bitPos += int(b)
	}

	return out
}

func unpackBits(data []byte, b uint8, count int) []uint32 {
	values := make([]uint32, count)

	bitPos := 0
	for i := 0; i < count; i++ {
		var v uint32
		remaining := int(b)
		pos := bitPos
		shift := uint(0)
		for remaining > 0 {
			byteIdx := pos / 8
			bitIdx := pos % 8
			bitsAvail := 8 - bitIdx
			if bitsAvail > remaining {
				bitsAvail = remaining
			}

			chunk := uint32(data[byteIdx]>>uint(bitIdx)) & ((1 << bitsAvail) - 1)
			v |= chunk << shift

			pos += bitsAvail
			shift += uint(bitsAvail)
			remaining -= bitsAvail
		}

		values[i] = v
		bitPos += int(b)
	}

	return values
}
