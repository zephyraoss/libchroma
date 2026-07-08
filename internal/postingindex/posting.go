package postingindex

import (
	"bytes"
	"io"
	"sort"

	"github.com/zephyraoss/libchroma/v2/internal/wire"
)

const (
	TuningConfigSize   = 64
	TuningConfigOffset = wire.HeaderSize
	SkipEntrySize      = 12

	DefaultStride       = 8
	DefaultQBits        = 2
	DefaultSkipInterval = 64
	DefaultMinHits      = 3
	DefaultTopK         = 100

	MaxOrdinal = 0xFF

	ckiFlagKnownMask = 0x3
)

type Posting struct {
	Hash          uint32
	FingerprintID uint32
	Ordinal       uint8
}

type SkipEntry struct {
	Hash   uint32
	Offset uint64
}

func QuantizeMask(qbits uint8) uint32 {
	return ^uint32(0) << qbits
}

func AppendSampled(dst []Posting, fpID uint32, values []uint32, stride, qbits uint8) []Posting {
	if stride == 0 {
		stride = DefaultStride
	}
	qmask := QuantizeMask(qbits)
	for i := 0; i < len(values); i += int(stride) {
		ord := i / int(stride)
		if ord > MaxOrdinal {
			break
		}
		dst = append(dst, Posting{
			Hash:          values[i] & qmask,
			FingerprintID: fpID,
			Ordinal:       uint8(ord),
		})
	}
	return dst
}

func postingLess(a, b Posting) bool {
	if a.Hash != b.Hash {
		return a.Hash < b.Hash
	}
	if a.FingerprintID != b.FingerprintID {
		return a.FingerprintID < b.FingerprintID
	}
	return a.Ordinal < b.Ordinal
}

func Prepare(postings []Posting) []Posting {
	sort.Slice(postings, func(i, j int) bool {
		return postingLess(postings[i], postings[j])
	})
	out := postings[:0]
	for i, p := range postings {
		if i == 0 || p != postings[i-1] {
			out = append(out, p)
		}
	}
	return out
}

func EncodePostings(sorted []Posting, skipInterval uint32) ([]byte, []SkipEntry, uint32) {
	var buf bytes.Buffer
	enc := newBucketEncoder(&buf, skipInterval)
	i := 0
	for i < len(sorted) {
		j := i
		for j < len(sorted) && sorted[j].Hash == sorted[i].Hash {
			j++
		}
		_ = enc.encodeBucket(sorted[i:j])
		i = j
	}
	return buf.Bytes(), enc.skip, enc.bucketCount
}

type bucketEncoder struct {
	w            io.Writer
	skipInterval uint32
	scratch      []byte
	prevHash     uint32
	blobLen      uint64
	bucketCount  uint32
	total        uint64
	skip         []SkipEntry
}

func newBucketEncoder(w io.Writer, skipInterval uint32) *bucketEncoder {
	if skipInterval == 0 {
		skipInterval = DefaultSkipInterval
	}
	return &bucketEncoder{w: w, skipInterval: skipInterval}
}

func (e *bucketEncoder) encodeBucket(postings []Posting) error {
	if len(postings) == 0 {
		return nil
	}
	h := postings[0].Hash
	if e.bucketCount%e.skipInterval == 0 {
		e.skip = append(e.skip, SkipEntry{Hash: h, Offset: e.blobLen})
	}
	buf := e.scratch[:0]
	buf = wire.AppendVarint(buf, h-e.prevHash)
	buf = wire.AppendVarint(buf, uint32(len(postings)))
	buf = wire.AppendVarint(buf, postings[0].FingerprintID)
	for k := 1; k < len(postings); k++ {
		buf = wire.AppendVarint(buf, postings[k].FingerprintID-postings[k-1].FingerprintID)
	}
	for _, p := range postings {
		buf = append(buf, p.Ordinal)
	}
	e.scratch = buf
	if _, err := e.w.Write(buf); err != nil {
		return err
	}
	e.blobLen += uint64(len(buf))
	e.total += uint64(len(postings))
	e.prevHash = h
	e.bucketCount++
	return nil
}
