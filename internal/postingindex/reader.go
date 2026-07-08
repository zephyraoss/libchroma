package postingindex

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/mmap"
	"github.com/zephyraoss/libchroma/internal/wire"
)

var overflowMagicCKI = [8]byte{'C', 'K', 'A', 'F', '-', 'I', 'O', 0}

type PostingIndex struct {
	F        *os.File
	Mmap     *mmap.Data
	Header   cktype.FileHeader
	Footer   cktype.Footer
	Tuning   cktype.TuningConfig
	FileSize int64
	HasOvfl  bool

	OverflowPostingCount uint32
	OverflowSkipCount    uint32
	OverflowSkipStart    int64
	OverflowPostingStart int64
	OverflowPostingEnd   int64
}

type region struct {
	skipStart    int64
	skipCount    int
	postingStart int64
	postingLen   int64
}

func Open(path string) (*PostingIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening posting index: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat posting index: %w", err)
	}
	fileSize := fi.Size()

	mm, err := mmap.MapFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap posting index: %w", err)
	}

	fail := func(err error) (*PostingIndex, error) {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	header, err := wire.ReadHeader(mm, wire.MagicCKI, fileSize)
	if err != nil {
		return fail(err)
	}

	if err := wire.ValidateFlags(header.Flags, ckiFlagKnownMask); err != nil {
		return fail(err)
	}
	if header.Flags&1 != 0 {
		return fail(fmt.Errorf("%w: unknown posting compression", cktype.ErrInvalidCompression))
	}
	hasOverflow := header.Flags&2 != 0

	tuning, err := readTuningConfig(mm)
	if err != nil {
		return fail(err)
	}

	if tuning.Stride == 0 {
		return fail(fmt.Errorf("%w: stride must be >= 1", cktype.ErrInvalidTuning))
	}
	if tuning.QBits > 24 {
		return fail(fmt.Errorf("%w: qbits %d exceeds 24", cktype.ErrInvalidTuning, tuning.QBits))
	}
	if tuning.SkipInterval == 0 {
		return fail(fmt.Errorf("%w: skip_interval must be >= 1", cktype.ErrInvalidTuning))
	}
	if uint64(tuning.SkipEntryCount)*SkipEntrySize != header.Section0Length {
		return fail(fmt.Errorf("%w: skip directory size mismatch", cktype.ErrInvalidTuning))
	}

	footer, err := wire.ReadFooter(mm, fileSize, wire.FooterMagicCKI)
	if err != nil {
		return fail(err)
	}

	idx := &PostingIndex{
		F:        f,
		Mmap:     mm,
		Header:   header,
		Footer:   footer,
		Tuning:   tuning,
		FileSize: fileSize,
		HasOvfl:  hasOverflow,
	}

	if hasOverflow && footer.OverflowOffset != 0 {
		if err := idx.readOverflowHeader(); err != nil {
			return fail(err)
		}
	}

	return idx, nil
}

func readTuningConfig(r *mmap.Data) (cktype.TuningConfig, error) {
	var buf [TuningConfigSize]byte
	if _, err := r.ReadAt(buf[:], TuningConfigOffset); err != nil {
		return cktype.TuningConfig{}, fmt.Errorf("reading tuning config: %w", err)
	}
	t := cktype.TuningConfig{
		Stride:         buf[0],
		QBits:          buf[1],
		SkipInterval:   binary.LittleEndian.Uint32(buf[0x02:]),
		BucketCount:    binary.LittleEndian.Uint32(buf[0x06:]),
		TotalPostings:  binary.LittleEndian.Uint64(buf[0x0A:]),
		SkipEntryCount: binary.LittleEndian.Uint32(buf[0x12:]),
		Strategy:       cktype.TuningStrategy(buf[0x16]),
	}
	if t.BucketCount > 0 {
		t.AvgPostingsPerBucket = uint32(t.TotalPostings / uint64(t.BucketCount))
	}
	return t, nil
}

func (idx *PostingIndex) readOverflowHeader() error {
	off := int64(idx.Footer.OverflowOffset)
	var buf [16]byte
	if _, err := idx.Mmap.ReadAt(buf[:], off); err != nil {
		return fmt.Errorf("%w: reading overflow header: %v", cktype.ErrCorruptOverflow, err)
	}

	var magic [8]byte
	copy(magic[:], buf[0:8])
	if magic != overflowMagicCKI {
		return fmt.Errorf("%w: bad overflow magic", cktype.ErrCorruptOverflow)
	}

	idx.OverflowPostingCount = binary.LittleEndian.Uint32(buf[8:12])
	idx.OverflowSkipCount = binary.LittleEndian.Uint32(buf[12:16])
	idx.OverflowSkipStart = off + 16
	idx.OverflowPostingStart = idx.OverflowSkipStart + int64(idx.OverflowSkipCount)*SkipEntrySize
	idx.OverflowPostingEnd = idx.FileSize - wire.FooterSize

	if idx.OverflowPostingStart > idx.OverflowPostingEnd {
		return fmt.Errorf("%w: overflow skip directory extends beyond file", cktype.ErrCorruptOverflow)
	}

	return nil
}

func (idx *PostingIndex) Close() error {
	if err := mmap.Unmap(idx.Mmap); err != nil {
		idx.F.Close()
		return fmt.Errorf("munmap posting index: %w", err)
	}
	return idx.F.Close()
}

func (idx *PostingIndex) mainRegion() region {
	return region{
		skipStart:    int64(idx.Header.Section0Offset),
		skipCount:    int(idx.Tuning.SkipEntryCount),
		postingStart: int64(idx.Header.Section1Offset),
		postingLen:   int64(idx.Header.Section1Length),
	}
}

func (idx *PostingIndex) overflowRegion() region {
	return region{
		skipStart:    idx.OverflowSkipStart,
		skipCount:    int(idx.OverflowSkipCount),
		postingStart: idx.OverflowPostingStart,
		postingLen:   idx.OverflowPostingEnd - idx.OverflowPostingStart,
	}
}

func (idx *PostingIndex) LookupHash(hash uint32) ([]cktype.SampledPosting, error) {
	h := hash & QuantizeMask(idx.Tuning.QBits)
	entries, err := idx.lookupRegion(idx.mainRegion(), h)
	if err != nil {
		return nil, err
	}
	if idx.HasOvfl {
		oEntries, err := idx.lookupRegion(idx.overflowRegion(), h)
		if err != nil {
			return nil, err
		}
		entries = append(entries, oEntries...)
	}
	return entries, nil
}

func (idx *PostingIndex) readSkipEntry(r region, i int) (SkipEntry, error) {
	var buf [SkipEntrySize]byte
	if _, err := idx.Mmap.ReadAt(buf[:], r.skipStart+int64(i)*SkipEntrySize); err != nil {
		return SkipEntry{}, fmt.Errorf("reading skip entry %d: %w", i, err)
	}
	return SkipEntry{
		Hash:   binary.LittleEndian.Uint32(buf[0:4]),
		Offset: binary.LittleEndian.Uint64(buf[4:12]),
	}, nil
}

func (idx *PostingIndex) lookupRegion(r region, target uint32) ([]cktype.SampledPosting, error) {
	if r.skipCount == 0 || r.postingLen <= 0 {
		return nil, nil
	}

	lo, hi := 0, r.skipCount-1
	found := -1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		e, err := idx.readSkipEntry(r, mid)
		if err != nil {
			return nil, err
		}
		if e.Hash <= target {
			found = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if found < 0 {
		return nil, nil
	}

	entry, err := idx.readSkipEntry(r, found)
	if err != nil {
		return nil, err
	}
	scanEnd := r.postingLen
	if found+1 < r.skipCount {
		next, err := idx.readSkipEntry(r, found+1)
		if err != nil {
			return nil, err
		}
		if int64(next.Offset) < scanEnd {
			scanEnd = int64(next.Offset)
		}
	}
	if int64(entry.Offset) >= r.postingLen {
		return nil, fmt.Errorf("%w: skip entry offset %d beyond posting section", cktype.ErrOffsetOutOfBounds, entry.Offset)
	}

	data := make([]byte, scanEnd-int64(entry.Offset))
	n, err := idx.Mmap.ReadAt(data, r.postingStart+int64(entry.Offset))
	if err != nil && n == 0 {
		return nil, fmt.Errorf("reading posting buckets: %w", err)
	}
	data = data[:n]

	pos := 0
	curHash := uint32(0)
	first := true
	for pos < len(data) {
		hashDelta, consumed, err := wire.DecodeVarint(data, pos)
		if err != nil {
			return nil, fmt.Errorf("decoding bucket hash delta: %w", err)
		}
		pos += consumed
		if first {
			curHash = entry.Hash
			first = false
		} else {
			curHash += hashDelta
		}

		count, consumed, err := wire.DecodeVarint(data, pos)
		if err != nil {
			return nil, fmt.Errorf("decoding bucket count: %w", err)
		}
		pos += consumed
		if count == 0 {
			return nil, fmt.Errorf("%w: empty bucket", cktype.ErrInvalidCompression)
		}

		if curHash > target {
			return nil, nil
		}
		if curHash == target {
			return decodeBucket(data, pos, count)
		}

		for i := uint32(0); i < count; i++ {
			_, consumed, err := wire.DecodeVarint(data, pos)
			if err != nil {
				return nil, fmt.Errorf("skipping bucket varint: %w", err)
			}
			pos += consumed
		}
		pos += int(count)
		if pos > len(data) {
			return nil, cktype.ErrInvalidCompression
		}
	}
	return nil, nil
}

func decodeBucket(data []byte, pos int, count uint32) ([]cktype.SampledPosting, error) {
	entries := make([]cktype.SampledPosting, 0, count)
	var fpID uint32
	for i := uint32(0); i < count; i++ {
		delta, consumed, err := wire.DecodeVarint(data, pos)
		if err != nil {
			return nil, fmt.Errorf("decoding posting fingerprint id: %w", err)
		}
		pos += consumed
		if i == 0 {
			fpID = delta
		} else {
			fpID += delta
		}
		entries = append(entries, cktype.SampledPosting{FingerprintID: fpID})
	}
	if pos+int(count) > len(data) {
		return nil, cktype.ErrInvalidCompression
	}
	for i := uint32(0); i < count; i++ {
		entries[i].Ordinal = data[pos+int(i)]
	}
	return entries, nil
}

func (idx *PostingIndex) QueryFull(values []uint32, opts *cktype.PostingQueryOptions) ([]cktype.PostingHit, error) {
	if len(values) == 0 {
		return nil, nil
	}

	minHits := DefaultMinHits
	topK := DefaultTopK
	if opts != nil {
		if opts.MinHits > 0 {
			minHits = opts.MinHits
		}
		if opts.TopK > 0 {
			topK = opts.TopK
		}
	}

	stride := int(idx.Tuning.Stride)
	votes := make(map[uint64]int, 4096)

	for p, v := range values {
		postings, err := idx.LookupHash(v)
		if err != nil {
			return nil, err
		}
		for _, pe := range postings {
			delta := int(pe.Ordinal)*stride - p
			key := uint64(pe.FingerprintID)<<32 | uint64(uint32(int32(delta)))
			votes[key]++
		}
	}

	type best struct {
		hits  int
		delta int
	}
	bestPerFP := make(map[uint32]best, 256)
	for key, hits := range votes {
		fpID := uint32(key >> 32)
		delta := int(int32(uint32(key)))
		b, ok := bestPerFP[fpID]
		if !ok || hits > b.hits || (hits == b.hits && deltaLess(delta, b.delta)) {
			bestPerFP[fpID] = best{hits: hits, delta: delta}
		}
	}

	hits := make([]cktype.PostingHit, 0, len(bestPerFP))
	for fpID, b := range bestPerFP {
		if b.hits < minHits {
			continue
		}
		hits = append(hits, cktype.PostingHit{FingerprintID: fpID, Hits: b.hits, Delta: b.delta})
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Hits != hits[j].Hits {
			return hits[i].Hits > hits[j].Hits
		}
		if hits[i].Delta != hits[j].Delta {
			return deltaLess(hits[i].Delta, hits[j].Delta)
		}
		return hits[i].FingerprintID < hits[j].FingerprintID
	})
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits, nil
}

func deltaLess(a, b int) bool {
	aa, ab := a, b
	if aa < 0 {
		aa = -aa
	}
	if ab < 0 {
		ab = -ab
	}
	if aa != ab {
		return aa < ab
	}
	return a < b
}
