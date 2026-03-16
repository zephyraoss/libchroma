package searchindex

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/mmap"
	"github.com/zephyraoss/libchroma/internal/wire"
)

const (
	ckxFlagKnownMask   = 0x3
	TuningConfigSize   = 64
	BucketEntrySize    = 12
	TuningConfigOffset = wire.HeaderSize
)

var overflowMagicCKX = [8]byte{'C', 'K', 'A', 'F', '-', 'X', 'O', 0}

// SearchIndex provides read access to a .ckx file via memory-mapping.
type SearchIndex struct {
	F                    *os.File
	Mmap                 *mmap.Data
	Header               cktype.FileHeader
	Footer               cktype.Footer
	Tuning               cktype.TuningConfig
	FileSize             int64
	HasOvfl              bool
	OverflowBucketStart  int64
	OverflowPostingStart int64
}

// Open opens and validates a .ckx file for reading.
func Open(path string) (*SearchIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening search index: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat search index: %w", err)
	}
	fileSize := fi.Size()

	mm, err := mmap.MapFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap search index: %w", err)
	}

	header, err := wire.ReadHeader(mm, wire.MagicCKX, fileSize)
	if err != nil {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	if err := wire.ValidateFlags(header.Flags, ckxFlagKnownMask); err != nil {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	hasOverflow := header.Flags&2 != 0

	tuning, err := readTuningConfig(mm)
	if err != nil {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	if uint16(tuning.NumBands)*uint16(tuning.BitsPerBand) > 32 {
		mmap.Unmap(mm)
		f.Close()
		return nil, fmt.Errorf("%w: num_bands*bits_per_band exceeds 32", cktype.ErrInvalidTuning)
	}
	expected := uint32(1) << tuning.BitsPerBand
	if tuning.BucketsPerBand != expected {
		mmap.Unmap(mm)
		f.Close()
		return nil, fmt.Errorf("%w: buckets_per_band mismatch", cktype.ErrInvalidTuning)
	}
	if tuning.TotalBuckets != uint32(tuning.NumBands)*tuning.BucketsPerBand {
		mmap.Unmap(mm)
		f.Close()
		return nil, fmt.Errorf("%w: total_buckets mismatch", cktype.ErrInvalidTuning)
	}

	footer, err := wire.ReadFooter(mm, fileSize, wire.FooterMagicCKX)
	if err != nil {
		mmap.Unmap(mm)
		f.Close()
		return nil, err
	}

	idx := &SearchIndex{
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
			mmap.Unmap(mm)
			f.Close()
			return nil, err
		}
	}

	return idx, nil
}

func readTuningConfig(r *mmap.Data) (cktype.TuningConfig, error) {
	var buf [TuningConfigSize]byte
	if _, err := r.ReadAt(buf[:], TuningConfigOffset); err != nil {
		return cktype.TuningConfig{}, fmt.Errorf("reading tuning config: %w", err)
	}
	return cktype.TuningConfig{
		NumBands:             buf[0],
		BitsPerBand:          buf[1],
		BucketsPerBand:       binary.LittleEndian.Uint32(buf[0x02:]),
		TotalBuckets:         binary.LittleEndian.Uint32(buf[0x06:]),
		TotalPostings:        binary.LittleEndian.Uint64(buf[0x0A:]),
		AvgPostingsPerBucket: binary.LittleEndian.Uint32(buf[0x12:]),
		Strategy:             cktype.TuningStrategy(buf[0x16]),
	}, nil
}

func (idx *SearchIndex) readOverflowHeader() error {
	off := int64(idx.Footer.OverflowOffset)
	var buf [16]byte
	if _, err := idx.Mmap.ReadAt(buf[:], off); err != nil {
		return fmt.Errorf("%w: reading overflow header: %v", cktype.ErrCorruptOverflow, err)
	}

	var magic [8]byte
	copy(magic[:], buf[0:8])
	if magic != overflowMagicCKX {
		return fmt.Errorf("%w: bad overflow magic", cktype.ErrCorruptOverflow)
	}

	overflowBucketCount := binary.LittleEndian.Uint32(buf[12:16])
	if overflowBucketCount != idx.Tuning.TotalBuckets {
		return fmt.Errorf("%w: overflow bucket count mismatch", cktype.ErrCorruptOverflow)
	}

	idx.OverflowBucketStart = off + 16
	idx.OverflowPostingStart = idx.OverflowBucketStart + int64(overflowBucketCount)*BucketEntrySize

	return nil
}

// Close releases resources associated with the search index.
func (idx *SearchIndex) Close() error {
	if err := mmap.Unmap(idx.Mmap); err != nil {
		idx.F.Close()
		return fmt.Errorf("munmap search index: %w", err)
	}
	return idx.F.Close()
}

// ExtractBands returns num_bands band values extracted from a sub-fingerprint.
func (idx *SearchIndex) ExtractBands(subFingerprint uint32) []uint32 {
	bands := make([]uint32, idx.Tuning.NumBands)
	mask := uint32((1 << idx.Tuning.BitsPerBand) - 1)
	for k := uint8(0); k < idx.Tuning.NumBands; k++ {
		bands[k] = (subFingerprint >> (uint32(k) * uint32(idx.Tuning.BitsPerBand))) & mask
	}
	return bands
}

// Search collects all posting entries for the given fingerprint's sub-fingerprints.
func (idx *SearchIndex) Search(fingerprint []uint32) ([]cktype.PostingEntry, error) {
	var results []cktype.PostingEntry
	for _, subFP := range fingerprint {
		bands := idx.ExtractBands(subFP)
		for k := uint8(0); k < idx.Tuning.NumBands; k++ {
			bucketIdx := uint32(k)*idx.Tuning.BucketsPerBand + bands[k]

			entries, err := idx.ReadPostingList(bucketIdx)
			if err != nil {
				return nil, err
			}
			results = append(results, entries...)

			if idx.HasOvfl {
				oEntries, err := idx.ReadOverflowPostingList(bucketIdx)
				if err != nil {
					return nil, err
				}
				results = append(results, oEntries...)
			}
		}
	}
	return results, nil
}

// ReadPostingList reads the posting list for a bucket from the main index.
func (idx *SearchIndex) ReadPostingList(bucketIndex uint32) ([]cktype.PostingEntry, error) {
	return idx.readPostingListFrom(
		int64(idx.Header.Section0Offset),
		int64(idx.Header.Section1Offset),
		bucketIndex,
	)
}

// ReadOverflowPostingList reads the posting list for a bucket from the overflow index.
func (idx *SearchIndex) ReadOverflowPostingList(bucketIndex uint32) ([]cktype.PostingEntry, error) {
	return idx.readPostingListFrom(
		idx.OverflowBucketStart,
		idx.OverflowPostingStart,
		bucketIndex,
	)
}

func (idx *SearchIndex) readPostingListFrom(dirStart, postingStart int64, bucketIndex uint32) ([]cktype.PostingEntry, error) {
	entryOff := dirStart + int64(bucketIndex)*BucketEntrySize
	var buf [BucketEntrySize]byte
	if _, err := idx.Mmap.ReadAt(buf[:], entryOff); err != nil {
		return nil, fmt.Errorf("reading bucket entry %d: %w", bucketIndex, err)
	}

	postingOffset := binary.LittleEndian.Uint64(buf[0:8])
	postingCount := binary.LittleEndian.Uint32(buf[8:12])

	if postingCount == 0 {
		return nil, nil
	}

	absOffset := postingStart + int64(postingOffset)
	if int64(postingCount) > (int64(idx.Mmap.Len())-absOffset)/2 {
		return nil, fmt.Errorf("%w: posting count %d exceeds available data", cktype.ErrOffsetOutOfBounds, postingCount)
	}

	maxSize := int64(6) + int64(postingCount-1)*7
	remaining := int64(idx.Mmap.Len()) - absOffset
	if maxSize > remaining {
		maxSize = remaining
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("%w: posting list offset", cktype.ErrOffsetOutOfBounds)
	}

	data := make([]byte, maxSize)
	n, err := idx.Mmap.ReadAt(data, absOffset)
	if err != nil && n < 6 {
		return nil, fmt.Errorf("reading posting list: %w", err)
	}
	data = data[:n]

	if len(data) < 6 {
		return nil, cktype.ErrInvalidCompression
	}

	entries := make([]cktype.PostingEntry, 0, postingCount)
	firstID := binary.LittleEndian.Uint32(data[0:4])
	firstPos := binary.LittleEndian.Uint16(data[4:6])
	entries = append(entries, cktype.PostingEntry{FingerprintID: firstID, Position: firstPos})

	offset := 6
	prevID := firstID
	for i := uint32(1); i < postingCount; i++ {
		delta, consumed, err := wire.DecodeVarint(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decoding posting varint: %w", err)
		}
		offset += consumed

		if offset+2 > len(data) {
			return nil, cktype.ErrInvalidCompression
		}
		pos := binary.LittleEndian.Uint16(data[offset:])
		offset += 2

		prevID += delta
		entries = append(entries, cktype.PostingEntry{FingerprintID: prevID, Position: pos})
	}

	return entries, nil
}
