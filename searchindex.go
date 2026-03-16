package chroma

import (
	"encoding/binary"
	"fmt"
	"os"
)

const ckxFlagKnownMask = 0x3 // bits 0-1

var overflowMagicCKX = [8]byte{'C', 'K', 'A', 'F', '-', 'X', 'O', 0}

// SearchIndex provides read access to a .ckx file via memory-mapping.
type SearchIndex struct {
	f                    *os.File
	mmap                 *mmapData
	header               FileHeader
	footer               Footer
	tuning               TuningConfig
	fileSize             int64
	hasOverflow          bool
	overflowBucketStart  int64
	overflowPostingStart int64
}

// OpenSearchIndex opens and validates a .ckx file for reading.
func OpenSearchIndex(path string) (*SearchIndex, error) {
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

	mm, err := mmapFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap search index: %w", err)
	}

	header, err := ReadHeader(mm, MagicCKX, fileSize)
	if err != nil {
		munmapData(mm)
		f.Close()
		return nil, err
	}

	if err := ValidateFlags(header.Flags, ckxFlagKnownMask); err != nil {
		munmapData(mm)
		f.Close()
		return nil, err
	}

	hasOverflow := header.Flags&2 != 0

	// Read tuning config at offset 96.
	tuning, err := readTuningConfig(mm)
	if err != nil {
		munmapData(mm)
		f.Close()
		return nil, err
	}

	// Validate tuning constraints.
	if uint16(tuning.NumBands)*uint16(tuning.BitsPerBand) > 32 {
		munmapData(mm)
		f.Close()
		return nil, fmt.Errorf("%w: num_bands*bits_per_band exceeds 32", ErrInvalidTuning)
	}
	expected := uint32(1) << tuning.BitsPerBand
	if tuning.BucketsPerBand != expected {
		munmapData(mm)
		f.Close()
		return nil, fmt.Errorf("%w: buckets_per_band mismatch", ErrInvalidTuning)
	}
	if tuning.TotalBuckets != uint32(tuning.NumBands)*tuning.BucketsPerBand {
		munmapData(mm)
		f.Close()
		return nil, fmt.Errorf("%w: total_buckets mismatch", ErrInvalidTuning)
	}

	footer, err := ReadFooter(mm, fileSize, FooterMagicCKX)
	if err != nil {
		munmapData(mm)
		f.Close()
		return nil, err
	}

	idx := &SearchIndex{
		f:           f,
		mmap:        mm,
		header:      header,
		footer:      footer,
		tuning:      tuning,
		fileSize:    fileSize,
		hasOverflow: hasOverflow,
	}

	if hasOverflow && footer.OverflowOffset != 0 {
		if err := idx.readOverflowHeader(); err != nil {
			munmapData(mm)
			f.Close()
			return nil, err
		}
	}

	return idx, nil
}

func readTuningConfig(r *mmapData) (TuningConfig, error) {
	var buf [tuningConfigSize]byte
	if _, err := r.ReadAt(buf[:], tuningConfigOffset); err != nil {
		return TuningConfig{}, fmt.Errorf("reading tuning config: %w", err)
	}
	return TuningConfig{
		NumBands:             buf[0],
		BitsPerBand:          buf[1],
		BucketsPerBand:       binary.LittleEndian.Uint32(buf[0x02:]),
		TotalBuckets:         binary.LittleEndian.Uint32(buf[0x06:]),
		TotalPostings:        binary.LittleEndian.Uint64(buf[0x0A:]),
		AvgPostingsPerBucket: binary.LittleEndian.Uint32(buf[0x12:]),
		Strategy:             TuningStrategy(buf[0x16]),
	}, nil
}

func (idx *SearchIndex) readOverflowHeader() error {
	off := int64(idx.footer.OverflowOffset)
	var buf [16]byte
	if _, err := idx.mmap.ReadAt(buf[:], off); err != nil {
		return fmt.Errorf("%w: reading overflow header: %v", ErrCorruptOverflow, err)
	}

	var magic [8]byte
	copy(magic[:], buf[0:8])
	if magic != overflowMagicCKX {
		return fmt.Errorf("%w: bad overflow magic", ErrCorruptOverflow)
	}

	overflowBucketCount := binary.LittleEndian.Uint32(buf[12:16])
	if overflowBucketCount != idx.tuning.TotalBuckets {
		return fmt.Errorf("%w: overflow bucket count mismatch", ErrCorruptOverflow)
	}

	// Overflow bucket directory starts after the 16-byte overflow header.
	idx.overflowBucketStart = off + 16
	// Overflow posting lists start after the bucket directory.
	idx.overflowPostingStart = idx.overflowBucketStart + int64(overflowBucketCount)*bucketEntrySize

	return nil
}

// Close releases resources associated with the search index.
func (idx *SearchIndex) Close() error {
	if err := munmapData(idx.mmap); err != nil {
		idx.f.Close()
		return fmt.Errorf("munmap search index: %w", err)
	}
	return idx.f.Close()
}

// Header returns the file header.
func (idx *SearchIndex) Header() FileHeader {
	return idx.header
}

// TuningConfig returns the tuning configuration.
func (idx *SearchIndex) Tuning() TuningConfig {
	return idx.tuning
}

// HasOverflow reports whether an overflow index is present.
func (idx *SearchIndex) HasOverflow() bool {
	return idx.hasOverflow
}

// ExtractBands returns num_bands band values extracted from a sub-fingerprint.
func (idx *SearchIndex) ExtractBands(subFingerprint uint32) []uint32 {
	bands := make([]uint32, idx.tuning.NumBands)
	mask := uint32((1 << idx.tuning.BitsPerBand) - 1)
	for k := uint8(0); k < idx.tuning.NumBands; k++ {
		bands[k] = (subFingerprint >> (uint32(k) * uint32(idx.tuning.BitsPerBand))) & mask
	}
	return bands
}

// Search collects all posting entries for the given fingerprint's sub-fingerprints.
func (idx *SearchIndex) Search(fingerprint []uint32) ([]PostingEntry, error) {
	var results []PostingEntry
	for _, subFP := range fingerprint {
		bands := idx.ExtractBands(subFP)
		for k := uint8(0); k < idx.tuning.NumBands; k++ {
			bucketIdx := uint32(k)*idx.tuning.BucketsPerBand + bands[k]

			// Main posting list.
			entries, err := idx.ReadPostingList(bucketIdx)
			if err != nil {
				return nil, err
			}
			results = append(results, entries...)

			// Overflow posting list.
			if idx.hasOverflow {
				oEntries, err := idx.readOverflowPostingList(bucketIdx)
				if err != nil {
					return nil, err
				}
				results = append(results, oEntries...)
			}
		}
	}
	return results, nil
}

// ReadPostingList reads and decompresses the posting list for a bucket from the main index.
func (idx *SearchIndex) ReadPostingList(bucketIndex uint32) ([]PostingEntry, error) {
	return idx.readPostingListFrom(
		int64(idx.header.Section0Offset),
		int64(idx.header.Section1Offset),
		bucketIndex,
	)
}

func (idx *SearchIndex) readOverflowPostingList(bucketIndex uint32) ([]PostingEntry, error) {
	return idx.readPostingListFrom(
		idx.overflowBucketStart,
		idx.overflowPostingStart,
		bucketIndex,
	)
}

func (idx *SearchIndex) readPostingListFrom(dirStart, postingStart int64, bucketIndex uint32) ([]PostingEntry, error) {
	// Read bucket directory entry (12 bytes).
	entryOff := dirStart + int64(bucketIndex)*bucketEntrySize
	var buf [bucketEntrySize]byte
	if _, err := idx.mmap.ReadAt(buf[:], entryOff); err != nil {
		return nil, fmt.Errorf("reading bucket entry %d: %w", bucketIndex, err)
	}

	postingOffset := binary.LittleEndian.Uint64(buf[0:8])
	postingCount := binary.LittleEndian.Uint32(buf[8:12])

	if postingCount == 0 {
		return nil, nil
	}

	// Read the compressed posting data.
	absOffset := postingStart + int64(postingOffset)
	// Sanity check postingCount against available file size.
	if int64(postingCount) > (int64(idx.mmap.Len())-absOffset)/2 {
		return nil, fmt.Errorf("%w: posting count %d exceeds available data", ErrOffsetOutOfBounds, postingCount)
	}
	// We don't know exact compressed size, so read up to a reasonable bound.
	// Max size per entry: 5 (varint) + 2 (position) = 7, plus 4+2 for first entry.
	maxSize := int64(6) + int64(postingCount-1)*7
	remaining := int64(idx.mmap.Len()) - absOffset
	if maxSize > remaining {
		maxSize = remaining
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("%w: posting list offset", ErrOffsetOutOfBounds)
	}

	data := make([]byte, maxSize)
	n, err := idx.mmap.ReadAt(data, absOffset)
	if err != nil && n < 6 {
		return nil, fmt.Errorf("reading posting list: %w", err)
	}
	data = data[:n]

	// Decompress: first entry is raw u32 + u16.
	if len(data) < 6 {
		return nil, ErrInvalidCompression
	}

	entries := make([]PostingEntry, 0, postingCount)
	firstID := binary.LittleEndian.Uint32(data[0:4])
	firstPos := binary.LittleEndian.Uint16(data[4:6])
	entries = append(entries, PostingEntry{FingerprintID: firstID, Position: firstPos})

	offset := 6
	prevID := firstID
	for i := uint32(1); i < postingCount; i++ {
		delta, consumed, err := decodeVarint(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decoding posting varint: %w", err)
		}
		offset += consumed

		if offset+2 > len(data) {
			return nil, ErrInvalidCompression
		}
		pos := binary.LittleEndian.Uint16(data[offset:])
		offset += 2

		prevID += delta
		entries = append(entries, PostingEntry{FingerprintID: prevID, Position: pos})
	}

	return entries, nil
}
