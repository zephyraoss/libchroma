package searchindex

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/datastore"
	"github.com/zephyraoss/libchroma/internal/wire"
)

// Builder constructs a .ckx file.
type Builder struct {
	f           *os.File
	path        string
	compression cktype.CompressionMethod
	datasetID   uuid.UUID
	tuning      cktype.TuningConfig
}

// NewBuilder creates a new builder that writes a .ckx file at path.
func NewBuilder(path string, compression cktype.CompressionMethod) (*Builder, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "ckx-build-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	return &Builder{
		f:           f,
		path:        path,
		compression: compression,
	}, nil
}

// SetTuningConfig sets the tuning configuration for the index.
func (b *Builder) SetTuningConfig(config cktype.TuningConfig) {
	b.tuning = config
}

// SetDatasetID sets the dataset UUID written to the file header.
func (b *Builder) SetDatasetID(id uuid.UUID) {
	b.datasetID = id
}

// AutoTune sets a default tuning configuration.
func (b *Builder) AutoTune(ds *datastore.DataStore, strategy cktype.TuningStrategy, availableRAM, storageBudget uint64) error {
	b.tuning = cktype.TuningConfig{
		NumBands:       4,
		BitsPerBand:    8,
		BucketsPerBand: 256,
		TotalBuckets:   1024,
		Strategy:       strategy,
	}
	return nil
}

func (b *Builder) validateTuning() error {
	t := b.tuning
	if uint16(t.NumBands)*uint16(t.BitsPerBand) > 32 {
		return fmt.Errorf("%w: num_bands*bits_per_band exceeds 32", cktype.ErrInvalidTuning)
	}
	expected := uint32(1) << t.BitsPerBand
	if t.BucketsPerBand != expected {
		return fmt.Errorf("%w: buckets_per_band %d != 2^%d", cktype.ErrInvalidTuning, t.BucketsPerBand, t.BitsPerBand)
	}
	if t.TotalBuckets != uint32(t.NumBands)*t.BucketsPerBand {
		return fmt.Errorf("%w: total_buckets mismatch", cktype.ErrInvalidTuning)
	}
	return nil
}

// BuildFrom iterates all records in the datastore, decompresses fingerprints,
// extracts band values, and builds posting lists for each bucket.
func (b *Builder) BuildFrom(ds *datastore.DataStore) error {
	if err := b.validateTuning(); err != nil {
		return err
	}

	numBuckets := int(b.tuning.TotalBuckets)
	buckets := make([][]cktype.PostingEntry, numBuckets)

	bandMask := uint32((1 << b.tuning.BitsPerBand) - 1)

	count := int(ds.Header.Section0Length / datastore.RecordSize)
	for i := 0; i < count; i++ {
		off := int64(ds.Header.Section0Offset) + int64(i)*datastore.RecordSize
		var buf [datastore.RecordSize]byte
		if _, err := ds.Mmap.ReadAt(buf[:], off); err != nil {
			return fmt.Errorf("reading record %d: %w", i, err)
		}

		rec := &cktype.Record{
			FingerprintID: binary.LittleEndian.Uint32(buf[0:4]),
			DataOffset:    uint64(binary.LittleEndian.Uint32(buf[4:8])) | uint64(buf[8])<<32 | uint64(buf[9])<<40,
			DataLength:    binary.LittleEndian.Uint16(buf[10:12]),
			DurationMs:    binary.LittleEndian.Uint32(buf[12:16]),
			RawCount:      binary.LittleEndian.Uint16(buf[16:18]),
		}

		fp, err := ds.ReadFingerprint(rec)
		if err != nil {
			return fmt.Errorf("decompressing fingerprint %d: %w", rec.FingerprintID, err)
		}

		for p, subFP := range fp.Values {
			for k := uint8(0); k < b.tuning.NumBands; k++ {
				bandValue := (subFP >> (uint32(k) * uint32(b.tuning.BitsPerBand))) & bandMask
				bucketIdx := uint32(k)*b.tuning.BucketsPerBand + bandValue
				buckets[bucketIdx] = append(buckets[bucketIdx], cktype.PostingEntry{
					FingerprintID: rec.FingerprintID,
					Position:      uint16(p),
				})
			}
		}
	}

	for i := range buckets {
		sort.Slice(buckets[i], func(a, c int) bool {
			if buckets[i][a].FingerprintID != buckets[i][c].FingerprintID {
				return buckets[i][a].FingerprintID < buckets[i][c].FingerprintID
			}
			return buckets[i][a].Position < buckets[i][c].Position
		})
	}

	type bucketDirEntry struct {
		offset uint64
		count  uint32
	}
	directory := make([]bucketDirEntry, numBuckets)
	var allPostings []byte
	var totalPostings uint64

	for i, bucket := range buckets {
		directory[i] = bucketDirEntry{
			offset: uint64(len(allPostings)),
			count:  uint32(len(bucket)),
		}
		totalPostings += uint64(len(bucket))
		if len(bucket) == 0 {
			continue
		}

		var encoded []byte
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], bucket[0].FingerprintID)
		encoded = append(encoded, tmp[:]...)
		var ptmp [2]byte
		binary.LittleEndian.PutUint16(ptmp[:], bucket[0].Position)
		encoded = append(encoded, ptmp[:]...)

		prevID := bucket[0].FingerprintID
		for _, entry := range bucket[1:] {
			delta := entry.FingerprintID - prevID
			encoded = wire.AppendVarint(encoded, delta)
			binary.LittleEndian.PutUint16(ptmp[:], entry.Position)
			encoded = append(encoded, ptmp[:]...)
			prevID = entry.FingerprintID
		}

		allPostings = append(allPostings, encoded...)
	}

	b.tuning.TotalPostings = totalPostings
	if numBuckets > 0 {
		b.tuning.AvgPostingsPerBucket = uint32(totalPostings / uint64(numBuckets))
	}

	section0Offset := uint64(wire.HeaderSize + TuningConfigSize)
	section0Length := uint64(numBuckets) * BucketEntrySize

	section1Offset := section0Offset + section0Length
	if section1Offset%8 != 0 {
		section1Offset += 8 - (section1Offset % 8)
	}
	section1Length := uint64(len(allPostings))

	var placeholder [wire.HeaderSize]byte
	if _, err := b.f.WriteAt(placeholder[:], 0); err != nil {
		return fmt.Errorf("writing header placeholder: %w", err)
	}

	if err := b.writeTuningConfig(); err != nil {
		return err
	}

	for i, entry := range directory {
		var buf [BucketEntrySize]byte
		binary.LittleEndian.PutUint64(buf[0:8], entry.offset)
		binary.LittleEndian.PutUint32(buf[8:12], entry.count)
		off := int64(section0Offset) + int64(i)*BucketEntrySize
		if _, err := b.f.WriteAt(buf[:], off); err != nil {
			return fmt.Errorf("writing bucket entry %d: %w", i, err)
		}
	}

	padStart := int64(section0Offset + section0Length)
	padEnd := int64(section1Offset)
	if padEnd > padStart {
		zeros := make([]byte, padEnd-padStart)
		if _, err := b.f.WriteAt(zeros, padStart); err != nil {
			return fmt.Errorf("writing alignment padding: %w", err)
		}
	}

	if len(allPostings) > 0 {
		if _, err := b.f.WriteAt(allPostings, int64(section1Offset)); err != nil {
			return fmt.Errorf("writing posting lists: %w", err)
		}
	}

	footerOffset := int64(section1Offset + section1Length)
	footer := cktype.Footer{
		OverflowOffset: 0,
		Magic:          wire.FooterMagicCKX,
	}
	if err := wire.WriteFooter(b.f, footerOffset, footer); err != nil {
		return fmt.Errorf("writing footer: %w", err)
	}

	var flags uint32
	if b.compression == cktype.CompressPFOR {
		flags |= 1
	}

	h := cktype.FileHeader{
		Magic:          wire.MagicCKX,
		VersionMajor:   wire.CurrentVersionMajor,
		VersionMinor:   wire.CurrentVersionMinor,
		Flags:          flags,
		RecordCount:    totalPostings,
		CreatedAt:      uint64(time.Now().Unix()),
		DatasetID:      b.datasetID,
		Section0Offset: section0Offset,
		Section0Length: section0Length,
		Section1Offset: section1Offset,
		Section1Length: section1Length,
	}
	if err := wire.WriteHeader(b.f, h); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	return nil
}

func (b *Builder) writeTuningConfig() error {
	var buf [TuningConfigSize]byte
	buf[0] = b.tuning.NumBands
	buf[1] = b.tuning.BitsPerBand
	binary.LittleEndian.PutUint32(buf[0x02:], b.tuning.BucketsPerBand)
	binary.LittleEndian.PutUint32(buf[0x06:], b.tuning.TotalBuckets)
	binary.LittleEndian.PutUint64(buf[0x0A:], b.tuning.TotalPostings)
	binary.LittleEndian.PutUint32(buf[0x12:], b.tuning.AvgPostingsPerBucket)
	buf[0x16] = uint8(b.tuning.Strategy)
	_, err := b.f.WriteAt(buf[:], TuningConfigOffset)
	if err != nil {
		return fmt.Errorf("writing tuning config: %w", err)
	}
	return nil
}

// Finish closes the temp file and renames it to the final path.
func (b *Builder) Finish() error {
	tmpPath := b.f.Name()
	if err := b.f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Rename(tmpPath, b.path); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}
	return nil
}
