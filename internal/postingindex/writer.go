package postingindex

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/zephyraoss/libchroma/v2/internal/cktype"
	"github.com/zephyraoss/libchroma/v2/internal/datastore"
	"github.com/zephyraoss/libchroma/v2/internal/wire"
)

const (
	DefaultSpillBufferBytes = 1 << 30

	spillRunRecordSize = 9

	postingMemBytes = 12
)

type Builder struct {
	f         *os.File
	path      string
	datasetID uuid.UUID
	tuning    cktype.TuningConfig
	postings  []Posting

	spillDir string
	runDir   string
	runLimit int
	runCount int
}

type BuilderOptions struct {
	SpillDir string

	SpillBufferBytes int64
}

func NewBuilder(path string) (*Builder, error) {
	return NewBuilderWithOptions(path, BuilderOptions{})
}

func NewBuilderWithOptions(path string, opts BuilderOptions) (*Builder, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "cki-build-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	b := &Builder{
		f:    f,
		path: path,
		tuning: cktype.TuningConfig{
			Stride:       DefaultStride,
			QBits:        DefaultQBits,
			SkipInterval: DefaultSkipInterval,
		},
	}
	if opts.SpillDir != "" {
		runDir, err := os.MkdirTemp(opts.SpillDir, "cki-runs-*")
		if err != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, fmt.Errorf("creating run directory: %w", err)
		}
		bufBytes := opts.SpillBufferBytes
		if bufBytes <= 0 {
			bufBytes = DefaultSpillBufferBytes
		}
		b.spillDir = opts.SpillDir
		b.runDir = runDir
		b.runLimit = int(bufBytes / postingMemBytes)
		if b.runLimit < 1 {
			b.runLimit = 1
		}
	}
	return b, nil
}

func (b *Builder) SetDatasetID(id uuid.UUID) {
	b.datasetID = id
}

func (b *Builder) SetTuningConfig(config cktype.TuningConfig) {
	if config.Stride == 0 {
		config.Stride = b.tuning.Stride
	}
	if config.SkipInterval == 0 {
		config.SkipInterval = b.tuning.SkipInterval
	}
	b.tuning = config
}

func (b *Builder) validateTuning() error {
	t := b.tuning
	if t.Stride == 0 {
		return fmt.Errorf("%w: stride must be >= 1", cktype.ErrInvalidTuning)
	}
	if t.QBits > 24 {
		return fmt.Errorf("%w: qbits %d exceeds 24", cktype.ErrInvalidTuning, t.QBits)
	}
	if t.SkipInterval == 0 {
		return fmt.Errorf("%w: skip_interval must be >= 1", cktype.ErrInvalidTuning)
	}
	return nil
}

func (b *Builder) Add(fpID uint32, sampledHashes []uint32, ordinals []uint8) error {
	if len(sampledHashes) != len(ordinals) {
		return fmt.Errorf("ckaf: fingerprint %d: %d hashes vs %d ordinals", fpID, len(sampledHashes), len(ordinals))
	}
	qmask := QuantizeMask(b.tuning.QBits)
	for i := range sampledHashes {
		b.postings = append(b.postings, Posting{
			Hash:          sampledHashes[i] & qmask,
			FingerprintID: fpID,
			Ordinal:       ordinals[i],
		})
	}
	return b.maybeFlushRun()
}

func (b *Builder) BuildFrom(ds *datastore.DataStore) error {
	if err := b.validateTuning(); err != nil {
		return err
	}
	count := int(ds.Header.Section0Length / datastore.RecordSize)
	for i := 0; i < count; i++ {
		off := int64(ds.Header.Section0Offset) + int64(i)*datastore.RecordSize
		rec, err := ds.ReadRecordAt(off)
		if err != nil {
			return fmt.Errorf("reading record %d: %w", i, err)
		}
		fp, err := ds.ReadFingerprint(rec)
		if err != nil {
			return fmt.Errorf("decompressing fingerprint %d: %w", rec.FingerprintID, err)
		}
		b.postings = AppendSampled(b.postings, rec.FingerprintID, fp.Values, b.tuning.Stride, b.tuning.QBits)
		if err := b.maybeFlushRun(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) maybeFlushRun() error {
	if b.runDir == "" || len(b.postings) < b.runLimit {
		return nil
	}
	return b.flushRun()
}

func (b *Builder) flushRun() error {
	if len(b.postings) == 0 {
		return nil
	}
	sorted := Prepare(b.postings)
	f, err := os.Create(b.runPath(b.runCount))
	if err != nil {
		return fmt.Errorf("creating run file: %w", err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var rec [spillRunRecordSize]byte
	for _, p := range sorted {
		binary.LittleEndian.PutUint32(rec[0:4], p.Hash)
		binary.LittleEndian.PutUint32(rec[4:8], p.FingerprintID)
		rec[8] = p.Ordinal
		if _, err := w.Write(rec[:]); err != nil {
			f.Close()
			return fmt.Errorf("writing run file: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("flushing run file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing run file: %w", err)
	}
	b.runCount++
	b.postings = b.postings[:0]
	return nil
}

func (b *Builder) runPath(i int) string {
	return filepath.Join(b.runDir, fmt.Sprintf("run-%06d", i))
}

func (b *Builder) Finish() error {
	if b.runDir != "" {
		defer os.RemoveAll(b.runDir)
	}
	if err := b.writeFile(); err != nil {
		b.f.Close()
		os.Remove(b.f.Name())
		return err
	}
	tmpPath := b.f.Name()
	if err := b.f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Rename(tmpPath, b.path); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}
	return nil
}

func (b *Builder) writeFile() error {
	if err := b.validateTuning(); err != nil {
		return err
	}
	if b.runDir != "" {
		return b.writeFileSpill()
	}

	sorted := Prepare(b.postings)
	blob, skip, bucketCount := EncodePostings(sorted, b.tuning.SkipInterval)

	return b.writeSections(skip, bucketCount, uint64(len(sorted)), uint64(len(blob)), func(offset int64) error {
		if len(blob) == 0 {
			return nil
		}
		if _, err := b.f.WriteAt(blob, offset); err != nil {
			return fmt.Errorf("writing posting buckets: %w", err)
		}
		return nil
	})
}

func (b *Builder) writeFileSpill() error {
	if err := b.flushRun(); err != nil {
		return err
	}

	blobFile, err := os.CreateTemp(b.spillDir, "cki-blob-*.tmp")
	if err != nil {
		return fmt.Errorf("creating blob spool: %w", err)
	}
	defer func() {
		blobFile.Close()
		os.Remove(blobFile.Name())
	}()

	blobW := bufio.NewWriterSize(blobFile, 1<<20)
	enc := newBucketEncoder(blobW, b.tuning.SkipInterval)
	if err := b.mergeRuns(enc); err != nil {
		return err
	}
	if err := blobW.Flush(); err != nil {
		return fmt.Errorf("flushing blob spool: %w", err)
	}

	return b.writeSections(enc.skip, enc.bucketCount, enc.total, enc.blobLen, func(offset int64) error {
		if enc.blobLen == 0 {
			return nil
		}
		if _, err := blobFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewinding blob spool: %w", err)
		}
		if _, err := io.Copy(io.NewOffsetWriter(b.f, offset), bufio.NewReaderSize(blobFile, 1<<20)); err != nil {
			return fmt.Errorf("copying posting buckets: %w", err)
		}
		return nil
	})
}

func (b *Builder) mergeRuns(enc *bucketEncoder) error {
	readers := make([]*runReader, 0, b.runCount)
	defer func() {
		for _, r := range readers {
			r.f.Close()
		}
	}()
	h := make(runHeap, 0, b.runCount)
	for i := 0; i < b.runCount; i++ {
		f, err := os.Open(b.runPath(i))
		if err != nil {
			return fmt.Errorf("opening run file: %w", err)
		}
		r := &runReader{f: f, r: bufio.NewReaderSize(f, 1<<20)}
		readers = append(readers, r)
		ok, err := r.next()
		if err != nil {
			return err
		}
		if ok {
			h = append(h, r)
		}
	}
	heap.Init(&h)

	var bucket []Posting
	var last Posting
	var emitted bool
	for h.Len() > 0 {
		r := h[0]
		p := r.cur
		ok, err := r.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
		if emitted && p == last {
			continue
		}
		if emitted && p.Hash != last.Hash {
			if err := enc.encodeBucket(bucket); err != nil {
				return fmt.Errorf("writing posting buckets: %w", err)
			}
			bucket = bucket[:0]
		}
		bucket = append(bucket, p)
		last = p
		emitted = true
	}
	if err := enc.encodeBucket(bucket); err != nil {
		return fmt.Errorf("writing posting buckets: %w", err)
	}
	return nil
}

type runReader struct {
	f   *os.File
	r   *bufio.Reader
	cur Posting
}

func (r *runReader) next() (bool, error) {
	var buf [spillRunRecordSize]byte
	if _, err := io.ReadFull(r.r, buf[:]); err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, fmt.Errorf("reading run file: %w", err)
	}
	r.cur = Posting{
		Hash:          binary.LittleEndian.Uint32(buf[0:4]),
		FingerprintID: binary.LittleEndian.Uint32(buf[4:8]),
		Ordinal:       buf[8],
	}
	return true, nil
}

type runHeap []*runReader

func (h runHeap) Len() int           { return len(h) }
func (h runHeap) Less(i, j int) bool { return postingLess(h[i].cur, h[j].cur) }
func (h runHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)        { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (b *Builder) writeSections(skip []SkipEntry, bucketCount uint32, totalPostings, blobLen uint64, writeBlob func(offset int64) error) error {
	b.tuning.TotalPostings = totalPostings
	b.tuning.BucketCount = bucketCount
	b.tuning.SkipEntryCount = uint32(len(skip))
	if bucketCount > 0 {
		b.tuning.AvgPostingsPerBucket = uint32(totalPostings / uint64(bucketCount))
	}

	section0Offset := uint64(wire.HeaderSize + TuningConfigSize)
	section0Length := uint64(len(skip)) * SkipEntrySize

	section1Offset := section0Offset + section0Length
	if section1Offset%8 != 0 {
		section1Offset += 8 - (section1Offset % 8)
	}
	section1Length := blobLen

	var placeholder [wire.HeaderSize]byte
	if _, err := b.f.WriteAt(placeholder[:], 0); err != nil {
		return fmt.Errorf("writing header placeholder: %w", err)
	}

	if err := b.writeTuningConfig(); err != nil {
		return err
	}

	skipDir := make([]byte, section0Length)
	for i, e := range skip {
		off := i * SkipEntrySize
		binary.LittleEndian.PutUint32(skipDir[off:], e.Hash)
		binary.LittleEndian.PutUint64(skipDir[off+4:], e.Offset)
	}
	if len(skipDir) > 0 {
		if _, err := b.f.WriteAt(skipDir, int64(section0Offset)); err != nil {
			return fmt.Errorf("writing skip directory: %w", err)
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

	if err := writeBlob(int64(section1Offset)); err != nil {
		return err
	}

	footerOffset := int64(section1Offset + section1Length)
	footer := cktype.Footer{
		OverflowOffset: 0,
		Magic:          wire.FooterMagicCKI,
	}
	if err := wire.WriteFooter(b.f, footerOffset, footer); err != nil {
		return fmt.Errorf("writing footer: %w", err)
	}

	h := cktype.FileHeader{
		Magic:          wire.MagicCKI,
		VersionMajor:   wire.CurrentVersionMajor,
		VersionMinor:   wire.CurrentVersionMinor,
		Flags:          0,
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
	buf[0] = b.tuning.Stride
	buf[1] = b.tuning.QBits
	binary.LittleEndian.PutUint32(buf[0x02:], b.tuning.SkipInterval)
	binary.LittleEndian.PutUint32(buf[0x06:], b.tuning.BucketCount)
	binary.LittleEndian.PutUint64(buf[0x0A:], b.tuning.TotalPostings)
	binary.LittleEndian.PutUint32(buf[0x12:], b.tuning.SkipEntryCount)
	buf[0x16] = uint8(b.tuning.Strategy)
	if _, err := b.f.WriteAt(buf[:], TuningConfigOffset); err != nil {
		return fmt.Errorf("writing tuning config: %w", err)
	}
	return nil
}
