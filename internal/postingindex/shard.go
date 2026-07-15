package postingindex

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/zephyraoss/libchroma/v2/internal/wire"
)

const (
	shardsPerWorker   = 4
	runSampleRecords  = 256
	shardReadBufBytes = 128 << 10
	shardDirEntrySize = 12
	shardCopyBufBytes = 1 << 20
)

type runFile struct {
	f       *os.File
	records int64
}

type shardResult struct {
	bodyPath    string
	dirPath     string
	firstHash   uint32
	lastHash    uint32
	bodyLen     uint64
	bucketCount uint64
	postings    uint64
}

type shardBlob struct {
	delta   []byte
	path    string
	bodyLen uint64
}

func (b *Builder) writeFileSharded() error {
	runs, err := b.openRuns()
	if err != nil {
		return err
	}
	defer closeRuns(runs)

	var totalRecords int64
	for _, r := range runs {
		totalRecords += r.records
	}
	if totalRecords == 0 {
		return b.writeSections(nil, 0, 0, 0, func(int64) error { return nil })
	}

	partitionStart := time.Now()
	bounds, err := sampleShardBounds(runs, b.concurrency*shardsPerWorker)
	if err != nil {
		return err
	}
	cuts, err := computeRunCuts(runs, bounds)
	if err != nil {
		return err
	}
	b.logf("cki finish partitioned runs=%d run_postings=%d shards=%d elapsed=%s",
		len(runs), totalRecords, len(bounds)-1, time.Since(partitionStart).Round(time.Millisecond))

	mergeStart := time.Now()
	shards, err := b.mergeShards(runs, cuts)
	if err != nil {
		return err
	}
	b.logf("cki finish shards merged elapsed=%s", time.Since(mergeStart).Round(time.Millisecond))

	planStart := time.Now()
	skip, bucketCount, total, blobLen, plan, err := b.assembleShardPlan(shards)
	if err != nil {
		return err
	}
	b.logf("cki finish skip directory built buckets=%d postings=%d skip_entries=%d elapsed=%s",
		bucketCount, total, len(skip), time.Since(planStart).Round(time.Millisecond))

	copyStart := time.Now()
	if err := b.writeSections(skip, bucketCount, total, blobLen, func(offset int64) error {
		return writeShardBlobs(b.f, offset, plan)
	}); err != nil {
		return err
	}
	b.logf("cki finish blob concatenated bytes=%d elapsed=%s", blobLen, time.Since(copyStart).Round(time.Millisecond))
	return nil
}

func (b *Builder) openRuns() ([]*runFile, error) {
	runs := make([]*runFile, 0, b.runCount)
	fail := func(err error) ([]*runFile, error) {
		closeRuns(runs)
		return nil, err
	}
	for i := 0; i < b.runCount; i++ {
		f, err := os.Open(b.runPath(i))
		if err != nil {
			return fail(fmt.Errorf("opening run file: %w", err))
		}
		runs = append(runs, &runFile{f: f})
		info, err := f.Stat()
		if err != nil {
			return fail(fmt.Errorf("stat run file: %w", err))
		}
		if info.Size()%spillRunRecordSize != 0 {
			return fail(fmt.Errorf("run file %s size %d is not a whole number of records", f.Name(), info.Size()))
		}
		runs[i].records = info.Size() / spillRunRecordSize
	}
	return runs, nil
}

func closeRuns(runs []*runFile) {
	for _, r := range runs {
		r.f.Close()
	}
}

func readRunHash(f *os.File, record int64) (uint32, error) {
	var buf [4]byte
	if _, err := f.ReadAt(buf[:], record*spillRunRecordSize); err != nil {
		return 0, fmt.Errorf("reading run file: %w", err)
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func sampleShardBounds(runs []*runFile, shardCount int) ([]uint64, error) {
	if shardCount < 1 {
		shardCount = 1
	}
	var samples []uint32
	for _, r := range runs {
		if r.records == 0 {
			continue
		}
		step := r.records / runSampleRecords
		if step == 0 {
			step = 1
		}
		for i := int64(0); i < r.records; i += step {
			h, err := readRunHash(r.f, i)
			if err != nil {
				return nil, err
			}
			samples = append(samples, h)
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	bounds := []uint64{0}
	for i := 1; i < shardCount && len(samples) > 0; i++ {
		h := uint64(samples[i*len(samples)/shardCount])
		if h > bounds[len(bounds)-1] {
			bounds = append(bounds, h)
		}
	}
	return append(bounds, 1<<32), nil
}

func computeRunCuts(runs []*runFile, bounds []uint64) ([][]int64, error) {
	cuts := make([][]int64, len(runs))
	for ri, r := range runs {
		c := make([]int64, len(bounds))
		c[len(bounds)-1] = r.records
		var searchErr error
		for bi := 1; bi < len(bounds)-1; bi++ {
			bound := uint32(bounds[bi])
			lo := c[bi-1]
			c[bi] = lo + int64(sort.Search(int(r.records-lo), func(k int) bool {
				if searchErr != nil {
					return true
				}
				h, err := readRunHash(r.f, lo+int64(k))
				if err != nil {
					searchErr = err
					return true
				}
				return h >= bound
			}))
			if searchErr != nil {
				return nil, searchErr
			}
		}
		cuts[ri] = c
	}
	return cuts, nil
}

func (b *Builder) mergeShards(runs []*runFile, cuts [][]int64) ([]*shardResult, error) {
	shardCount := len(cuts[0]) - 1
	results := make([]*shardResult, shardCount)

	var mu sync.Mutex
	var firstErr error
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}

	workers := b.concurrency
	if workers > shardCount {
		workers = shardCount
	}
	shardCh := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range shardCh {
				if failed() {
					continue
				}
				res, err := b.mergeShard(s, runs, cuts)
				if err != nil {
					fail(err)
					continue
				}
				results[s] = res
			}
		}()
	}
	for s := 0; s < shardCount; s++ {
		shardCh <- s
	}
	close(shardCh)
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

func (b *Builder) mergeShard(s int, runs []*runFile, cuts [][]int64) (*shardResult, error) {
	res := &shardResult{
		bodyPath: filepath.Join(b.runDir, fmt.Sprintf("shard-%05d.body", s)),
		dirPath:  filepath.Join(b.runDir, fmt.Sprintf("shard-%05d.dir", s)),
	}
	body, err := os.Create(res.bodyPath)
	if err != nil {
		return nil, fmt.Errorf("creating shard body: %w", err)
	}
	defer body.Close()
	dir, err := os.Create(res.dirPath)
	if err != nil {
		return nil, fmt.Errorf("creating shard directory: %w", err)
	}
	defer dir.Close()

	h := make(runHeap, 0, len(runs))
	for ri, r := range runs {
		lo := cuts[ri][s] * spillRunRecordSize
		hi := cuts[ri][s+1] * spillRunRecordSize
		if lo == hi {
			continue
		}
		rd := &runReader{r: bufio.NewReaderSize(io.NewSectionReader(r.f, lo, hi-lo), shardReadBufBytes)}
		ok, err := rd.next()
		if err != nil {
			return nil, err
		}
		if ok {
			h = append(h, rd)
		}
	}

	bodyW := bufio.NewWriterSize(body, 1<<20)
	dirW := bufio.NewWriterSize(dir, 256<<10)
	enc := &shardEncoder{w: bodyW, dir: dirW}
	if err := mergeHeap(&h, enc.encodeBucket); err != nil {
		return nil, err
	}
	if err := bodyW.Flush(); err != nil {
		return nil, fmt.Errorf("flushing shard body: %w", err)
	}
	if err := dirW.Flush(); err != nil {
		return nil, fmt.Errorf("flushing shard directory: %w", err)
	}

	res.firstHash = enc.firstHash
	res.lastHash = enc.prevHash
	res.bodyLen = enc.blobLen
	res.bucketCount = enc.bucketCount
	res.postings = enc.total
	return res, nil
}

type shardEncoder struct {
	w           io.Writer
	dir         io.Writer
	scratch     []byte
	started     bool
	firstHash   uint32
	prevHash    uint32
	blobLen     uint64
	bucketCount uint64
	total       uint64
}

func (e *shardEncoder) encodeBucket(postings []Posting) error {
	if len(postings) == 0 {
		return nil
	}
	h := postings[0].Hash
	var entry [shardDirEntrySize]byte
	binary.LittleEndian.PutUint32(entry[0:4], h)
	binary.LittleEndian.PutUint64(entry[4:12], e.blobLen)
	if _, err := e.dir.Write(entry[:]); err != nil {
		return err
	}
	buf := e.scratch[:0]
	if e.started {
		buf = wire.AppendVarint(buf, h-e.prevHash)
	} else {
		e.firstHash = h
		e.started = true
	}
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

func (b *Builder) assembleShardPlan(shards []*shardResult) ([]SkipEntry, uint32, uint64, uint64, []shardBlob, error) {
	skipInterval := uint64(b.tuning.SkipInterval)
	var (
		skip       []SkipEntry
		prevHash   uint32
		bucketBase uint64
		blobLen    uint64
		total      uint64
		plan       []shardBlob
	)
	for _, s := range shards {
		if s.bucketCount == 0 {
			continue
		}
		delta := wire.AppendVarint(nil, s.firstHash-prevHash)
		segStart := blobLen
		if err := scanShardDir(s.dirPath, func(local uint64, hash uint32, off uint64) {
			if (bucketBase+local)%skipInterval != 0 {
				return
			}
			gOff := segStart
			if local > 0 {
				gOff = segStart + uint64(len(delta)) + off
			}
			skip = append(skip, SkipEntry{Hash: hash, Offset: gOff})
		}); err != nil {
			return nil, 0, 0, 0, nil, err
		}
		plan = append(plan, shardBlob{delta: delta, path: s.bodyPath, bodyLen: s.bodyLen})
		blobLen += uint64(len(delta)) + s.bodyLen
		bucketBase += s.bucketCount
		total += s.postings
		prevHash = s.lastHash
	}
	return skip, uint32(bucketBase), total, blobLen, plan, nil
}

func scanShardDir(path string, fn func(local uint64, hash uint32, off uint64)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening shard directory: %w", err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256<<10)
	var entry [shardDirEntrySize]byte
	for local := uint64(0); ; local++ {
		if _, err := io.ReadFull(r, entry[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("reading shard directory: %w", err)
		}
		fn(local, binary.LittleEndian.Uint32(entry[0:4]), binary.LittleEndian.Uint64(entry[4:12]))
	}
}

func writeShardBlobs(f *os.File, offset int64, plan []shardBlob) error {
	w := bufio.NewWriterSize(io.NewOffsetWriter(f, offset), shardCopyBufBytes)
	copyBuf := make([]byte, shardCopyBufBytes)
	for _, sb := range plan {
		if _, err := w.Write(sb.delta); err != nil {
			return fmt.Errorf("copying posting buckets: %w", err)
		}
		src, err := os.Open(sb.path)
		if err != nil {
			return fmt.Errorf("opening shard body: %w", err)
		}
		n, err := io.CopyBuffer(w, src, copyBuf)
		src.Close()
		if err != nil {
			return fmt.Errorf("copying posting buckets: %w", err)
		}
		if n != int64(sb.bodyLen) {
			return fmt.Errorf("shard body %s is %d bytes, expected %d", sb.path, n, sb.bodyLen)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("copying posting buckets: %w", err)
	}
	return nil
}
