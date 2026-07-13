package cktype

import "github.com/google/uuid"

type CompressionMethod uint8

const (
	CompressVarint CompressionMethod = 0
	CompressPFOR   CompressionMethod = 1
)

type TuningStrategy uint8

const (
	TuneManual   TuningStrategy = 0
	TuneBalanced TuningStrategy = 1
	TuneLowRAM   TuningStrategy = 2
	TuneSpeed    TuningStrategy = 3
)

type FileHeader struct {
	Magic          [8]byte
	VersionMajor   uint16
	VersionMinor   uint16
	Flags          uint32
	RecordCount    uint64
	CreatedAt      uint64
	SourceDate     uint64
	DatasetID      uuid.UUID
	Section0Offset uint64
	Section0Length uint64
	Section1Offset uint64
	Section1Length uint64
}

type Record struct {
	FingerprintID uint32
	DataOffset    uint64
	DataLength    uint16
	DurationMs    uint32
	RawCount      uint16
	FromOverflow  bool
}

type Fingerprint struct {
	ID         uint32
	DurationMs uint32
	Values     []uint32
}

type PostingEntry struct {
	FingerprintID uint32
	Position      uint16
}

type SampledPosting struct {
	FingerprintID uint32
	Ordinal       uint8
}

type PostingHit struct {
	FingerprintID uint32
	Hits          int
	Delta         int
}

type PostingQueryOptions struct {
	MinHits int
	TopK    int
}

type MappingRecord struct {
	FingerprintID uint32
	MBID          uuid.UUID
	TrackID       uint32
	StringOffset  uint32
	StringLength  uint32
	FromOverflow  bool
}

type TrackMetadata struct {
	Title   string
	Artist  string
	Release string
	Year    string
}

type Match struct {
	FingerprintID uint32
	BitErrorRate  float64
	Score         MatchScore
}

type MatchScore int

const (
	MatchStrong MatchScore = iota
	MatchLikely
	MatchWeak
	MatchNone
)

type MatchResult struct {
	Match    Match
	Metadata *TrackMetadata
	MBID     *uuid.UUID
}

type TuningConfig struct {
	NumBands             uint8
	BitsPerBand          uint8
	BucketsPerBand       uint32
	TotalBuckets         uint32
	TotalPostings        uint64
	AvgPostingsPerBucket uint32
	Strategy             TuningStrategy

	Stride         uint8
	QBits          uint8
	SkipInterval   uint32
	BucketCount    uint32
	SkipEntryCount uint32
}

type BucketEntry struct {
	PostingOffset uint64
	PostingCount  uint32
}

type Footer struct {
	OverflowOffset uint64
	Magic          [8]byte
}

type QueryOptions struct {
	MaxCandidates     int
	MinMatchThreshold int
	MaxBitErrorRate   float64
	IncludeMetadata   bool
}

type DatasetOptions struct {
	ReadOnly bool
}

type DatasetStats struct {
	RecordCount        uint64
	HasOverflow        bool
	OverflowCount      uint32
	HasMetadata        bool
	MetadataCount      uint64
	HasSearchIndex     bool
	TuningConfig       TuningConfig
	HasPostingIndex    bool
	PostingIndexTuning TuningConfig
	HasDataStore       bool
}

type OverflowRecord struct {
	FingerprintID uint32
	DurationMs    uint32
	Values        []uint32
}

type OverflowMappingRecord struct {
	FingerprintID uint32
	MBID          uuid.UUID
	TrackID       uint32
	Metadata      *TrackMetadata
}
