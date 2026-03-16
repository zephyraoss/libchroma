package chroma

import "github.com/zephyraoss/libchroma/internal/cktype"

// Type aliases re-export internal types as the public API.
type (
	CompressionMethod    = cktype.CompressionMethod
	TuningStrategy       = cktype.TuningStrategy
	FileHeader           = cktype.FileHeader
	Record               = cktype.Record
	Fingerprint          = cktype.Fingerprint
	PostingEntry         = cktype.PostingEntry
	MappingRecord        = cktype.MappingRecord
	TrackMetadata        = cktype.TrackMetadata
	Match                = cktype.Match
	MatchScore           = cktype.MatchScore
	MatchResult          = cktype.MatchResult
	TuningConfig         = cktype.TuningConfig
	BucketEntry          = cktype.BucketEntry
	Footer               = cktype.Footer
	QueryOptions         = cktype.QueryOptions
	DatasetOptions       = cktype.DatasetOptions
	DatasetStats         = cktype.DatasetStats
	OverflowRecord       = cktype.OverflowRecord
	OverflowMappingRecord = cktype.OverflowMappingRecord
)

const (
	CompressVarint = cktype.CompressVarint
	CompressPFOR   = cktype.CompressPFOR

	TuneManual   = cktype.TuneManual
	TuneBalanced = cktype.TuneBalanced
	TuneLowRAM   = cktype.TuneLowRAM
	TuneSpeed    = cktype.TuneSpeed

	MatchStrong = cktype.MatchStrong
	MatchLikely = cktype.MatchLikely
	MatchWeak   = cktype.MatchWeak
	MatchNone   = cktype.MatchNone
)
