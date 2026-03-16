package chroma

import "github.com/zephyraoss/libchroma/internal/cktype"

var (
	ErrBadMagic           = cktype.ErrBadMagic
	ErrUnsupportedVersion = cktype.ErrUnsupportedVersion
	ErrUnknownFlags       = cktype.ErrUnknownFlags
	ErrCorruptOverflow    = cktype.ErrCorruptOverflow
	ErrOffsetOutOfBounds  = cktype.ErrOffsetOutOfBounds
	ErrRecordNotFound     = cktype.ErrRecordNotFound
	ErrDatasetMismatch    = cktype.ErrDatasetMismatch
	ErrInvalidTuning      = cktype.ErrInvalidTuning
	ErrEmptyDataset       = cktype.ErrEmptyDataset
	ErrInvalidCompression = cktype.ErrInvalidCompression
	ErrTruncatedVarint    = cktype.ErrTruncatedVarint
)
