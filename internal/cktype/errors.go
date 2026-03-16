package cktype

import "errors"

var (
	ErrBadMagic           = errors.New("ckaf: invalid magic number")
	ErrUnsupportedVersion = errors.New("ckaf: unsupported format version")
	ErrUnknownFlags       = errors.New("ckaf: unknown flag bits set")
	ErrCorruptOverflow    = errors.New("ckaf: overflow region failed validation")
	ErrOffsetOutOfBounds  = errors.New("ckaf: offset exceeds file size")
	ErrRecordNotFound     = errors.New("ckaf: fingerprint ID not found")
	ErrDatasetMismatch    = errors.New("ckaf: dataset_id mismatch between files")
	ErrInvalidTuning      = errors.New("ckaf: invalid tuning configuration")
	ErrEmptyDataset       = errors.New("ckaf: dataset contains no records")
	ErrInvalidCompression = errors.New("ckaf: invalid compressed data")
	ErrTruncatedVarint    = errors.New("ckaf: truncated varint encoding")
)
