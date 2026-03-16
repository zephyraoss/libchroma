package wire

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/zephyraoss/libchroma/internal/cktype"
)

const (
	HeaderSize          = 96
	CurrentVersionMajor = 0
	CurrentVersionMinor = 1
)

var (
	MagicCKD = [8]byte{'C', 'K', 'A', 'F', '-', 'D', 0, 0}
	MagicCKX = [8]byte{'C', 'K', 'A', 'F', '-', 'X', 0, 0}
	MagicCKM = [8]byte{'C', 'K', 'A', 'F', '-', 'M', 0, 0}
)

// ReadHeader reads and validates a 96-byte CKAF header from r.
func ReadHeader(r io.ReaderAt, expectedMagic [8]byte, fileSize int64) (cktype.FileHeader, error) {
	if fileSize < HeaderSize {
		return cktype.FileHeader{}, fmt.Errorf("%w: file too small for header", cktype.ErrBadMagic)
	}

	var buf [HeaderSize]byte
	if _, err := r.ReadAt(buf[:], 0); err != nil {
		return cktype.FileHeader{}, fmt.Errorf("reading header: %w", err)
	}

	var h cktype.FileHeader
	copy(h.Magic[:], buf[0:8])
	if h.Magic != expectedMagic {
		return cktype.FileHeader{}, fmt.Errorf("%w: got %q, want %q", cktype.ErrBadMagic, h.Magic[:], expectedMagic[:])
	}

	h.VersionMajor = binary.LittleEndian.Uint16(buf[0x08:])
	h.VersionMinor = binary.LittleEndian.Uint16(buf[0x0A:])
	h.Flags = binary.LittleEndian.Uint32(buf[0x0C:])
	h.RecordCount = binary.LittleEndian.Uint64(buf[0x10:])
	h.CreatedAt = binary.LittleEndian.Uint64(buf[0x18:])
	h.SourceDate = binary.LittleEndian.Uint64(buf[0x20:])
	copy(h.DatasetID[:], buf[0x28:0x38])

	h.Section0Offset = binary.LittleEndian.Uint64(buf[0x40:])
	h.Section0Length = binary.LittleEndian.Uint64(buf[0x48:])
	h.Section1Offset = binary.LittleEndian.Uint64(buf[0x50:])
	h.Section1Length = binary.LittleEndian.Uint64(buf[0x58:])

	if h.VersionMajor != CurrentVersionMajor {
		return cktype.FileHeader{}, fmt.Errorf("%w: version %d.%d", cktype.ErrUnsupportedVersion, h.VersionMajor, h.VersionMinor)
	}

	uFileSize := uint64(fileSize)
	if h.Section0Length > uFileSize || h.Section0Offset > uFileSize-h.Section0Length {
		return cktype.FileHeader{}, fmt.Errorf("%w: section 0 extends beyond file", cktype.ErrOffsetOutOfBounds)
	}
	if h.Section1Length > uFileSize || h.Section1Offset > uFileSize-h.Section1Length {
		return cktype.FileHeader{}, fmt.Errorf("%w: section 1 extends beyond file", cktype.ErrOffsetOutOfBounds)
	}

	return h, nil
}

// WriteHeader writes a 96-byte CKAF header to w at offset 0.
func WriteHeader(w io.WriterAt, h cktype.FileHeader) error {
	var buf [HeaderSize]byte
	copy(buf[0:8], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[0x08:], h.VersionMajor)
	binary.LittleEndian.PutUint16(buf[0x0A:], h.VersionMinor)
	binary.LittleEndian.PutUint32(buf[0x0C:], h.Flags)
	binary.LittleEndian.PutUint64(buf[0x10:], h.RecordCount)
	binary.LittleEndian.PutUint64(buf[0x18:], h.CreatedAt)
	binary.LittleEndian.PutUint64(buf[0x20:], h.SourceDate)
	copy(buf[0x28:0x38], h.DatasetID[:])

	binary.LittleEndian.PutUint64(buf[0x40:], h.Section0Offset)
	binary.LittleEndian.PutUint64(buf[0x48:], h.Section0Length)
	binary.LittleEndian.PutUint64(buf[0x50:], h.Section1Offset)
	binary.LittleEndian.PutUint64(buf[0x58:], h.Section1Length)

	_, err := w.WriteAt(buf[:], 0)
	return err
}

// ValidateFlags checks that no unknown flag bits are set.
func ValidateFlags(flags uint32, knownMask uint32) error {
	if flags & ^knownMask != 0 {
		return fmt.Errorf("%w: 0x%08X", cktype.ErrUnknownFlags, flags & ^knownMask)
	}
	return nil
}

// SetHeaderFlagBit reads the current flags from offset 0x0C, ORs with the given
// bits, and writes them back.
func SetHeaderFlagBit(f io.ReaderAt, w io.WriterAt, bits uint32) error {
	var buf [4]byte
	if _, err := f.ReadAt(buf[:], 0x0C); err != nil {
		return fmt.Errorf("reading flags: %w", err)
	}
	flags := binary.LittleEndian.Uint32(buf[:])
	flags |= bits
	binary.LittleEndian.PutUint32(buf[:], flags)
	if _, err := w.WriteAt(buf[:], 0x0C); err != nil {
		return fmt.Errorf("writing flags: %w", err)
	}
	return nil
}
