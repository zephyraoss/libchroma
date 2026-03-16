package chroma

import (
	"encoding/binary"
	"fmt"
	"io"
)

const FooterSize = 16

var (
	FooterMagicCKD = [8]byte{'C', 'K', 'A', 'F', '-', 'D', 'F', 0}
	FooterMagicCKX = [8]byte{'C', 'K', 'A', 'F', '-', 'X', 'F', 0}
	FooterMagicCKM = [8]byte{'C', 'K', 'A', 'F', '-', 'M', 'F', 0}
)

// ReadFooter reads and validates the 16-byte footer from the end of the file.
func ReadFooter(r io.ReaderAt, fileSize int64, expectedMagic [8]byte) (Footer, error) {
	if fileSize < HeaderSize+FooterSize {
		return Footer{}, fmt.Errorf("%w: file too small for footer", ErrBadMagic)
	}

	var buf [FooterSize]byte
	off := fileSize - FooterSize
	if _, err := r.ReadAt(buf[:], off); err != nil {
		return Footer{}, fmt.Errorf("reading footer: %w", err)
	}

	var f Footer
	f.OverflowOffset = binary.LittleEndian.Uint64(buf[0:8])
	copy(f.Magic[:], buf[8:16])

	if f.Magic != expectedMagic {
		return Footer{}, fmt.Errorf("%w: footer magic got %q, want %q", ErrBadMagic, f.Magic[:], expectedMagic[:])
	}

	// Validate overflow offset
	if f.OverflowOffset != 0 && f.OverflowOffset >= uint64(fileSize)-FooterSize {
		return Footer{}, fmt.Errorf("%w: overflow_offset %d beyond file", ErrOffsetOutOfBounds, f.OverflowOffset)
	}

	return f, nil
}

// WriteFooter writes a 16-byte footer at the given offset.
func WriteFooter(w io.WriterAt, offset int64, f Footer) error {
	var buf [FooterSize]byte
	binary.LittleEndian.PutUint64(buf[0:8], f.OverflowOffset)
	copy(buf[8:16], f.Magic[:])
	_, err := w.WriteAt(buf[:], offset)
	return err
}
