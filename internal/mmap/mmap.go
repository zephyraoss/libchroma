package mmap

import "io"

// Data represents a memory-mapped file region.
type Data struct {
	Bytes []byte
}

// ReadAt implements io.ReaderAt over the mapped region.
func (m *Data) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(m.Bytes)) {
		return 0, io.EOF
	}
	n := copy(p, m.Bytes[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Len returns the length of the mapped region.
func (m *Data) Len() int {
	return len(m.Bytes)
}
