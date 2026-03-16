package chroma

import "io"

// mmapData represents a memory-mapped file region.
type mmapData struct {
	data []byte
}

// ReadAt implements io.ReaderAt over the mapped region.
func (m *mmapData) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Len returns the length of the mapped region.
func (m *mmapData) Len() int {
	return len(m.data)
}
