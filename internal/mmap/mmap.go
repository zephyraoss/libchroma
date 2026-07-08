package mmap

import "io"

type Data struct {
	Bytes []byte
}

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

func (m *Data) Len() int {
	return len(m.Bytes)
}
