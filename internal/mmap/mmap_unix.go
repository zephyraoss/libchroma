//go:build !windows

package mmap

import (
	"fmt"
	"os"
	"syscall"
)

func MapFile(f *os.File) (*Data, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat for mmap: %w", err)
	}
	size := fi.Size()
	if size == 0 {
		return &Data{Bytes: nil}, nil
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}
	return &Data{Bytes: data}, nil
}

func Unmap(m *Data) error {
	if m.Bytes == nil {
		return nil
	}
	bytes := m.Bytes
	m.Bytes = nil
	return syscall.Munmap(bytes)
}

func (m *Data) Advise(off, length int64) {
	if m.Bytes == nil || length <= 0 || off < 0 || off >= int64(len(m.Bytes)) {
		return
	}
	const pageMask = int64(4095)
	start := off &^ pageMask
	end := off + length
	if end > int64(len(m.Bytes)) {
		end = int64(len(m.Bytes))
	}
	_ = syscall.Madvise(m.Bytes[start:end], syscall.MADV_WILLNEED)
}
