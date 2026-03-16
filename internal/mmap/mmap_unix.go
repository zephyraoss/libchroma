//go:build !windows

package mmap

import (
	"fmt"
	"os"
	"syscall"
)

// MapFile memory-maps the entire file read-only.
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

// Unmap unmaps the memory region.
func Unmap(m *Data) error {
	if m.Bytes == nil {
		return nil
	}
	return syscall.Munmap(m.Bytes)
}
