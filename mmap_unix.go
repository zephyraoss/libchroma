//go:build !windows

package chroma

import (
	"fmt"
	"os"
	"syscall"
)

// mmapFile memory-maps the entire file read-only.
func mmapFile(f *os.File) (*mmapData, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat for mmap: %w", err)
	}
	size := fi.Size()
	if size == 0 {
		return &mmapData{data: nil}, nil
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}
	return &mmapData{data: data}, nil
}

// munmapData unmaps the memory region.
func munmapData(m *mmapData) error {
	if m.data == nil {
		return nil
	}
	return syscall.Munmap(m.data)
}
