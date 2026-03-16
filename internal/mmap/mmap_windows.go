//go:build windows

package mmap

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
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

	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READONLY, uint32(size>>32), uint32(size), nil)
	if err != nil {
		return nil, fmt.Errorf("CreateFileMapping: %w", err)
	}
	defer windows.CloseHandle(h)

	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		return nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), size)
	return &Data{Bytes: data}, nil
}

func Unmap(m *Data) error {
	if m.Bytes == nil {
		return nil
	}
	return windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&m.Bytes[0])))
}
