package transferio

import (
	"context"
	"os"
)

type StorageSink interface {
	WriteAt(ctx context.Context, p []byte, off int64) (int, error)
	Preallocate(ctx context.Context, size int64) error
	Close() error
}

type FileSink struct {
	File *os.File
}

func (s *FileSink) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	return s.File.WriteAt(p, off)
}

func (s *FileSink) Preallocate(ctx context.Context, size int64) error {
	return s.File.Truncate(size)
}

func (s *FileSink) Close() error {
	return s.File.Close()
}
