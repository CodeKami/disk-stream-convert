package transferio

import (
	"context"
	"os"
)

// FileReadStorage provides read access to a local file.
type FileReadStorage struct {
	path string
	file *os.File
	size int64
}

// NewFileReadStorage opens a file for reading and returns a storage instance.
func NewFileReadStorage(path string) (*FileReadStorage, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &FileReadStorage{
		path: path,
		file: file,
		size: fi.Size(),
	}, nil
}

func (s *FileReadStorage) Read(p []byte) (int, error) {
	return s.file.Read(p)
}

func (s *FileReadStorage) ReadAt(p []byte, off int64) (int, error) {
	return s.file.ReadAt(p, off)
}

func (s *FileReadStorage) Size() (int64, bool) {
	return s.size, true
}

func (s *FileReadStorage) Close() error {
	return s.file.Close()
}

// FileWriteStorage provides write access to a local file.
type FileWriteStorage struct {
	path string
	file *os.File
}

// NewFileWriteStorage opens a file for writing and returns a storage instance.
func NewFileWriteStorage(path string, append bool) (*FileWriteStorage, error) {
	flag := os.O_WRONLY | os.O_CREATE
	if append {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	file, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	return &FileWriteStorage{
		path: path,
		file: file,
	}, nil
}

func (s *FileWriteStorage) Write(p []byte) (int, error) {
	return s.file.Write(p)
}

// WriteAt writes data at the given offset (io.WriterAt).
func (s *FileWriteStorage) WriteAt(p []byte, off int64) (int, error) {
	return s.file.WriteAt(p, off)
}

// Preallocate resizes the file to the specified size.
func (s *FileWriteStorage) Preallocate(ctx context.Context, size int64) error {
	return s.file.Truncate(size)
}

func (s *FileWriteStorage) Size() (int64, bool) {
	fi, err := s.file.Stat()
	if err != nil {
		return 0, false
	}
	return fi.Size(), true
}

func (s *FileWriteStorage) Close() error {
	return s.file.Close()
}
