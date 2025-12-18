package transferio

import (
	"context"
	"io"
)

// Storage defines a common storage interface implemented by all backends.
type Storage interface {
	io.Closer
	// Size returns the storage size; if unknown, returns 0 and false.
	Size() (int64, bool)
}



// ReadAtStorage defines random-access read storage.
type ReadAtStorage interface {
	Storage
	io.ReaderAt
}

// WriteAtStorage defines random-access write storage.
type WriteAtStorage interface {
	Storage
	io.WriterAt
	// Preallocate storage space if supported.
	Preallocate(ctx context.Context, size int64) error
}

// StreamRead defines a streaming read interface.
type StreamRead interface {
	Storage
	io.ReadCloser
}

// StreamWrite defines a streaming write interface.
type StreamWrite interface {
	Storage
	io.WriteCloser
	// Preallocate storage space if supported.
	Preallocate(ctx context.Context, size int64) error
}
