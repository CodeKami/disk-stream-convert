package diskfmt

import (
	"context"
)

type StreamReader interface {
	Open(ctx context.Context) error
	Read(p []byte) (n int, offset int64, err error)
	Capacity() int64
	Close() error
}

type StreamWriter interface {
	Open(ctx context.Context, capacity int64) error
	Write(p []byte) (n int, err error)
	Close() error
}
