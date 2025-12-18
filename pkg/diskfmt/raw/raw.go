package raw

import (
	"context"
	"io"

	"disk-stream-convert/pkg/transferio"
)

type Reader struct {
	Source   transferio.StreamRead
	reader   io.ReadCloser
	offset   int64
	capacity int64
}

func NewReader(source transferio.StreamRead) *Reader {
	return &Reader{
		Source: source,
	}
}

func (r *Reader) Open(ctx context.Context) error {
	// If Source has Open(ctx) method, call it; otherwise assume it's already an io.ReadCloser.
	type openable interface {
		Open(ctx context.Context) (io.ReadCloser, error)
	}
	if o, ok := r.Source.(openable); ok {
		rc, err := o.Open(ctx)
		if err != nil {
			return err
		}
		r.reader = rc
	} else {
		// Source should itself implement io.ReadCloser
		if rc, ok := r.Source.(io.ReadCloser); ok {
			r.reader = rc
		} else {
			return io.ErrUnexpectedEOF
		}
	}
	if size, ok := r.Source.Size(); ok {
		r.capacity = size
	}
	r.offset = 0
	return nil
}

func (r *Reader) Read(p []byte) (int, int64, error) {
	n, err := io.ReadFull(r.reader, p)
	if err != nil {
		if err == io.EOF {
			return n, r.offset, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			// got partial data
			off := r.offset
			r.offset += int64(n)
			return n, off, nil
		}
		return n, r.offset, err
	}
	off := r.offset
	r.offset += int64(n)
	return n, off, nil
}

func (r *Reader) Capacity() int64 {
	return r.capacity
}

func (r *Reader) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}

type Writer struct {
	Sink     transferio.WriteAtStorage
	Prealloc bool
	offset   int64
	ctx      context.Context
}

func NewWriter(sink transferio.WriteAtStorage, prealloc bool) *Writer {
	return &Writer{Sink: sink, Prealloc: prealloc}
}

func (w *Writer) Open(ctx context.Context, capacity int64) error {
	w.offset = 0
	w.ctx = ctx
	if w.Prealloc {
		return w.Sink.Preallocate(ctx, capacity)
	}
	return nil
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.Sink.WriteAt(p, w.offset)
	if err != nil {
		return n, err
	}
	w.offset += int64(n)
	return n, nil
}

func (w *Writer) Close() error {
	return w.Sink.Close()
}
