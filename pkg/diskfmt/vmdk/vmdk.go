package vmdk

import (
	"context"
	vmdkstream "disk-stream-convert/format/vmdk-stream"
	"disk-stream-convert/pkg/transferio"
	"io"
)

type Reader struct {
	Source transferio.StreamRead
	vs     *vmdkstream.VMDKStream
	rc     io.ReadCloser
}

func NewReader(source transferio.StreamRead) *Reader {
	return &Reader{Source: source}
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
		r.rc = rc
		r.vs = vmdkstream.NewVMDKStreamReader(rc)
	} else {
		if rc, ok := r.Source.(io.ReadCloser); ok {
			r.rc = rc
			r.vs = vmdkstream.NewVMDKStreamReader(rc)
		} else {
			return io.ErrUnexpectedEOF
		}
	}
	return r.vs.InitStream()
}

func (r *Reader) Read(p []byte) (int, int64, error) {
	// vmdkstream.Next returns offset, n, error
	off, n, err := r.vs.Next(p)
	if err != nil {
		return n, int64(off), err
	}
	return n, int64(off), nil
}

func (r *Reader) Capacity() int64 {
	return int64(r.vs.CapacityBytes())
}

func (r *Reader) Close() error {
	if r.rc != nil {
		return r.rc.Close()
	}
	return nil
}

type Writer struct {
	Sink transferio.WriteAtStorage
	vs   *vmdkstream.VMDKStream
	adapter *sinkWriterAdapter
}

func NewWriter(sink transferio.WriteAtStorage) *Writer {
	return &Writer{Sink: sink}
}

func (w *Writer) Open(ctx context.Context, capacity int64) error {
	w.adapter = &sinkWriterAdapter{ctx: ctx, sink: w.Sink}
	w.vs = vmdkstream.NewVMDKStreamWriter(w.adapter)
	// VMDK needs a filename in header, we can default it or pass it.
	// For now default to "disk.img".
	return w.vs.Create("disk.img", uint64(capacity))
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.vs.Write(p)
}

func (w *Writer) Close() error {
	if err := w.vs.Close(); err != nil {
		return err
	}
	return w.Sink.Close()
}

type sinkWriterAdapter struct {
	ctx  context.Context
	sink transferio.WriteAtStorage
	off  int64
}

func (s *sinkWriterAdapter) Write(p []byte) (n int, err error) {
	n, err = s.sink.WriteAt(p, s.off)
	s.off += int64(n)
	return
}
