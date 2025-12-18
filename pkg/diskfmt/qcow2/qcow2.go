package qcow2

import (
	"context"
	qcow2fmt "disk-stream-convert/format/qcow2"
	"disk-stream-convert/pkg/transferio"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	Source  transferio.StreamRead
	q       *qcow2fmt.Qcow2Format
	rc      io.ReadCloser
	tmpFile *os.File
	offset  int64
}

func NewReader(source transferio.StreamRead) *Reader {
	return &Reader{Source: source}
}

func (r *Reader) Open(ctx context.Context) error {
	var inputReader qcow2fmt.ReadAndReadAt

	// Check if Source supports ReadAt (e.g. local file)
	if ra, ok := r.Source.(io.ReaderAt); ok {
		// If it's also a ReadCloser (which StreamRead is), we can use it directly.
		if rc, ok := r.Source.(io.ReadCloser); ok {
			inputReader = &readAtWrapper{Reader: rc, ReaderAt: ra}
			r.rc = rc
		} else {
			// Should not happen for StreamRead
			return fmt.Errorf("source implements ReaderAt but not Reader")
		}
	} else {
		// Source does not support ReadAt. Buffer to temp file.
		tmp, err := os.CreateTemp("", "dsc-qcow2-import-*")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		r.tmpFile = tmp

		type openable interface {
			Open(ctx context.Context) (io.ReadCloser, error)
		}

		var src io.ReadCloser
		if o, ok := r.Source.(openable); ok {
			s, err := o.Open(ctx)
			if err != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return err
			}
			src = s
		} else {
			src = r.Source
		}

		// Copy to temp file
		if _, err := io.Copy(tmp, src); err != nil {
			src.Close()
			tmp.Close()
			os.Remove(tmp.Name())
			return fmt.Errorf("failed to buffer qcow2 to temp file: %w", err)
		}
		// We don't close src here strictly if it wasn't opened by us,
		// but since we consumed it, it's safer to leave it to the caller or Close().
		// However, for temp file logic, we are done with src.
		// If src IS r.Source, we might want to close it now to free resources,
		// as we are switching to tmpFile.
		// But let's be careful not to double close if r.Close() calls Source.Close().
		// Actually r.rc will be set to tmp.

		// Let's not close src unless we opened it via openable.
		if _, ok := r.Source.(openable); ok {
			src.Close()
		}

		// Rewind temp file
		if _, err := tmp.Seek(0, 0); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}

		r.rc = tmp
		inputReader = tmp // os.File implements Read and ReadAt
	}

	q, err := qcow2fmt.NewQcow2Format(inputReader)
	if err != nil {
		return err
	}
	r.q = q
	return nil
}

func (r *Reader) Read(p []byte) (int, int64, error) {
	n, err := r.q.ReadAt(p, r.offset)
	readOffset := r.offset
	if n > 0 {
		r.offset += int64(n)
	}
	return n, readOffset, err
}

func (r *Reader) Capacity() int64 {
	if r.q == nil {
		return 0
	}
	s, _ := r.q.Size()
	return int64(s)
}

func (r *Reader) Close() error {
	var err error
	if r.tmpFile != nil {
		r.tmpFile.Close()
		os.Remove(r.tmpFile.Name())
		r.tmpFile = nil
	}
	if r.rc != nil {
		if e := r.rc.Close(); e != nil {
			err = e
		}
	}
	// Also close Source if it wasn't r.rc (e.g. if we buffered it)
	// But Source interface says it's a Closer.
	// If we buffered, r.rc is tmpFile. Source is left alone.
	// We should probably close Source too.
	if r.Source != nil && r.Source != r.rc {
		if e := r.Source.Close(); e != nil {
			// Keep original error if any
			if err == nil {
				err = e
			}
		}
	}
	return err
}

type readAtWrapper struct {
	io.Reader
	io.ReaderAt
}
