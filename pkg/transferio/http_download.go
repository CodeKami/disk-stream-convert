package transferio

import (
	"context"
	"errors"
	"io"
)

type HTTPDownload struct {
	W      io.Writer
	offset int64
}

func NewHTTPDownload(w io.Writer) *HTTPDownload {
	return &HTTPDownload{
		W: w,
	}
}

func (s *HTTPDownload) Write(p []byte) (int, error) {
	return s.WriteAt(p, s.offset)
}

func (s *HTTPDownload) WriteAt(p []byte, off int64) (int, error) {
	if off != s.offset {
		return 0, errors.New("http download does not support random write")
	}

	n, err := s.W.Write(p)
	if err != nil {
		return n, err
	}

	s.offset += int64(n)
	return n, nil
}

func (s *HTTPDownload) Preallocate(ctx context.Context, size int64) error {
	return nil
}

func (s *HTTPDownload) Size() (int64, bool) {
	return s.offset, false
}

func (s *HTTPDownload) Close() error {
	if c, ok := s.W.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

var _ StreamWrite = (*HTTPDownload)(nil)
