package transferio

import (
	"context"
	"io"
)

type HTTPUpload struct {
	R         io.ReadCloser
	KnownSize int64
}

func NewHTTPUpload(r io.ReadCloser, knownSize int64) *HTTPUpload {
	return &HTTPUpload{
		R:         r,
		KnownSize: knownSize,
	}
}

func (s *HTTPUpload) Open(ctx context.Context) (io.ReadCloser, error) {
	return s.R, nil
}

func (s *HTTPUpload) Size() (int64, bool) {
	if s.KnownSize > 0 {
		return s.KnownSize, true
	}
	return 0, false
}

func (s *HTTPUpload) Read(p []byte) (int, error) {
	return s.R.Read(p)
}

func (s *HTTPUpload) Close() error {
	return s.R.Close()
}

var _ StreamRead = (*HTTPUpload)(nil)
