package transferio

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

// DataSource 定义数据源接口
// Size() 可选返回数据流总大小（若已知），用于预分配
type DataSource interface {
	Open(ctx context.Context) (io.ReadCloser, error)
	Size() (int64, bool)
}

type HTTPSource struct {
	URL           string
	contentLength int64
}

func (s *HTTPSource) Open(ctx context.Context) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, errors.New(string(body))
	}
	s.contentLength = resp.ContentLength
	return resp.Body, nil
}

func (s *HTTPSource) Size() (int64, bool) {
	if s.contentLength > 0 {
		return s.contentLength, true
	}
	return 0, false
}

type UploadSource struct {
	R          io.ReadCloser
	KnownSize  int64
}

func (s *UploadSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return s.R, nil
}

func (s *UploadSource) Size() (int64, bool) {
	if s.KnownSize > 0 {
		return s.KnownSize, true
	}
	return 0, false
}

type FileSource struct {
	Path string
	size int64
}

func (s *FileSource) Open(ctx context.Context) (io.ReadCloser, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s.size = fi.Size()
	return f, nil
}

func (s *FileSource) Size() (int64, bool) {
	if s.size > 0 {
		return s.size, true
	}
	// Attempt to stat if not opened yet
	fi, err := os.Stat(s.Path)
	if err == nil {
		s.size = fi.Size()
		return s.size, true
	}
	return 0, false
}
