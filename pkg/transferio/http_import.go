package transferio

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
)

type HTTPImport struct {
	URL           string
	contentLength int64
	body          io.ReadCloser
}

func NewHTTPImport(url string) *HTTPImport {
	return &HTTPImport{
		URL: url,
	}
}

func (s *HTTPImport) Open(ctx context.Context) (io.ReadCloser, error) {
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
	s.body = resp.Body
	return resp.Body, nil
}

func (s *HTTPImport) Size() (int64, bool) {
	if s.contentLength > 0 {
		return s.contentLength, true
	}
	return 0, false
}

func (s *HTTPImport) Read(p []byte) (int, error) {
	if s.body == nil {
		return 0, io.EOF
	}
	return s.body.Read(p)
}

func (s *HTTPImport) Close() error {
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}

var _ StreamRead = (*HTTPImport)(nil)
