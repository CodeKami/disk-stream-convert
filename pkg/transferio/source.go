package transferio

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
)

type DataSource interface {
	Open(ctx context.Context) (io.ReadCloser, error)
}

type HTTPSource struct {
	URL string
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
	return resp.Body, nil
}

type UploadSource struct {
	R io.ReadCloser
}

func (s *UploadSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return s.R, nil
}
