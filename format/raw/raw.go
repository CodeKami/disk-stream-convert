package format

import "io"

type RawStream struct {
	Reader io.Reader
	Offset int64
}

func NewRawStream(r io.Reader) *RawStream {
	return &RawStream{Reader: r}
}

func (rs *RawStream) Read(p []byte) (int64, int, error) {
	n, err := rs.Reader.Read(p)
	off := rs.Offset
	rs.Offset += int64(n)
	return off, n, err
}

