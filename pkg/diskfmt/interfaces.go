package diskfmt

import "io"

type StreamFormatReader interface {
	InitStream(r io.Reader) error
	CapacityBytes() uint64
	BlockBytes() int
	Next(p []byte) (uint64, int, error)
}

type RandomFormatReader interface {
	InitRandom(r io.ReaderAt) error
	CapacityBytes() uint64
	BlockBytes() int
	Next(p []byte) (uint64, int, error)
}

type FormatWriter interface {
	Init(w io.Writer, fileName string, capacityBytes uint64) error
	WriteBlock(p []byte) (int, error)
	Close() error
}
