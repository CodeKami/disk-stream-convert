package converter

import (
	"context"
	"io"

	"disk-stream-convert/pkg/diskfmt"
)

// StreamConverter encapsulates common conversion logic.
type StreamConverter struct {
	Reader   diskfmt.StreamReader
	Writer   diskfmt.StreamWriter
}

// Run executes the conversion process.
func (sc *StreamConverter) Run(ctx context.Context) (written uint64, capacity uint64, err error) {
	if err := sc.Reader.Open(ctx); err != nil {
		return 0, 0, err
	}
	defer sc.Reader.Close()

	capacity = uint64(sc.Reader.Capacity())

	if err := sc.Writer.Open(ctx, int64(capacity)); err != nil {
		return 0, 0, err
	}
	defer sc.Writer.Close()

	blockBytes := 1 << 20
	buf := make([]byte, blockBytes)
	zero := make([]byte, 1<<16)
	var writeCursor uint64

	for {
		n, off, err := sc.Reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			if n > 0 {
				if _, wErr := sc.Writer.Write(buf[:n]); wErr != nil {
					return written, capacity, wErr
				}
				written += uint64(n)
			}
			return written, capacity, err
		}

		if n > 0 {
			if uint64(off) > writeCursor {
				gap := uint64(off) - writeCursor
				for gap > 0 {
					chunk := zero
					if gap < uint64(len(zero)) {
						chunk = zero[:gap]
					}
					if _, wErr := sc.Writer.Write(chunk); wErr != nil {
						return written, capacity, wErr
					}
					written += uint64(len(chunk))
					writeCursor += uint64(len(chunk))
					gap -= uint64(len(chunk))
				}
			}
			if _, wErr := sc.Writer.Write(buf[:n]); wErr != nil {
				return written, capacity, wErr
			}
			written += uint64(n)
			writeCursor += uint64(n)
		}
	}

	if writeCursor < capacity {
		remaining := capacity - writeCursor
		for remaining > 0 {
			chunk := zero
			if remaining < uint64(len(zero)) {
				chunk = zero[:remaining]
			}
			if _, wErr := sc.Writer.Write(chunk); wErr != nil {
				return written, capacity, wErr
			}
			written += uint64(len(chunk))
			writeCursor += uint64(len(chunk))
			remaining -= uint64(len(chunk))
		}
	}

	return written, capacity, nil
}
