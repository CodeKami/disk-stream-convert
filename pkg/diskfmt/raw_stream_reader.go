package diskfmt

import (
	"io"
)

// RawStreamReader 实现了 StreamFormatReader 接口，用于读取 RAW 格式流
type RawStreamReader struct {
	reader        io.Reader
	offset        uint64
	capacityBytes uint64 // 若无法提前知晓则可能为0
	blockBytes    int
}

// SetCapacity 允许外部设置已知的大小（例如从 Content-Length 获取）
func (r *RawStreamReader) SetCapacity(cap uint64) {
	r.capacityBytes = cap
}

func (r *RawStreamReader) InitStream(rc io.Reader) error {
	r.reader = rc
	r.offset = 0
	r.blockBytes = 1 << 20 // 默认 1MB 块大小
	// RAW 流本身无头信息，Init 即完成
	return nil
}

func (r *RawStreamReader) CapacityBytes() uint64 {
	return r.capacityBytes
}

func (r *RawStreamReader) BlockBytes() int {
	return r.blockBytes
}

func (r *RawStreamReader) Next(p []byte) (uint64, int, error) {
	n, err := io.ReadFull(r.reader, p)
	if err != nil {
		if err == io.EOF {
			return r.offset, 0, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			// 读到了部分数据但也结束了
			if n > 0 {
				currentOffset := r.offset
				r.offset += uint64(n)
				return currentOffset, n, nil
			}
			return r.offset, 0, io.EOF
		}
		return r.offset, 0, err
	}
	currentOffset := r.offset
	r.offset += uint64(n)
	return currentOffset, n, nil
}
