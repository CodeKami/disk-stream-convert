package main

import (
	"context"
	"errors"
	"io"

	"disk-stream-convert/pkg/diskfmt"
	"disk-stream-convert/pkg/transferio"
)

// StreamConverter 封装通用的转换逻辑
type StreamConverter struct {
	Source   transferio.DataSource
	Sink     transferio.StorageSink
	Prealloc bool
	SrcFmt   string
	DstFmt   string
}

// Run 执行转换过程
// 核心逻辑：
// 1. 打开源流
// 2. 初始化 FormatReader（解析源格式）
// 3. (可选) 执行预分配
// 4. 循环读取 Block 并写入 Sink
func (sc *StreamConverter) Run(ctx context.Context) (written uint64, capacity uint64, err error) {
	rc, err := sc.Source.Open(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer rc.Close()

	var reader diskfmt.StreamFormatReader

	switch sc.SrcFmt {
	case "vmdk":
		reader = &diskfmt.VMDKStreamReader{}
	case "raw":
		// RAW 需要特殊处理：如果 Source 能提供大小，则注入给 Reader
		rawReader := &diskfmt.RawStreamReader{}
		if size, ok := sc.Source.Size(); ok {
			rawReader.SetCapacity(uint64(size))
		}
		reader = rawReader
	default:
		return 0, 0, errors.New("unsupported source format: " + sc.SrcFmt)
	}

	if err := reader.InitStream(rc); err != nil {
		return 0, 0, err
	}

	capacity = reader.CapacityBytes()
	blockBytes := reader.BlockBytes()
	if blockBytes <= 0 {
		blockBytes = 1 << 20 // 默认 1MB
	}

	// 目标目前只支持 RAW（即直接写入 Sink），所以这里直接对接 Sink
	// 如果未来支持 dst=vmdk，则需要引入 FormatWriter 并对接 Sink 的流式写入接口
	if sc.DstFmt != "raw" {
		return 0, 0, errors.New("unsupported destination format: " + sc.DstFmt)
	}

	// 预分配逻辑
	if sc.Prealloc && capacity > 0 {
		if err := sc.Sink.Preallocate(ctx, int64(capacity)); err != nil {
			return 0, 0, err
		}
	}

	buf := make([]byte, blockBytes)
	for {
		offset, n, err := reader.Next(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			// 部分读取的情况通常由 Next 内部处理返回 n>0 和 nil error，或者 n>0 和 EOF
			// 如果 Next 返回了 error 且不是 EOF，那就是真错误
			if n > 0 {
				// 尝试写入已读部分
				if _, wErr := sc.Sink.WriteAt(ctx, buf[:n], int64(offset)); wErr != nil {
					return written, capacity, wErr
				}
				written += uint64(n)
			}
			return written, capacity, err
		}

		if n > 0 {
			if _, wErr := sc.Sink.WriteAt(ctx, buf[:n], int64(offset)); wErr != nil {
				return written, capacity, wErr
			}
			written += uint64(n)
		}
	}

	// 如果是 RAW 且 capacity 未知，则最终写入量即为容量
	if capacity == 0 && sc.SrcFmt == "raw" {
		capacity = written
	}

	return written, capacity, nil
}
