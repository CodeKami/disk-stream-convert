package diskfmt

import "io"

// RawFormatWriter 实现了 FormatWriter 接口，但实际上 RAW 格式的写出通常由 FileSink 直接处理。
// 如果需要对接转换逻辑（例如 VMDK -> RAW 流），此 Writer 可作为一个简单的 passthrough。
type RawFormatWriter struct {
	writer io.Writer
}

func (w *RawFormatWriter) Init(writer io.Writer, fileName string, capacityBytes uint64) error {
	w.writer = writer
	// RAW 格式无需写头，亦无需预分配（预分配通常在 Sink 层做）
	return nil
}

func (w *RawFormatWriter) WriteBlock(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *RawFormatWriter) Close() error {
	// 无需写尾部
	return nil
}
