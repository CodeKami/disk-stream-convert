package diskfmt

import (
	"io"

	vmdkstream "disk-stream-convert/format/vmdk-stream"
)

type VMDKStreamWriter struct {
	vs *vmdkstream.VMDKStream
}

func (w *VMDKStreamWriter) Init(writer io.Writer, fileName string, capacityBytes uint64) error {
	w.vs = vmdkstream.NewVMDKStreamWriter(writer)
	return w.vs.Create(fileName, capacityBytes)
}

func (w *VMDKStreamWriter) WriteBlock(p []byte) (int, error) {
	return w.vs.Write(p)
}

func (w *VMDKStreamWriter) Close() error {
	return w.vs.Close()
}
