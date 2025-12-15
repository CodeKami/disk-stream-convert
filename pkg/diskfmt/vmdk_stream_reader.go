package diskfmt

import (
	"errors"
	"io"

	vmdkstream "disk-stream-convert/format/vmdk-stream"
)

type VMDKStreamReader struct {
	vs            *vmdkstream.VMDKStream
	capacityBytes uint64
	blockBytes    int
}

func (r *VMDKStreamReader) InitStream(rc io.Reader) error {
	r.vs = vmdkstream.NewVMDKStreamReader(rc)

	hdr := make([]byte, 512)
	if _, err := io.ReadFull(rc, hdr); err != nil {
		return err
	}
	y, err := r.vs.IsHeaderForAPI(hdr)
	if err != nil {
		return err
	}
	if !y {
		return errors.New("invalid vmdk stream header")
	}

	unnecessary := make([]byte, r.vs.Header.Overhead<<vmdkstream.SECTOR_SIZE_SHIFT-512)
	if _, err := io.ReadFull(rc, unnecessary); err != nil {
		return err
	}

	r.capacityBytes = uint64(r.vs.Header.Capacity) << vmdkstream.SECTOR_SIZE_SHIFT
	r.blockBytes = int(uint64(r.vs.Header.GrainSize) << vmdkstream.SECTOR_SIZE_SHIFT)
	return nil
}

func (r *VMDKStreamReader) CapacityBytes() uint64 {
	return r.capacityBytes
}

func (r *VMDKStreamReader) BlockBytes() int {
	return r.blockBytes
}

func (r *VMDKStreamReader) Next(p []byte) (uint64, int, error) {
	return r.vs.Read(p)
}
