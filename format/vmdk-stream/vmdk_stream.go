package format

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
)

type VMDKStream struct {
	Header         SparseExtentHeader
	GrainNum       uint64
	GrainDirectory []uint32
	GrainTabel     []uint32
	Writer         io.Writer
	WriteSize      SectorType
	Reader         io.Reader
	ReadSize       SectorType
}

func NewVMDKStreamReader(r io.Reader) *VMDKStream {
	var vs VMDKStream

	vs.Reader = r

	return &vs
}

func NewVMDKStreamWriter(w io.Writer) *VMDKStream {
	var vs VMDKStream

	vs.Writer = w

	return &vs
}

func isAllZeroByte(data []byte) bool {
	zeroSlice := make([]byte, len(data))
	return bytes.Equal(data, zeroSlice)
}

func isZeroGrainTable(gt []uint32) bool {
	for i := 0; i < len(gt); i++ {
		if gt[i] != 0 {
			return false
		}
	}

	return true
}

func alignToSectorSize(size uint64) uint64 {
	mask := uint64(SECTOR_SIZE - 1)
	return (size + mask) >> SECTOR_SIZE_SHIFT << SECTOR_SIZE_SHIFT
}

func alignBufToSectorSize(buf *bytes.Buffer) error {
	alignedBufSize := int(alignToSectorSize(uint64(buf.Len())))
	curBufSize := buf.Len()

	for i := curBufSize; i < alignedBufSize; i++ {
		err := buf.WriteByte(byte(0))
		if err != nil {
			return err
		}
	}

	return nil
}

func (vs *VMDKStream) Create(fileName string, capability uint64) error {
	var hdr SparseExtentHeader
	var descriptorBuf bytes.Buffer
	capability = alignToSectorSize(capability)

	hdr.MagicNumber = VMDKMagic
	hdr.Version = SPARSE_VERSION_INCOMPAT_FLAGS
	hdr.Flags = SPARSEFLAG_VALID_NEWLINE_DETECTOR | SPARSEFLAG_COMPRESSED | SPARSEFLAG_EMBEDDED_LBA
	hdr.Capacity = SectorType(capability / SECTOR_SIZE)
	hdr.GrainSize = 128 // 64k
	hdr.DescriptorOffset = 1

	descriptor := makeDiskDescriptorFile(fileName, uint64(hdr.Capacity), generateCID())
	hdr.DescriptorSize = SectorType(alignToSectorSize(uint64(len(descriptor))) >> SECTOR_SIZE_SHIFT)

	hdr.NumGTEsPerGT = 512
	hdr.RgdOffset = 0
	hdr.GdOffset = SPARSE_GD_AT_END
	hdr.Overhead = 1 + hdr.DescriptorSize // Header size + Descriptor size
	hdr.UncleanShutdown = false
	hdr.SingleEndLineChar = SPARSE_SINGLE_END_LINE_CHAR
	hdr.NonEndLineChar = SPARSE_NON_END_LINE_CHAR
	hdr.DoubleEndLineChar1 = SPARSE_DOUBLE_END_LINE_CHAR1
	hdr.DoubleEndLineChar2 = SPARSE_DOUBLE_END_LINE_CHAR2
	hdr.CompressAlgorithm = COMPRESSION_DEFLATE

	vs.Header = hdr

	err := binary.Write(vs.Writer, binary.LittleEndian, hdr)
	if err != nil {
		return err
	}
	vs.WriteSize += 1

	_, err = descriptorBuf.WriteString(descriptor)
	if err != nil {
		return err
	}

	err = alignBufToSectorSize(&descriptorBuf)
	if err != nil {
		return err
	}

	_, err = vs.Writer.Write(descriptorBuf.Bytes())
	if err != nil {
		return err
	}

	vs.WriteSize += hdr.DescriptorSize

	return nil
}

func (vs *VMDKStream) writeGrainTable() (SectorType, error) {
	var gt SpecialMarker
	var buf bytes.Buffer

	if isZeroGrainTable(vs.GrainTabel) {
		vs.GrainTabel = []uint32{}
		return 0, nil
	}

	gt.Val = SectorType(vs.Header.NumGTEsPerGT * 4 >> SECTOR_SIZE_SHIFT) // 4 is sizeOf uint32
	gt.Size = 0
	gt.Type = MARKER_GT

	err := binary.Write(vs.Writer, binary.LittleEndian, gt)
	if err != nil {
		return 0, err
	}

	vs.WriteSize += 1
	grainTableSector := vs.WriteSize

	for i := len(vs.GrainTabel); i < int(vs.Header.NumGTEsPerGT); i++ {
		vs.GrainTabel = append(vs.GrainTabel, 0)
	}

	err = binary.Write(&buf, binary.LittleEndian, vs.GrainTabel)
	if err != nil {
		return 0, err
	}

	err = alignBufToSectorSize(&buf)
	if err != nil {
		return 0, err
	}

	n, err := vs.Writer.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}

	vs.WriteSize += SectorType(n >> SECTOR_SIZE_SHIFT)
	vs.GrainTabel = []uint32{}
	return grainTableSector, nil
}

func (vs *VMDKStream) writeGrainDirectory() (SectorType, error) {
	var gd SpecialMarker
	var buf bytes.Buffer

	gd.Val = vs.Header.GetGrainDirectorySectorSize()
	gd.Size = 0
	gd.Type = MARKER_GD

	err := binary.Write(vs.Writer, binary.LittleEndian, gd)
	if err != nil {
		return 0, err
	}

	vs.WriteSize += 1
	grainDirectorySector := vs.WriteSize

	err = binary.Write(&buf, binary.LittleEndian, vs.GrainDirectory)
	if err != nil {
		return 0, err
	}

	err = alignBufToSectorSize(&buf)
	if err != nil {
		return 0, err
	}

	n, err := vs.Writer.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}

	vs.WriteSize += SectorType(n >> SECTOR_SIZE_SHIFT)
	vs.GrainDirectory = []uint32{}
	return grainDirectorySector, nil
}

func (vs *VMDKStream) Write(p []byte) (int, error) {
	// assert len(p) == GrainSize<<SECTOR_SIZE_SHIFT
	var buf bytes.Buffer
	var compressBuf bytes.Buffer
	var gm GrainMarker

	if isAllZeroByte(p) {
		vs.GrainTabel = append(vs.GrainTabel, 0)
		vs.GrainNum += 1
		if len(vs.GrainTabel) == int(vs.Header.NumGTEsPerGT) {
			grainTableSector, err := vs.writeGrainTable()
			if err != nil {
				return len(p), err
			}
			vs.GrainDirectory = append(vs.GrainDirectory, uint32(grainTableSector))
		}
		return len(p), nil
	}

	zw := zlib.NewWriter(&compressBuf)
	zw.Write(p)
	zw.Close()

	gm.Lba = SectorType(vs.GrainNum * uint64(vs.Header.GrainSize))
	gm.Size = uint32(compressBuf.Len())

	err := binary.Write(&buf, binary.LittleEndian, gm)
	if err != nil {
		return 0, err
	}

	_, err = buf.Write(compressBuf.Bytes())
	if err != nil {
		return 0, err
	}

	err = alignBufToSectorSize(&buf)
	if err != nil {
		return 0, err
	}

	n, err := vs.Writer.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	vs.GrainTabel = append(vs.GrainTabel, uint32(vs.WriteSize))
	vs.WriteSize += SectorType(n >> SECTOR_SIZE_SHIFT)

	vs.GrainNum += 1
	if len(vs.GrainTabel) == int(vs.Header.NumGTEsPerGT) {
		grainTableSector, err := vs.writeGrainTable()
		if err != nil {
			return len(p), err
		}
		vs.GrainDirectory = append(vs.GrainDirectory, uint32(grainTableSector))
	}
	return len(p), nil
}

func (vs *VMDKStream) writeFooter() error {
	var fm SpecialMarker

	fm.Val = 1
	fm.Size = 0
	fm.Type = MARKER_FOOTER

	err := binary.Write(vs.Writer, binary.LittleEndian, fm)
	if err != nil {
		return err
	}
	vs.WriteSize += 1

	err = binary.Write(vs.Writer, binary.LittleEndian, vs.Header)
	if err != nil {
		return err
	}
	vs.WriteSize += 1

	return nil
}

func (vs *VMDKStream) writeEOS() error {
	var eos SpecialMarker

	eos.Val = 0
	eos.Size = 0
	eos.Type = MARKER_EOS

	err := binary.Write(vs.Writer, binary.LittleEndian, eos)
	if err != nil {
		return err
	}

	vs.WriteSize += 1
	return nil
}

func (vs *VMDKStream) Close() error {
	if len(vs.GrainTabel) != 0 {
		grainTableSector, err := vs.writeGrainTable()
		if err != nil {
			return err
		}

		vs.GrainDirectory = append(vs.GrainDirectory, uint32(grainTableSector))
	}

	grainDirectorySector, err := vs.writeGrainDirectory()
	if err != nil {
		return err
	}

	vs.Header.GdOffset = grainDirectorySector
	err = vs.writeFooter()
	if err != nil {
		return err
	}

	return vs.writeEOS()
}

func (vs *VMDKStream) isHeader(b []byte) (bool, error) {
	bufReader := bytes.NewReader(b)
	err := binary.Read(bufReader, binary.LittleEndian, &vs.Header)
	if err != nil {
		return false, err
	}

	if vs.Header.MagicNumber != VMDKMagic {
		return false, nil
	}

	unnecessaryBuf := make([]byte, vs.Header.Overhead<<SECTOR_SIZE_SHIFT-512)
	if _, err := io.ReadFull(vs.Reader, unnecessaryBuf); err != nil {
		return true, err
	}

	return true, nil
}

func (vs *VMDKStream) IsHeaderForAPI(b []byte) (bool, error) {
    bufReader := bytes.NewReader(b)
    if err := binary.Read(bufReader, binary.LittleEndian, &vs.Header); err != nil {
        return false, err
    }
    if vs.Header.MagicNumber != VMDKMagic {
        return false, nil
    }
    return true, nil
}

func (vs *VMDKStream) isSpecialMarker(b []byte) (bool, error) {
	bufReader := bytes.NewReader(b)
	marker := SpecialMarker{}

	err := binary.Read(bufReader, binary.LittleEndian, &marker)
	if err != nil {
		return false, err
	}

	if marker.Size != 0 {
		return false, nil
	}

	if marker.Type == MARKER_FOOTER || marker.Type == MARKER_GD || marker.Type == MARKER_GT {
		unnecessaryBuf := make([]byte, marker.Val<<SECTOR_SIZE_SHIFT)
		if _, err := io.ReadFull(vs.Reader, unnecessaryBuf); err != nil {
			return true, err
		}
		return true, nil
	}

	return false, nil
}

func (vs *VMDKStream) isEOSMarker(b []byte) (bool, error) {
	bufReader := bytes.NewReader(b)
	marker := SpecialMarker{}

	err := binary.Read(bufReader, binary.LittleEndian, &marker)
	if err != nil {
		return false, err
	}

	return marker.Size == 0 && marker.Type == MARKER_EOS, nil
}

func (vs *VMDKStream) parseGrainMarker(b []byte) (*GrainMarker, error) {
	bufReader := bytes.NewReader(b)
	marker := GrainMarker{}

	err := binary.Read(bufReader, binary.LittleEndian, &marker)
	if err != nil {
		return nil, err
	}

	return &marker, nil
}

func (vs *VMDKStream) Read(p []byte) (uint64, int, error) {
	buf := make([]byte, 512)
	var n int

	for {
		if _, err := io.ReadFull(vs.Reader, buf); err != nil {
			return 0, 0, err
		}

		y, err := vs.isHeader(buf)
		if err != nil {
			return 0, 0, err
		}
		if y {
			continue
		}

		y, err = vs.isSpecialMarker(buf)
		if err != nil {
			return 0, 0, err
		}
		if y {
			continue
		}

		y, _ = vs.isEOSMarker(buf)
		if y {
			return 0, 0, io.EOF
		}

		marker, _ := vs.parseGrainMarker(buf)
		// buf size is 512 bytes, GrainMarker size is 12, so data size is 500.
		if marker.Size > 500 {
			// Get additional data.
			if marker.Size > 128*1024 { // 128k
				return 0, 0, errors.New("invaild grain marker size")
			}
			eBuf := make([]byte, alignToSectorSize(uint64(marker.Size-500)))

			if _, err := io.ReadFull(vs.Reader, eBuf); err != nil {
				return 0, 0, err
			}

			buf = append(buf, eBuf...)
		}

		// Compressed data starts from the 12th byte.
		br := bytes.NewReader(buf[12 : marker.Size+12])

		z, err := zlib.NewReader(br)
		if err != nil {
			return 0, 0, err
		}

		// zlib reader only supports reading up to 32k data.
		// Here need to read zlib reader in a loop.
		bw := &bytes.Buffer{}
		_, err = io.Copy(bw, z)
		z.Close()
		if err != nil {
			return 0, 0, err
		}

		n = copy(p, bw.Bytes())

		return uint64(marker.Lba) << SECTOR_SIZE_SHIFT, n, err
	}
}
