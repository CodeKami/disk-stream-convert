package format

import (
	"fmt"
	"math/rand"
)

const (
	// Magic bytes for VMDK file format.
	VMDKMagic = 0x564D444B // 'V' 'M' 'D' 'K'
)

const (
	SECTOR_SIZE_SHIFT = 9
	SECTOR_SIZE       = 1 << SECTOR_SIZE_SHIFT
)

const (
	SPARSE_VERSION_INCOMPAT_FLAGS     uint32     = 3
	SPARSEFLAG_VALID_NEWLINE_DETECTOR uint32     = 1 << 0
	SPARSEFLAG_COMPRESSED             uint32     = 1 << 16
	SPARSEFLAG_EMBEDDED_LBA           uint32     = 1 << 17
	SPARSE_GD_AT_END                  SectorType = 0xFFFFFFFFFFFFFFFF
	SPARSE_SINGLE_END_LINE_CHAR                  = '\n'
	SPARSE_NON_END_LINE_CHAR                     = ' '
	SPARSE_DOUBLE_END_LINE_CHAR1                 = '\r'
	SPARSE_DOUBLE_END_LINE_CHAR2                 = '\n'
)

// Marker Constants
const (
	MARKER_EOS    uint32 = 0
	MARKER_GT     uint32 = 1
	MARKER_GD     uint32 = 2
	MARKER_FOOTER uint32 = 3
)

// Compress Algorithm Constants
const (
	COMPRESSION_NONE    uint16 = 0
	COMPRESSION_DEFLATE uint16 = 1
)

type SectorType uint64

type SparseExtentHeader struct {
	MagicNumber        uint32
	Version            uint32
	Flags              uint32
	Capacity           SectorType
	GrainSize          SectorType
	DescriptorOffset   SectorType
	DescriptorSize     SectorType
	NumGTEsPerGT       uint32
	RgdOffset          SectorType
	GdOffset           SectorType
	Overhead           SectorType
	UncleanShutdown    bool
	SingleEndLineChar  byte
	NonEndLineChar     byte
	DoubleEndLineChar1 byte
	DoubleEndLineChar2 byte
	CompressAlgorithm  uint16
	Pad                [433]byte
}

func (hdr *SparseExtentHeader) GetGrainDirectorySectorSize() SectorType {
	var totalGrain uint64
	var totalGrainTables uint32

	if hdr.Capacity%hdr.GrainSize != 0 {
		totalGrain = uint64(hdr.Capacity/hdr.GrainSize) + 1
	} else {
		totalGrain = uint64(hdr.Capacity / hdr.GrainSize)
	}

	if totalGrain%uint64(hdr.NumGTEsPerGT) != 0 {
		totalGrainTables = uint32(totalGrain/uint64(hdr.NumGTEsPerGT)) + 1
	} else {
		totalGrainTables = uint32(totalGrain / uint64(hdr.NumGTEsPerGT))
	}

	return SectorType(alignToSectorSize(uint64(totalGrainTables)*4) >> SECTOR_SIZE_SHIFT) // 4 is size of uint32
}

type GrainMarker struct {
	Lba  SectorType
	Size uint32
}

type SpecialMarker struct {
	Val  SectorType
	Size uint32
	Type uint32
	Pad  [496]byte
}

const diskDescriptorFileTemplate = `# Disk DescriptorFile
version=1
encoding="UTF-8"
CID=%08x
parentCID=ffffffff
createType="streamOptimized"

# Extent description
RW %d SPARSE "%s"

# The Disk Data Base
#DDB

ddb.longContentID = "%08x%08x%08x%08x"
ddb.virtualHWVersion = "6" # This field is obsolete, used by ESX3.x and older only. Compatible with compat6.
ddb.geometry.cylinders = "%d"
ddb.geometry.heads = "255" # 255/63 is good for anything bigger than 4GB.
ddb.geometry.sectors = "63"
ddb.adapterType = "lsilogic"
ddb.toolsInstallType = "4" # unmanaged (open-vm-tools)
ddb.toolsVersion = "2147483647" # default is 2^31-1 (unknown)`

func generateCID() uint32 {
	var cid uint32

	for {
		cid = rand.Uint32()

		// Do not accept 0xFFFFFFFF and 0xFFFFFFFE.
		// They may be interpreted by some software as no parent or disk full of zeroes.
		if cid != 0xFFFFFFFF && cid != 0xFFFFFFFE {
			break
		}
	}

	return cid
}

func makeDiskDescriptorFile(fileName string, capacity uint64, cid uint32) string {
	var cylinders uint32

	if capacity > 65535*255*63 {
		cylinders = 65535
	} else {
		cylinders = uint32((capacity + 255*63 - 1) / (255 * 63))
	}

	ret := fmt.Sprintf(
		diskDescriptorFileTemplate,
		cid,
		capacity,
		fileName,
		rand.Uint32(),
		rand.Uint32(),
		rand.Uint32(),
		cid,
		cylinders,
	)

	return ret
}
