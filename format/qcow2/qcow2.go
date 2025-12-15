package format

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/goburrow/cache"
)

const (
	// Each table is going to be around a single cluster in size.
	// So this will store up to 64MB of tables in memory.
	maxCachedTables = 1000
)

type Qcow2Format struct {
	header      *HeaderAndAdditionalFields
	table       cache.LoadingCache
	reader      ReadAndReadAt
	clusterSize int64
}

func NewQcow2Format(r ReadAndReadAt) (*Qcow2Format, error) {
	hdr, err := readHeader(r)
	if err != nil {
		return nil, err
	}

	q := &Qcow2Format{
		header:      hdr,
		reader:      r,
		clusterSize: int64(1 << hdr.ClusterBits),
	}

	q.table = cache.NewLoadingCache(q.tableLoader, cache.WithMaximumSize(maxCachedTables))

	return q, nil
}

func readHeader(r io.Reader) (*HeaderAndAdditionalFields, error) {
	var hdr Header
	if err := binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return nil, fmt.Errorf("failed to read image header: %w", err)
	}

	if hdr.Magic != Magic {
		return nil, fmt.Errorf("invalid magic bytes")
	}

	if hdr.Version != Version3 {
		return nil, fmt.Errorf("only version 3 is supported")
	}

	if hdr.BackingFileOffset != 0 {
		return nil, fmt.Errorf("backing files are not supported")
	}

	if hdr.CryptMethod != NoEncryption {
		return nil, fmt.Errorf("encryption is not supported")
	}

	if hdr.IncompatibleFeatures != 0 {
		return nil, fmt.Errorf("incompatible features are not supported")
	}

	var additionalFields *HeaderAdditionalFields
	if hdr.HeaderLength > uint32(unsafe.Sizeof(hdr)) {
		additionalFields = &HeaderAdditionalFields{}
		if err := binary.Read(r, binary.BigEndian, additionalFields); err != nil {
			return nil, fmt.Errorf("failed to read additional header fields: %w", err)
		}
	}

	if additionalFields != nil && additionalFields.CompressionType != CompressionTypeDeflate {
		return nil, fmt.Errorf("unsupported compression type")
	}

	var extensions []HeaderExtension
	for {
		var headerExtension HeaderExtension
		if err := binary.Read(r, binary.BigEndian, &headerExtension.HeaderExtensionMetadata); err != nil {
			return nil, fmt.Errorf("failed to read header extension type and length: %w", err)
		}

		if headerExtension.Type == EndOfHeaderExtensionArea {
			break
		}

		if headerExtension.Type == BackingFileFormatName ||
			headerExtension.Type == ExternalDataFileName ||
			headerExtension.Type == FullDiskEncryptionHeader {
			return nil, fmt.Errorf("unsupported header extension")
		}

		headerExtension.Data = make([]byte, headerExtension.Length)
		if _, err := io.ReadFull(r, headerExtension.Data); err != nil {
			return nil, fmt.Errorf("failed to read header extension data: %w", err)
		}

		extensions = append(extensions, headerExtension)
	}

	return &HeaderAndAdditionalFields{
		Header:           hdr,
		AdditionalFields: additionalFields,
		Extensions:       extensions,
	}, nil
}

type tableKey struct {
	imageOffset int64
	n           int
}

func (q *Qcow2Format) tableLoader(key cache.Key) (cache.Value, error) {
	imageOffset := key.(tableKey).imageOffset
	n := key.(tableKey).n

	buf := make([]byte, 8*n)
	if _, err := q.reader.ReadAt(buf, imageOffset); err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	t := make([]uint64, n)
	for i := range t {
		t[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	return t, nil
}

func (q *Qcow2Format) readTable(imageOffset int64, n int) ([]uint64, error) {
	t, err := q.table.Get(tableKey{imageOffset: imageOffset, n: n})
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	return t.([]uint64), nil
}

func (q *Qcow2Format) getL2Entry(offset int64) (L2TableEntry, error) {
	var l2entry L2TableEntry
	l2Entries := q.clusterSize / 8
	l2Index := (offset / q.clusterSize) % l2Entries
	l1Index := (offset / q.clusterSize) / l2Entries

	if l1Index >= int64(q.header.L1Size) {
		return NewUnallocatedL2Entry(), nil
	}

	l1Table, err := q.readTable(int64(q.header.L1TableOffset), int(q.header.L1Size))
	if err != nil {
		return l2entry, err
	}

	l1Entry := L1TableEntry(l1Table[l1Index])
	if !l1Entry.Used() {
		return NewUnallocatedL2Entry(), nil
	}

	l2TableOffset := l1Entry.Offset()
	if l2TableOffset <= 0 {
		return NewUnallocatedL2Entry(), nil
	}

	l2Table, err := q.readTable(l2TableOffset, int(l2Entries))
	if err != nil {
		return l2entry, err
	}

	l2Entry := L2TableEntry(l2Table[l2Index])

	return l2Entry, nil
}

func (q *Qcow2Format) Size() (uint64, error) {
	offset := int64(q.header.Size) - q.clusterSize

	for offset >= 0 {
		l2Entry, err := q.getL2Entry(offset)
		if err != nil {
			return 0, err
		}

		if l2Entry.Used() {
			return uint64(l2Entry.Offset(q.header) + q.clusterSize), nil
		} else if l2Entry.Compressed() {
			return uint64(l2Entry.Offset(q.header) + l2Entry.CompressedSize(q.header)), nil
		}

		offset -= q.clusterSize
	}

	return q.header.L1TableOffset + uint64(8*q.header.L1Size), nil
}
