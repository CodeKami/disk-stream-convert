package format

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"testing"
)

func TestQcow2ReadAt(t *testing.T) {
	// 1. Construct a minimal QCOW2 image in memory
	clusterBits := uint32(9)
	clusterSize := uint64(1 << clusterBits) // 512

	// Layout:
	// Header: 0 - 104 (approx)
	// L1 Table: 512 (Cluster 1)
	// L2 Table: 1024 (Cluster 2)
	// Data Cluster (Normal): 1536 (Cluster 3)
	// Data Cluster (Compressed): 2048 (Cluster 4)

	imgSize := uint64(clusterSize * 3) // 3 clusters: 0 (Unallocated), 1 (Normal), 2 (Compressed)

	header := Header{
		Magic:             Magic,
		Version:           Version3,
		ClusterBits:       clusterBits,
		Size:              imgSize,
		L1Size:            1, // 1 entry in L1 table
		L1TableOffset:     clusterSize, // L1 table at offset 512
		HeaderLength:      104, // Basic header size
		RefcountTableClusters: 0,
		RefcountTableOffset: 0,
		RefcountOrder:     4,
	}

	buf := new(bytes.Buffer)
	
	// Write Header
	if err := binary.Write(buf, binary.BigEndian, header); err != nil {
		t.Fatal(err)
	}
	// Pad header to cluster size
	pad(buf, int(clusterSize) - buf.Len())

	// Write L1 Table (at 512)
	// Entry 0 points to L2 Table at 1024
	l1Entry := NewL1TableEntry(int64(clusterSize * 2)) // 1024
	if err := binary.Write(buf, binary.BigEndian, l1Entry); err != nil {
		t.Fatal(err)
	}
	pad(buf, int(clusterSize * 2) - buf.Len())

	// Write L2 Table (at 1024)
	l2Entries := make([]uint64, clusterSize/8)
	
	// Entry 0 is 0 (Unallocated)
	
	// Entry 1: Normal Cluster at 1536
	normalEntry := NewL2TableEntry(nil, int64(clusterSize*3), false, 0)
	l2Entries[1] = uint64(normalEntry)
	
	// Entry 2: Compressed Cluster at 2048
	// Prepare compressed data first to know size
	rawData := make([]byte, clusterSize)
	for i := range rawData {
		rawData[i] = 0xAA // Pattern
	}
	var compressedBuf bytes.Buffer
	w, _ := flate.NewWriter(&compressedBuf, flate.BestCompression)
	w.Write(rawData)
	w.Close()
	compressedBytes := compressedBuf.Bytes()
	
	// Mock Header for helper functions (Offset/CompressedSize)
	mockHdr := &HeaderAndAdditionalFields{Header: header}
	
	compressedEntry := NewL2TableEntry(mockHdr, int64(clusterSize*4), true, int64(len(compressedBytes)))
	l2Entries[2] = uint64(compressedEntry)
	
	if err := binary.Write(buf, binary.BigEndian, l2Entries); err != nil {
		t.Fatal(err)
	}
	pad(buf, int(clusterSize * 3) - buf.Len())

	// Write Data Cluster (Normal) at 1536
	normalData := make([]byte, clusterSize)
	for i := range normalData {
		normalData[i] = 0xFF
	}
	buf.Write(normalData)
	
	// Write Data Cluster (Compressed) at 2048
	buf.Write(compressedBytes)
	pad(buf, int(clusterSize * 5) - buf.Len()) // Padding to end

	// Create Qcow2Format
	r := bytes.NewReader(buf.Bytes())
	q, err := NewQcow2Format(r)
	if err != nil {
		t.Fatalf("NewQcow2Format failed: %v", err)
	}

	// Test 1: Read Unallocated (Cluster 0)
	out := make([]byte, clusterSize)
	n, err := q.ReadAt(out, 0)
	if err != nil {
		t.Errorf("ReadAt unallocated failed: %v", err)
	}
	if n != int(clusterSize) {
		t.Errorf("ReadAt unallocated n=%d, want %d", n, clusterSize)
	}
	for i, b := range out {
		if b != 0 {
			t.Errorf("ReadAt unallocated byte %d = %x, want 0", i, b)
			break
		}
	}

	// Test 2: Read Normal (Cluster 1, Offset 512)
	n, err = q.ReadAt(out, int64(clusterSize))
	if err != nil {
		t.Errorf("ReadAt normal failed: %v", err)
	}
	for i, b := range out {
		if b != 0xFF {
			t.Errorf("ReadAt normal byte %d = %x, want FF", i, b)
			break
		}
	}

	// Test 3: Read Compressed (Cluster 2, Offset 1024)
	n, err = q.ReadAt(out, int64(clusterSize*2))
	if err != nil {
		t.Errorf("ReadAt compressed failed: %v", err)
	}
	for i, b := range out {
		if b != 0xAA {
			t.Errorf("ReadAt compressed byte %d = %x, want AA", i, b)
			break
		}
	}
}

func pad(buf *bytes.Buffer, n int) {
	if n > 0 {
		buf.Write(make([]byte, n))
	}
}
