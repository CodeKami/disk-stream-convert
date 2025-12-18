package main

import (
	"bytes"
	"context"
	"disk-stream-convert/pkg/converter"
	"disk-stream-convert/pkg/diskfmt/raw"
	"disk-stream-convert/pkg/diskfmt/vmdk"
	"disk-stream-convert/pkg/transferio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func createVMDKFromRaw(t *testing.T, dir string, name string, data []byte) string {
	out := filepath.Join(dir, name)
	sink, err := transferio.NewFileWriteStorage(out, false)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer sink.Close()
	src := transferio.NewHTTPUpload(io.NopCloser(bytes.NewReader(data)), int64(len(data)))
	reader := raw.NewReader(src)
	writer := vmdk.NewWriter(sink)
	c := &converter.StreamConverter{Reader: reader, Writer: writer}
	if _, _, err := c.Run(context.Background()); err != nil {
		t.Fatalf("convert: %v", err)
	}
	return out
}

func TestUploadRawToRaw(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	data := bytes.Repeat([]byte{0x11, 0x22, 0x33, 0x44}, 1024*512)

	req := httptest.NewRequest(http.MethodPost, "/upload?src=raw&dst=raw&name=test.img", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))
	rr := httptest.NewRecorder()
	uploadHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.WrittenBytes != uint64(len(data)) {
		t.Fatalf("written=%d want=%d", resp.WrittenBytes, len(data))
	}
	if resp.CapacityBytes != uint64(len(data)) {
		t.Fatalf("capacity=%d want=%d", resp.CapacityBytes, len(data))
	}
	out := resp.Output
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(b, data) {
		t.Fatalf("output content mismatch")
	}
}

func TestImportRawToRaw(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	data := bytes.Repeat([]byte{0xaa, 0xbb, 0xcc, 0xdd}, 1024*256)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer ts.Close()

	req := httptest.NewRequest(http.MethodGet, "/import?url="+ts.URL+"/file.bin&src=raw&dst=raw", nil)
	rr := httptest.NewRecorder()
	importHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.WrittenBytes != uint64(len(data)) {
		t.Fatalf("written=%d want=%d", resp.WrittenBytes, len(data))
	}
	if resp.CapacityBytes != uint64(len(data)) {
		t.Fatalf("capacity=%d want=%d", resp.CapacityBytes, len(data))
	}
	b, err := os.ReadFile(resp.Output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(b, data) {
		t.Fatalf("output content mismatch")
	}
}

func TestExportRawToRaw(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	data := bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 1024*64)
	srcPath := filepath.Join(dir, "src.img")
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/export?src=raw&dst=raw&path="+srcPath, nil)
	rr := httptest.NewRecorder()
	exportHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("content-type=%s", ct)
	}
	if cd := rr.Header().Get("Content-Disposition"); cd != "attachment; filename=\"src.img\"" {
		t.Fatalf("content-disposition=%s", cd)
	}
	if !bytes.Equal(rr.Body.Bytes(), data) {
		t.Fatalf("exported body mismatch")
	}
}

func TestExportRawToVMDKHeaders(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	data := bytes.Repeat([]byte{0xff}, 1024*8)
	srcPath := filepath.Join(dir, "disk.raw")
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/export?src=raw&dst=vmdk&path="+srcPath, nil)
	rr := httptest.NewRecorder()
	exportHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	if cd := rr.Header().Get("Content-Disposition"); cd != "attachment; filename=\"disk.vmdk\"" {
		t.Fatalf("content-disposition=%s", cd)
	}
	if rr.Body.Len() == 0 {
		t.Fatalf("empty vmdk body")
	}
}

func TestUploadRawToVMDK(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	data := bytes.Repeat([]byte{0x55}, 1024*128)
	req := httptest.NewRequest(http.MethodPost, "/upload?src=raw&dst=vmdk&name=raw2vmdk.vmdk", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))
	rr := httptest.NewRecorder()
	uploadHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.CapacityBytes != uint64(len(data)) {
		t.Fatalf("capacity=%d want=%d", resp.CapacityBytes, len(data))
	}
	b, err := os.ReadFile(resp.Output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Contains(b, []byte("createType=\"streamOptimized\"")) {
		t.Fatalf("vmdk descriptor not found")
	}
}

func TestUploadVMDKToRaw(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	orig := bytes.Repeat([]byte{0x10, 0x20, 0x30, 0x40}, 1024*64)
	vmdkPath := createVMDKFromRaw(t, dir, "src.vmdk", orig)
	vmdkBytes, err := os.ReadFile(vmdkPath)
	if err != nil {
		t.Fatalf("read vmdk: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload?src=vmdk&dst=raw&name=out.img", bytes.NewReader(vmdkBytes))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(vmdkBytes))
	rr := httptest.NewRecorder()
	uploadHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	out, err := os.ReadFile(resp.Output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(out, orig) {
		t.Fatalf("decoded raw mismatch")
	}
}

func TestImportRawToVMDK(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	data := bytes.Repeat([]byte{0x77}, 1024*64)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer ts.Close()

	req := httptest.NewRequest(http.MethodGet, "/import?url="+ts.URL+"/raw.img&src=raw&dst=vmdk", nil)
	rr := httptest.NewRecorder()
	importHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.CapacityBytes != uint64(len(data)) {
		t.Fatalf("capacity=%d want=%d", resp.CapacityBytes, len(data))
	}
	b, err := os.ReadFile(resp.Output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Contains(b, []byte("createType=\"streamOptimized\"")) {
		t.Fatalf("vmdk descriptor not found")
	}
}

func TestImportVMDKToRaw(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	orig := bytes.Repeat([]byte{0x99}, 1024*32)
	vmdkPath := createVMDKFromRaw(t, dir, "import.vmdk", orig)
	vmdkBytes, err := os.ReadFile(vmdkPath)
	if err != nil {
		t.Fatalf("read vmdk: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(vmdkBytes)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(vmdkBytes)
	}))
	defer ts.Close()

	req := httptest.NewRequest(http.MethodGet, "/import?url="+ts.URL+"/disk.vmdk&src=vmdk&dst=raw", nil)
	rr := httptest.NewRecorder()
	importHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	out, err := os.ReadFile(resp.Output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(out, orig) {
		t.Fatalf("decoded raw mismatch")
	}
}

func TestExportVMDKToRaw(t *testing.T) {
	dir := t.TempDir()
	serverOutputDir = dir

	orig := bytes.Repeat([]byte{0x12, 0x34}, 1024*32)
	vmdkPath := createVMDKFromRaw(t, dir, "exp.vmdk", orig)

	req := httptest.NewRequest(http.MethodGet, "/export?src=vmdk&dst=raw&path="+vmdkPath, nil)
	rr := httptest.NewRecorder()
	exportHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("content-type=%s", ct)
	}
	if cd := rr.Header().Get("Content-Disposition"); cd != "attachment; filename=\"exp.vmdk\"" {
		t.Fatalf("content-disposition=%s", cd)
	}
	if !bytes.Equal(rr.Body.Bytes(), orig) {
		t.Fatalf("exported body mismatch")
	}
}
