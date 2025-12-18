package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"disk-stream-convert/pkg/converter"
	"disk-stream-convert/pkg/diskfmt"
	"disk-stream-convert/pkg/diskfmt/raw"
	"disk-stream-convert/pkg/diskfmt/vmdk"
	"disk-stream-convert/pkg/transferio"
)

func getReader(srcFmt string, source transferio.StreamRead) (diskfmt.StreamReader, error) {
	switch srcFmt {
	case "raw":
		return raw.NewReader(source), nil
	case "vmdk":
		return vmdk.NewReader(source), nil
	default:
		return nil, errors.New("unsupported source format: " + srcFmt)
	}
}

func getWriter(dstFmt string, sink transferio.WriteAtStorage, prealloc bool) (diskfmt.StreamWriter, error) {
	switch dstFmt {
	case "raw":
		return raw.NewWriter(sink, prealloc), nil
	case "vmdk":
		return vmdk.NewWriter(sink), nil
	default:
		return nil, errors.New("unsupported destination format: " + dstFmt)
	}
}

type importRequest struct {
	URL      string `json:"url"`
	Prealloc bool   `json:"prealloc"`
	Src      string `json:"src"`
	Dst      string `json:"dst"`
}

type importResponse struct {
	Output         string `json:"output"`
	WrittenBytes   uint64 `json:"writtenBytes"`
	CapacityBytes  uint64 `json:"capacityBytes"`
	ElapsedSeconds int64  `json:"elapsedSeconds"`
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	prealloc := r.URL.Query().Get("prealloc") == "true"
	src := r.URL.Query().Get("src")
	dst := r.URL.Query().Get("dst")
	if src == "" || dst == "" {
		writeErr(w, http.StatusBadRequest, errors.New("missing src or dst"))
		return
	}
	outDir := serverOutputDir
	if outDir == "" {
		writeErr(w, http.StatusInternalServerError, errors.New("server misconfigured: output dir empty"))
		return
	}
	var (
		rc        io.ReadCloser
		name      string
		knownSize int64
	)
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		file, fh, err := r.FormFile("file")
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		rc = file
		name = fh.Filename
		knownSize = fh.Size
	} else {
		rc = r.Body
		name = r.URL.Query().Get("name")
		if name == "" {
			name = "upload.img"
		}
		knownSize = r.ContentLength
	}
	outPath := filepath.Join(outDir, path.Base(name))
	sink, err := transferio.NewFileWriteStorage(outPath, false)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer sink.Close()

	ctx := r.Context()
	start := time.Now()

	dataSource := transferio.NewHTTPUpload(rc, knownSize)

	reader, err := getReader(src, dataSource)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	writer, err := getWriter(dst, sink, prealloc)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	c := &converter.StreamConverter{
		Reader: reader,
		Writer: writer,
	}

	written, capacity, err := c.Run(ctx)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}

	resp := importResponse{
		Output:         outPath,
		WrittenBytes:   written,
		CapacityBytes:  capacity,
		ElapsedSeconds: int64(time.Since(start).Seconds()),
	}
	json.NewEncoder(w).Encode(resp)
}

func importHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req importRequest
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
	} else {
		req.URL = r.URL.Query().Get("url")
		if r.URL.Query().Get("prealloc") == "true" {
			req.Prealloc = true
		}
		req.Src = r.URL.Query().Get("src")
		req.Dst = r.URL.Query().Get("dst")
	}

	if req.URL == "" {
		writeErr(w, http.StatusBadRequest, errors.New("missing url"))
		return
	}
	if req.Src == "" || req.Dst == "" {
		writeErr(w, http.StatusBadRequest, errors.New("missing src or dst"))
		return
	}

	outDir := serverOutputDir
	if outDir == "" {
		writeErr(w, http.StatusInternalServerError, errors.New("server misconfigured: output dir empty"))
		return
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	outPath, err := deriveOutputPath(outDir, req.URL)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	sink, err := transferio.NewFileWriteStorage(outPath, false)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer sink.Close()

	ctx := r.Context()
	start := time.Now()

	source := transferio.NewHTTPImport(req.URL)
	reader, err := getReader(req.Src, source)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	writer, err := getWriter(req.Dst, sink, req.Prealloc)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	c := &converter.StreamConverter{
		Reader: reader,
		Writer: writer,
	}

	written, capacity, err := c.Run(ctx)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}

	json.NewEncoder(w).Encode(importResponse{
		Output:         outPath,
		WrittenBytes:   written,
		CapacityBytes:  capacity,
		ElapsedSeconds: int64(time.Since(start).Seconds()),
	})
}

func exportHandler(w http.ResponseWriter, r *http.Request) {
	src := r.URL.Query().Get("src")
	dst := r.URL.Query().Get("dst")
	filePath := r.URL.Query().Get("path")

	if src == "" || dst == "" || filePath == "" {
		writeErr(w, http.StatusBadRequest, errors.New("missing src, dst or path"))
		return
	}

	if _, err := os.Stat(filePath); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}

	filename := filepath.Base(filePath)
	if dst == "vmdk" {
		ext := filepath.Ext(filename)
		if ext != "" {
			filename = strings.TrimSuffix(filename, ext) + ".vmdk"
		} else {
			filename += ".vmdk"
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

	source, err := transferio.NewFileReadStorage(filePath)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	reader, err := getReader(src, source)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	sink := &transferio.HTTPDownload{W: w}
	writer, err := getWriter(dst, sink, false)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	c := &converter.StreamConverter{
		Reader: reader,
		Writer: writer,
	}

	if _, _, err := c.Run(r.Context()); err != nil {
		// Log error to stdout since we can't change HTTP status effectively after streaming starts
		// In a real app, we might use a trailer or log it.
		// fmt.Println("Export failed:", err)
	}
}

var serverOutputDir string

func deriveOutputPath(baseDir string, src string) (string, error) {
	u, err := url.Parse(src)
	if err != nil {
		return "", err
	}
	name := path.Base(u.Path)
	if name == "" || name == "/" || name == "." {
		name = "import.img"
	}
	return filepath.Join(baseDir, name), nil
}

func writeErr(w http.ResponseWriter, code int, err error) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": err.Error(),
	})
}

func main() {
	outDir := flag.String("outdir", "/tmp/disk-streams", "output directory for local files")
	flag.Parse()
	serverOutputDir = *outDir

	http.HandleFunc("/import", importHandler)
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/export", exportHandler)
	srv := &http.Server{
		Addr:              ":8080",
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
