package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"disk-stream-convert/pkg/transferio"
)

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
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	sink := &transferio.FileSink{File: f}
	defer sink.Close()

	ctx := r.Context()
	start := time.Now()

	converter := &StreamConverter{
		Source: &transferio.UploadSource{
			R:         rc,
			KnownSize: knownSize,
		},
		Sink:     sink,
		Prealloc: prealloc,
		SrcFmt:   src,
		DstFmt:   dst,
	}

	written, capacity, err := converter.Run(ctx)
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

	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	sink := &transferio.FileSink{File: f}
	defer sink.Close()

	ctx := r.Context()
	start := time.Now()

	converter := &StreamConverter{
		Source:   &transferio.HTTPSource{URL: req.URL},
		Sink:     sink,
		Prealloc: req.Prealloc,
		SrcFmt:   req.Src,
		DstFmt:   req.Dst,
	}

	written, capacity, err := converter.Run(ctx)
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
	srv := &http.Server{
		Addr:              ":8080",
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
