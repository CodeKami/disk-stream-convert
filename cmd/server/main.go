package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	vmdkstream "disk-stream-convert/format/vmdk-stream"
	"disk-stream-convert/pkg/transferio"
)

type importRequest struct {
	URL      string `json:"url"`
	Prealloc bool   `json:"prealloc"`
}

type importResponse struct {
	Output         string `json:"output"`
	WrittenBytes   uint64 `json:"writtenBytes"`
	CapacityBytes  uint64 `json:"capacityBytes"`
	ElapsedSeconds int64  `json:"elapsedSeconds"`
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
	}

	if req.URL == "" {
		writeErr(w, http.StatusBadRequest, errors.New("missing url"))
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
	source := &transferio.HTTPSource{URL: req.URL}
	written, capacity, err := importVMDK(ctx, source, sink, req.Prealloc)
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

func importVMDK(ctx context.Context, source transferio.DataSource, sink transferio.StorageSink, prealloc bool) (written uint64, capacityBytes uint64, err error) {
	rc, err := source.Open(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer rc.Close()

	vs := vmdkstream.NewVMDKStreamReader(rc)

	hdr := make([]byte, 512)
	if _, err := io.ReadFull(rc, hdr); err != nil {
		return 0, 0, err
	}
	y, err := vs.IsHeaderForAPI(hdr)
	if err != nil {
		return 0, 0, err
	}
	if !y {
		return 0, 0, errors.New("invalid vmdk stream header")
	}

	unnecessary := make([]byte, vs.Header.Overhead<<vmdkstream.SECTOR_SIZE_SHIFT-512)
	if _, err := io.ReadFull(rc, unnecessary); err != nil {
		return 0, 0, err
	}

	capacityBytes = uint64(vs.Header.Capacity) << vmdkstream.SECTOR_SIZE_SHIFT
	grainBytes := uint64(vs.Header.GrainSize) << vmdkstream.SECTOR_SIZE_SHIFT

	if prealloc {
		if err := sink.Preallocate(ctx, int64(capacityBytes)); err != nil {
			return 0, 0, err
		}
	}

	buf := make([]byte, int(grainBytes))
	for {
		lba, n, err := vs.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return written, capacityBytes, err
		}
		if n > 0 {
			if _, err := sink.WriteAt(ctx, buf[:n], int64(lba)); err != nil {
				return written, capacityBytes, err
			}
			written += uint64(n)
		}
	}

	return written, capacityBytes, nil
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

	http.HandleFunc("/import-vmdk", importHandler)
	srv := &http.Server{
		Addr:              ":8080",
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
