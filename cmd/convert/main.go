package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"disk-stream-convert/pkg/converter"
	"disk-stream-convert/pkg/diskfmt"
	"disk-stream-convert/pkg/diskfmt/qcow2"
	"disk-stream-convert/pkg/diskfmt/raw"
	"disk-stream-convert/pkg/diskfmt/vmdk"
	"disk-stream-convert/pkg/transferio"
)

func main() {
	src := flag.String("src", "", "Source file path or URL")
	dst := flag.String("dst", "", "Destination file path")
	srcFmt := flag.String("src-fmt", "", "Source format (vmdk, raw)")
	dstFmt := flag.String("dst-fmt", "raw", "Destination format (raw)")
	prealloc := flag.Bool("prealloc", false, "Preallocate destination file")

	flag.Parse()

	if *src == "" || *dst == "" {
		fmt.Println("Error: -src and -dst are required")
		flag.Usage()
		os.Exit(1)
	}

	if *srcFmt == "" {
		fmt.Println("Error: -src-fmt is required")
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	var source transferio.StreamRead
	if strings.HasPrefix(*src, "http://") || strings.HasPrefix(*src, "https://") {
		source = transferio.NewHTTPImport(*src)
	} else {
		var err error
		source, err = transferio.NewFileReadStorage(*src)
		if err != nil {
			fmt.Printf("Error opening source file: %v\n", err)
			os.Exit(1)
		}
	}

	sink, err := transferio.NewFileWriteStorage(*dst, false)
	if err != nil {
		fmt.Printf("Error opening destination file: %v\n", err)
		os.Exit(1)
	}
	defer sink.Close()

	var reader diskfmt.StreamReader
	switch *srcFmt {
	case "raw":
		reader = raw.NewReader(source)
	case "vmdk":
		reader = vmdk.NewReader(source)
	case "qcow2":
		reader = qcow2.NewReader(source)
	default:
		fmt.Println("Error: unsupported source format:", *srcFmt)
		os.Exit(1)
	}

	var writer diskfmt.StreamWriter
	switch *dstFmt {
	case "raw":
		writer = raw.NewWriter(sink, *prealloc)
	case "vmdk":
		writer = vmdk.NewWriter(sink)
	default:
		fmt.Println("Error: unsupported source format:", *srcFmt)
		os.Exit(1)
	}

	c := &converter.StreamConverter{
		Reader: reader,
		Writer: writer,
	}

	fmt.Printf("Starting conversion from %s (%s) to %s (%s)...\n", *src, *srcFmt, *dst, *dstFmt)
	start := time.Now()

	written, capacity, err := c.Run(ctx)
	if err != nil {
		fmt.Printf("Conversion failed: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	fmt.Printf("Conversion successful!\n")
	fmt.Printf("Written: %d bytes\n", written)
	fmt.Printf("Capacity: %d bytes\n", capacity)
	fmt.Printf("Elapsed: %v\n", elapsed)
}
