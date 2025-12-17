package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"disk-stream-convert/pkg/converter"
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

	var source transferio.DataSource
	if strings.HasPrefix(*src, "http://") || strings.HasPrefix(*src, "https://") {
		source = &transferio.HTTPSource{URL: *src}
	} else {
		source = &transferio.FileSource{Path: *src}
	}

	f, err := os.OpenFile(*dst, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Printf("Error opening destination file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	sink := &transferio.FileSink{File: f}

	c := &converter.StreamConverter{
		Source:   source,
		Sink:     sink,
		Prealloc: *prealloc,
		SrcFmt:   *srcFmt,
		DstFmt:   *dstFmt,
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
