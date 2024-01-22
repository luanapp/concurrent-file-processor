package main

import (
	"fmt"
	"time"

	processor "github.com/luanapp/parallel-file-processor"
)

const (
	userJson    = "testdata.json"
	libraryJson = "ol_cdump.json"
)

func main() {
	start := time.Now()

	stream := processor.NewProcessor[processor.RawLibraryData, processor.LibData](processor.Json)
	users := stream.Process(libraryJson, processor.ParseLibraryData)
	fmt.Printf("%d users processed\n", len(users))
	fmt.Printf("took %v\n", time.Since(start))
}
