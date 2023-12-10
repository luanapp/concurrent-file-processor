package main

import (
	"fmt"
	"time"

	processor "github.com/luanapp/parallel-json-processor"
)

const (
	userJson    = "testdata.json"
	libraryJson = "ol_cdump.json"
)

func main() {
	start := time.Now()

	stream := processor.NewJsonStream[processor.RawLibraryData, processor.LibData]()
	users := stream.Process(libraryJson, processor.ParseLibraryData)
	fmt.Printf("%d users processed\n", len(users))
	fmt.Printf("took %v\n", time.Since(start))
}
