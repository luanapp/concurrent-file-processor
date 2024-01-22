package processor

import (
	"encoding/csv"
	"io"
	"os"
)

type (
	csvStream[T, U any] struct{}
)

const (
	metadataHeader = "header"
)

func NewCsvStream[T, U any]() Processor[T, U] {
	return &csvStream[T, U]{}
}

func (c *csvStream[T, U]) Process(path string, hydrateFunc HydrateFunc[T, U]) []U {
	return concurrentProcess(path, hydrateFunc, c.processFileType)
}

func (c *csvStream[T, U]) processFileType(file *os.File, errCh chan error, fileData chan Data[T]) {
	reader := csv.NewReader(file)
	reader.ReuseRecord = true

	lineNumber := 0
	header, err := reader.Read()
	if err != nil {
		errCh <- err
		return
	}

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errCh <- err
		}

		fileData <- Data[T]{
			Content: c.toT(line),
			metadata: map[string]any{
				metadataLineNumber: lineNumber,
				metadataHeader:     header,
			},
		}
		lineNumber++
	}

	return
}

func (c *csvStream[T, U]) toT(value []string) T {
	var ret any
	ret = value
	return ret.(T)
}
