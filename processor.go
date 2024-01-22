package processor

import (
	"fmt"
	"os"
)

type (
	Data[T any] struct {
		Content  T
		metadata map[string]any
	}
	HydrateFunc[T any, U any] func(input T, metadata map[string]any) (U, error)
)

type Type int

const (
	Json Type = iota + 1
	Csv
)

const (
	metadataLineNumber = "lineNumber"
)

type (
	Processor[T, U any] interface {
		Process(path string, hydrateFunc HydrateFunc[T, U]) []U
		processFileType(file *os.File, errCh chan error, fileData chan Data[T])
	}

	ProcessFileFunc[T any] func(file *os.File, errCh chan error, fileData chan Data[T])
)

func NewProcessor[T, U any](t Type) Processor[T, U] {
	switch t {
	case Json:
		return NewJsonStream[T, U]()
	case Csv:
		return NewCsvStream[T, U]()
	default:
		return nil
	}
}

func concurrentProcess[T, U any](path string, hydrateFunc HydrateFunc[T, U], fileTypeProcess ProcessFileFunc[T]) []U {
	dataCh, errCh, done := readFile(path, fileTypeProcess)
	return processData(dataCh, errCh, done, hydrateFunc)
}

func processData[T, U any](dataCh <-chan Data[T], errCh <-chan error, done <-chan struct{}, hydrateFunc HydrateFunc[T, U]) []U {
	dataSlice := make([]U, 0)
	for {
		select {
		case data := <-dataCh:
			d, _ := hydrateFunc(data.Content, data.metadata)
			dataSlice = append(dataSlice, d)
		case err := <-errCh:
			fmt.Printf("error processing line: %v", err)
		case <-done:
			return dataSlice
		}
	}
}

func readFile[T any](path string, fileProc ProcessFileFunc[T]) (<-chan Data[T], <-chan error, <-chan struct{}) {
	fileData := make(chan Data[T], 4)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(fileData)
		defer close(errCh)
		defer close(done)

		file, err := os.Open(path)
		if err != nil {
			errCh <- err
			return
		}
		defer func(f *os.File) {
			err = f.Close()
			if err != nil {
				errCh <- err
			}
		}(file)

		fileProc(file, errCh, fileData)

		done <- struct{}{}
	}()

	return fileData, errCh, done
}
