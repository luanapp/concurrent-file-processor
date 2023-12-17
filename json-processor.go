package json_processor

import (
	"encoding/json"
	"fmt"
	"os"
)

type (
	Data[T any] struct {
		Content T
	}

	Stream[T, U any] struct{}

	HydrateFunc[T any, U any] func(input T) (U, error)
)

func NewJsonStream[T, U any]() Stream[T, U] {
	return Stream[T, U]{}
}

func (s *Stream[T, U]) Process(path string, hydrateFunc HydrateFunc[T, U]) []U {
	dataCh, errCh, done := s.readFile(path)
	return s.processData(dataCh, errCh, done, hydrateFunc)
}

func (s *Stream[T, U]) processData(dataCh <-chan Data[T], errCh <-chan error, done <-chan struct{}, hydrateFunc HydrateFunc[T, U]) []U {

	dataSlice := make([]U, 0)
	for {
		select {
		case data := <-dataCh:
			d, _ := hydrateFunc(data.Content)
			dataSlice = append(dataSlice, d)
		case err := <-errCh:
			fmt.Printf("error processing line: %v", err)
		case <-done:
			return dataSlice
		}
	}
}

func (s *Stream[T, U]) readFile(path string) (<-chan Data[T], <-chan error, <-chan struct{}) {
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

		decoder := json.NewDecoder(file)

		if _, err = decoder.Token(); err != nil {
			errCh <- err
			return
		}

		for decoder.More() {
			data := new(T)
			err = decoder.Decode(data)
			if err != nil {
				errCh <- err
			}

			fileData <- Data[T]{Content: *data}
		}

		if _, err = decoder.Token(); err != nil {
			errCh <- err
			return
		}

		done <- struct{}{}
	}()

	return fileData, errCh, done
}
