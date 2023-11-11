package json_processor

import (
	"encoding/json"
	"os"
	"sync"
)

type (
	Data[T comparable] struct {
		Content T
		Err     error
	}

	Stream[T, U comparable] struct {
		m sync.Mutex
	}

	HydrateFunc[T, U comparable] func(input T) (U, error)
)

func NewJsonStream[T, U comparable]() Stream[T, U] {
	return Stream[T, U]{}
}

func (s *Stream[T, U]) Process(path string, hydrateFunc HydrateFunc[T, U]) []U {
	dataCh := s.readFile(path)
	return s.processData(dataCh, hydrateFunc)
}

func (s *Stream[T, U]) processData(dataCh <-chan Data[T], hydrateFunc HydrateFunc[T, U]) []U {
	dataSlice := make([]U, 0)
	for data := range dataCh {
		go func(dt Data[T]) {
			d, _ := hydrateFunc(dt.Content)

			s.m.Lock()
			dataSlice = append(dataSlice, d)
			s.m.Unlock()
		}(data)
	}
	return dataSlice
}

func (s *Stream[T, U]) readFile(path string) <-chan Data[T] {
	fileData := make(chan Data[T], 4)
	go func() {
		defer close(fileData)

		file, err := os.Open(path)
		if err != nil {
			fileData <- Data[T]{Err: err}
			return
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				fileData <- Data[T]{Err: err}
			}
		}(file)

		decoder := json.NewDecoder(file)

		if _, err = decoder.Token(); err != nil {
			fileData <- Data[T]{Err: err}
		}

		for decoder.More() {
			data := new(T)
			err = decoder.Decode(data)
			if err != nil {
				fileData <- Data[T]{Err: err}
			}

			fileData <- Data[T]{Content: *data}
		}

		if _, err = decoder.Token(); err != nil {
			fileData <- Data[T]{Err: err}
		}
	}()

	return fileData
}
