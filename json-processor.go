package json_processor

import (
	"encoding/json"
	"os"
	"sync"
)

type (
	Data[T any] struct {
		Content T
		Err     error
	}

	Stream[T, U any] struct {
		m sync.RWMutex
	}

	HydrateFunc[T any, U any] func(input T) (U, error)
)

func NewJsonStream[T, U any]() Stream[T, U] {
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
			defer s.m.Unlock()
			dataSlice = append(dataSlice, d)
		}(data)
	}

	s.m.RLock()
	defer s.m.RUnlock()
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
		defer func(f *os.File) {
			err := f.Close()
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
