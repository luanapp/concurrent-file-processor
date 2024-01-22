package processor

import (
	"encoding/json"
	"os"
)

type (
	jsonStream[T, U any] struct{}
)

func NewJsonStream[T, U any]() Processor[T, U] {
	return &jsonStream[T, U]{}
}

func (s *jsonStream[T, U]) Process(path string, hydrateFunc HydrateFunc[T, U]) []U {
	return concurrentProcess(path, hydrateFunc, s.processFileType)
}

func (s *jsonStream[T, U]) processFileType(file *os.File, errCh chan error, fileData chan Data[T]) {
	decoder := json.NewDecoder(file)

	if _, err := decoder.Token(); err != nil {
		errCh <- err
		return
	}

	line := 0
	for decoder.More() {
		data := new(T)
		err := decoder.Decode(data)
		if err != nil {
			errCh <- err
		}

		fileData <- Data[T]{
			Content: *data,
			metadata: map[string]any{
				metadataLineNumber: line,
			},
		}
		line++
	}

	if _, err := decoder.Token(); err != nil {
		errCh <- err
		return
	}
	return
}
