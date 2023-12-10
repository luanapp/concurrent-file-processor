package json_processor

import (
	"errors"
	"time"
)

type (
	User struct {
		Name  string `json:"name"`
		Age   int8   `json:"age"`
		Email string `json:"email"`
	}

	HydratedUser struct {
		Name      string
		BirthYear time.Time
		AccountId string
	}
)

type (
	RawLibraryData map[string]any
	LibData        interface {
		GetName() string
	}
)

type Author struct {
	Name string `json:"name"`
}

func (a Author) GetName() string {
	return a.Name
}

type Edition struct {
	Title string `json:"title"`
}

func (e Edition) GetName() string {
	return e.Title
}

func ParseLibraryData(input RawLibraryData) (LibData, error) {
	switch getLibraryType(input) {
	case "/type/author":
		name := (input)["name"].(string)
		return &Author{Name: name}, nil
	case "/type/edition":
		name := (input)["title"].(string)
		return &Edition{Title: name}, nil
	default:
		return nil, errors.New("library type not supported")
	}
}

func getLibraryType(input RawLibraryData) string {
	lType := input["type"]
	switch lType.(type) {
	case map[string]string:
		lTypeKey := lType.(map[string]string)
		return lTypeKey["key"]
	default:
		return ""
	}
}
