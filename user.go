package json_processor

import "time"

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
