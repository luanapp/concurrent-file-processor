package processor

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	userJson         = "sample_files/testdata.json"
	libraryJson      = "sample_files/ol_cdump.json"
	organizationsCsv = "sample_files/organizations-2000000.csv"
)

func TestJson_Process(t *testing.T) {
	t.Run("Run parallel process", func(t *testing.T) {
		stream := NewJsonStream[RawLibraryData, LibData]()
		users := stream.Process(libraryJson, ParseLibraryData)
		fmt.Printf("%d users processed", len(users))

		assert.True(t, len(users) > 0)
	})
}

func TestCsv_Process(t *testing.T) {
	t.Run("Run parallel process", func(t *testing.T) {
		stream := NewCsvStream[[]string, map[string]any]()
		organizations := stream.Process(organizationsCsv, ParseOrganizationData)
		fmt.Printf("%d organizations processed", len(organizations))

		assert.True(t, len(organizations) > 0)
	})
}

func TestUnmarshal(t *testing.T) {
	t.Run("Run unmarshal", func(t *testing.T) {
		m := sync.RWMutex{}

		bytes, err := os.ReadFile(libraryJson)
		if err != nil {
			t.Error(err)
		}

		users := make([]User, 0)
		err = json.Unmarshal(bytes, &users)
		if err != nil {
			t.Error(err)
		}

		assert.True(t, len(users) > 0)

		hUsers := make([]HydratedUser, 0, len(users))
		for _, user := range users {
			go func(u User) {
				time.Sleep(100 * time.Millisecond)
				hUser := HydratedUser{
					Name:      u.Name,
					BirthYear: time.Now().AddDate(-int(u.Age), 0, 0),
				}

				m.Lock()
				defer m.Unlock()
				hUsers = append(hUsers, hUser)
			}(user)
		}

		m.RLock()
		defer m.RUnlock()
		fmt.Printf("%d users processed", len(hUsers))
	})
}
