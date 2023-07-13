package progressor

import (
	"fmt"
	"sync"

	"github.com/ws6/calculator/utils/config"
)

var (
	NOT_FOUND_PROGRESS = fmt.Errorf(`not found progress`)
)

type Progressor interface {
	//TODO initialize the progressor with configurations
	GetProgress(string) (*Progress, error)
	SaveProgress(string, *Progress) error
	DriverName() string
	Boostraper
}

type Boostraper interface {
	//global configer and a sectionName
	NewProgressor(config.Configer, string) (Progressor, error)
	Close() error
}

var RegisterProgressor, GetProgressor = func() (
	func(Progressor),

	func(string) Progressor,
) {
	cache := make(map[string]Progressor)
	// initialized := make(map[string]bool)
	var lock = &sync.Mutex{}

	return func(p Progressor) {
			lock.Lock()
			cache[p.DriverName()] = p

			// if b, ok := initialized[p.DriverName()]; !ok || !b {

			// 	initialized[p.DriverName()] = true
			// }

			lock.Unlock()

		},
		func(name string) Progressor {

			lock.Lock()
			found, ok := cache[name]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}
}()
