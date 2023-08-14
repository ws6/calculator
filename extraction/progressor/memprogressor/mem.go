package memprogressor

import (
	"sync"

	"github.com/ws6/calculator/extraction/progressor"

	"github.com/ws6/calculator/utils/config"
)

const (
	MEM_PROGRESSOR = `MEM_PROGRESSOR`
)

var GetMap, SetMap, GetValue = func() (
	func() map[string]*progressor.Progress,
	func(string, *progressor.Progress),
	func(string) *progressor.Progress,
) {
	_m := make(map[string]*progressor.Progress)
	var lock = sync.Mutex{}
	return func() map[string]*progressor.Progress {
			return _m
		},
		func(k string, v *progressor.Progress) {
			lock.Lock()
			_m[k] = v
			lock.Unlock()
		},
		func(k string) *progressor.Progress {
			lock.Lock()
			found, ok := _m[k]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}
}()

func init() {
	progressor.RegisterProgressor(new(Mem))
}

type Mem struct {
	cache map[string]*progressor.Progress
	lock  sync.Mutex
}

func (self *Mem) NewProgressor(cfg config.Configer, sectionName string) (progressor.Progressor, error) {
	ret := new(Mem)
	ret.cache = make(map[string]*progressor.Progress)
	return ret, nil

}
func (self *Mem) Close() error {
	return nil
}

func (self *Mem) DriverName() string {
	return MEM_PROGRESSOR
}

func (self *Mem) GetProgress(name string) (*progressor.Progress, error) {
	found := GetValue(name)
	if found != nil {
		return found, nil
	}
	return nil, progressor.NOT_FOUND_PROGRESS
}

func (self *Mem) SaveProgress(name string, p *progressor.Progress) error {
	SetMap(name, p)
	return nil
}
