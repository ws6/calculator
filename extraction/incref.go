package extraction

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/utils/confighelper"
	"github.com/ws6/calculator/utils/loghelper"

	"github.com/beego/beego/v2/core/config"
	"github.com/ws6/klib"
)

var log = loghelper.NewLogger()

const (
	CALCULATOR_CONFIGER_PATH   = `CALCULATOR_CONFIGER_PATH`
	DEFAULT_PRODUCER_CHAN_SIZE = 1000
)

type Incref interface {
	GetChan(ctx context.Context, p *progressor.Progress) (chan map[string]interface{}, error)

	//UpdateProgress is in memory level. before commit
	UpdateProgress(map[string]interface{}, *progressor.Progress) error

	//instance name. used by tansformer to pull out matched instance configurations
	ComputeUnit
	IncrefOptional
}

type ComputeUnit interface {
	Name() string //configuration key
	//event type. decides which transform to consume
	Type() string //event producer type for consumer to use
}

type EventPublisher interface {
	BuildMessage(context.Context, map[string]interface{})
	GetProducer()
	CloseProducer()
}

type ForEached interface {
	ForEach(context.Context, map[string]interface{}) (*klib.Message, error)
}

func NewIncrefConfig(iniFileName, sectionName string) (*confighelper.SectionConfig, error) {
	var err error
	ret := new(confighelper.SectionConfig)
	ret.SectionName = sectionName
	ret.ConfigMap = make(map[string]string)
	ret.Configer, err = GetConfiger(iniFileName)
	if err != nil {
		return nil, err
	}
	ret.ConfigMap, err = confighelper.GetSectionHasRef(ret.Configer, ret.SectionName)
	if err != nil {
		return nil, fmt.Errorf(`no section:%s`, err.Error())
	}
	return ret, nil
}
func NewIncrefConfigFromConfiger(configer config.Configer, sectionName string) (*confighelper.SectionConfig, error) {
	var err error
	ret := new(confighelper.SectionConfig)
	ret.SectionName = sectionName
	ret.ConfigMap = make(map[string]string)
	ret.Configer = configer
	if err != nil {
		return nil, err
	}
	_m, err := confighelper.GetSectionHasRef(ret.Configer, ret.SectionName)
	if err != nil {
		return nil, fmt.Errorf(`no section:%s`, err.Error())
	}
	for k, v := range _m {
		ret.ConfigMap[k] = v //copied
	}

	return ret, nil
}

//incref.go is the interface for incremental-refresh

type IncrefOptional interface {
	SaveProgressOnFail(error) bool
	//global configer and a particular section to boostrap
	NewIncref(*confighelper.SectionConfig) (Incref, error)
	Close() error
}

//TODO a driver for apply Incref
//a register for compile infref implementations
//the registration is based on config?

func TypeCheck(Incref) {}

var RegisterType, GetIncrefType, GetAllType = func() (
	func(Incref),
	func(string) Incref,
	func() []Incref,
) {
	cache := make(map[string]Incref)
	var lock = &sync.Mutex{}
	return func(ir Incref) {
			lock.Lock()
			cache[ir.Type()] = ir
			lock.Unlock()
		}, func(name string) Incref {
			lock.Lock()
			found, ok := cache[name]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}, func() []Incref {
			ret := []Incref{}
			lock.Lock()
			defer lock.Unlock()
			for _, ir := range cache {
				ret = append(ret, ir)
			}
			return ret
		}

}()

func GetAllTypeNames() []string {
	all := GetAllType()
	ret := []string{}
	for _, t := range all {
		ret = append(ret, t.Type())
	}
	return ret
}

func GetConfiger(pn string) (config.Configer, error) {
	return config.NewConfig(`ini`, pn)
}

type RefreshStat struct {
	Total  int64
	Failed int64
}

//InitProgrssorFromConfigSection and initialized
func InitProgrssorFromConfigSection(configer config.Configer, sectionName string) (progressor.Progressor, error) {
	configMap, err := confighelper.GetSectionHasRef(configer, sectionName)
	if err != nil {
		return nil, fmt.Errorf(`GetSectionHasRef[%s]:%s`, sectionName, err.Error())
	}
	driverName := configMap[`type`]
	if driverName == "" {
		return nil, fmt.Errorf(`progressor driver is empty under section[%s]`, sectionName)
	}
	prog := progressor.GetProgressor(driverName)
	if prog == nil {
		return nil, fmt.Errorf(`not progDriver:%s`, driverName)
	}
	return prog.NewProgressor(configer, sectionName)
}

func ToKmessage(item map[string]interface{}) (*klib.Message, error) {
	body, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}

	kmsg := new(klib.Message)

	kmsg.Value = body
	//TODO overwrite headers and keys
	return kmsg, nil
}

func GetEventBus(cfg *confighelper.SectionConfig) (*klib.Klib, error) {
	kcfg, err := confighelper.GetSectionHasRef(cfg.Configer, cfg.SectionName)
	if err != nil {
		return nil, err
	}

	return klib.NewKlib(kcfg)
}

func GetEventBusTopic(cfg *confighelper.SectionConfig) (string, error) {
	return cfg.ConfigMap[`event_bus_topic`], nil
	return cfg.Configer.String(fmt.Sprintf(`%s::event_bus_topic`, cfg.SectionName))
}
func GetProducerChanCap(cfg *confighelper.SectionConfig) int {
	ret, err := cfg.Configer.Int(fmt.Sprintf(`%s::producer_chan_size`, cfg.SectionName))
	if err != nil {
		return DEFAULT_PRODUCER_CHAN_SIZE
	}
	return ret
}

func MandateHeaders(ir ComputeUnit, msg *klib.Message) {
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	msg.Headers[`calculator_name`] = ir.Name()
	msg.Headers[`calculator_type`] = ir.Type()
	msg.Headers[`calculator_unit_type`] = `extractor`
}
