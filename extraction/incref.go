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

// type IncrefConfig struct {
// 	//given incref a global access of other configurations
// 	Configer config.Configer
// 	//either given a section name from Configer to fetch
// 	SectionName string
// 	//or passed in a ConfigMap for boostrap
// 	ConfigMap map[string]string
// }

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
	ret.ConfigMap, err = confighelper.GetSectionHasRef(ret.Configer, ret.SectionName)
	if err != nil {
		return nil, fmt.Errorf(`no section:%s`, err.Error())
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

func Refresh(ctx context.Context, cfg *confighelper.SectionConfig, _ir Incref) (*RefreshStat, error) {
	eventBus, err := GetEventBus(cfg)
	if err != nil {
		return nil, fmt.Errorf(`GetEventBus:%s`, err.Error())
	}

	defer eventBus.Close()

	topic, err := GetEventBusTopic(cfg)

	if err != nil {
		return nil, fmt.Errorf(`GetEventBusTopic:%s`, err.Error())
	}
	if topic == "" {
		return nil, fmt.Errorf(`event_bus_topic is empty`)
	}

	msgChan := make(chan *klib.Message, GetProducerChanCap(cfg))
	defer close(msgChan)
	go func() {
		if err := eventBus.ProduceChan(ctx, topic, msgChan); err != nil {
			log.Errorf(`ProduceChan:%s`, err.Error())
		}
	}()

	//TODO open a kmessage chan

	ir, err := _ir.NewIncref(cfg)
	ret := new(RefreshStat)
	if err != nil {
		return ret, fmt.Errorf(`NewIncref:%s`, err.Error())
	}

	progDriver, err := cfg.Configer.String(
		fmt.Sprintf(`%s::progressor`, cfg.SectionName),
	)
	if err != nil {
		return nil, fmt.Errorf(`progDriver:%s`, err.Error())
	}

	prog, err := InitProgrssorFromConfigSection(cfg.Configer, progDriver)

	if err != nil {
		return nil, fmt.Errorf(`InitProgrssorFromConfigSection:%s`, err.Error())
	}
	if prog == nil {
		return nil, fmt.Errorf(`prog is nil`)
	}

	//clean up
	defer ir.Close()

	// prog := progressor.GetInUsedProgressor()
	if prog == nil {
		return ret, fmt.Errorf(`no in used progress`)
	}
	progress, err := prog.GetProgress(ir.Name())
	if err != nil {
		if err != progressor.NOT_FOUND_PROGRESS {
			return ret, fmt.Errorf(`GetProgress:%s`, err.Error())
		}
		//TODO warning
		log.Debug(`no progress found,continue`)
		progress = new(progressor.Progress)

	}
	ch, err := ir.GetChan(ctx, progress)
	if err != nil {
		return ret, fmt.Errorf(`Refresh:%s`, err.Error())
	}

	if ch == nil {
		return ret, fmt.Errorf(`no channel created`)
	}
	foreached := false
	fe, ok := ir.(ForEached)
	if ok {
		foreached = true
	}
	for item := range ch {

		ret.Total++
		//TODO conduct the klib.Message to the EventBus with auto matic routing
		topub, err := ToKmessage(item)
		if err != nil {
			if !ir.SaveProgressOnFail(err) {
				return ret, fmt.Errorf(`ForEach:%s`, err.Error())
			}
		}
		if foreached {
			_topub, err := fe.ForEach(ctx, item)
			if err != nil {
				ret.Failed++

				if !ir.SaveProgressOnFail(err) {
					return ret, fmt.Errorf(`ForEach:%s`, err.Error())
				}
				//TODO error log
				continue
			}
			topub = _topub
		}
		MandateHeaders(ir, topub)
		select {
		case msgChan <- topub:
			log.Debug(`publised message`)
		case <-ctx.Done(): //!!missing the last item update progress; overlap
			break

		}
		ir.UpdateProgress(item, progress)

	}

	return ret, prog.SaveProgress(ir.Name(), progress)
}
