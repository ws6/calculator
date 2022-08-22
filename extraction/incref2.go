package extraction

import (
	"context"
	"strconv"

	"fmt"

	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/dlock"
	"github.com/ws6/klib"
)

const (
	DEFAULT_PROGESS_BATCH_SIZE = 1000
)

type Extractor struct {
	Cfg             *confighelper.SectionConfig
	ir              Incref
	prog            progressor.Progressor
	eventBus        *klib.Klib
	DistributedLock *dlock.Dlock
	eventBusTopic   string
	Scheduler       string
	msgChan         chan *klib.Message
}

// msgChan := make(chan *klib.Message, GetProducerChanCap(extractor.Cfg))
// defer close(msgChan)
// return ret, nil

func (self *Extractor) Close() error {
	close(self.msgChan)
	self.ir.Close()
	self.prog.Close()
	self.eventBus.Close()
	if self.DistributedLock != nil {
		self.DistributedLock.Close()
	}

	return nil
}

func NewExtractor(ctx context.Context, cfg *confighelper.SectionConfig, IncrefName string) (*Extractor, error) {
	ret := new(Extractor)
	ret.Cfg = cfg

	ret.msgChan = make(chan *klib.Message, GetProducerChanCap(ret.Cfg))

	dlockConfigSection, err := cfg.Configer.String(fmt.Sprintf(`%s::dlock_config_section`, cfg.SectionName))
	if err == nil && dlockConfigSection != "" {
		dlockcfg, err1 := cfg.Configer.GetSection(dlockConfigSection)
		if err1 != nil {
			return nil, fmt.Errorf(`GetSection(%s):%s`, dlockConfigSection, err1.Error())
		}
		ret.DistributedLock, err = dlock.NewDlock(dlockcfg)

		if err != nil {
			return nil, fmt.Errorf(`NewDlock:%s`, err.Error())
		}
		fmt.Println(`dlock enabled`)
	}

	eventBus, err := GetEventBus(cfg)
	if err != nil {
		return nil, fmt.Errorf(`GetEventBus:%s`, err.Error())
	}

	ret.eventBus = eventBus

	topic, err := GetEventBusTopic(cfg)

	if err != nil {
		return nil, fmt.Errorf(`GetEventBusTopic:%s`, err.Error())
	}
	if topic == "" {
		return nil, fmt.Errorf(`event_bus_topic is empty`)
	}
	ret.eventBusTopic = topic

	irPre := GetIncrefType(IncrefName)
	if irPre == nil {
		return nil, fmt.Errorf(`incref [%s] not exist`, IncrefName)
	}
	cfg2 := cfg.Copy()
	ir, err := irPre.NewIncref(cfg2)
	if err != nil {
		return nil, fmt.Errorf(`NewIncref[%s]:%s`, IncrefName, err.Error())
	}
	ret.ir = ir

	// progDriver, err := cfg.Configer.String(
	// 	fmt.Sprintf(`%s::progressor`, cfg.SectionName),
	// )
	// if err != nil {
	// 	return nil, fmt.Errorf(`progDriver:%s`, err.Error())
	// }
	progDriver := cfg2.ConfigMap[`progressor`]
	prog, err := InitProgrssorFromConfigSection(cfg2.Configer, progDriver)

	if err != nil {
		return nil, fmt.Errorf(`InitProgrssorFromConfigSection:%s`, err.Error())
	}

	ret.prog = prog
	go func() {
		if err := ret.eventBus.ProduceChan(ctx, ret.eventBusTopic, ret.msgChan); err != nil {
			log.Errorf(`ProduceChan:%s`, err.Error())
		}
	}()
	return ret, nil
}

func Refresh(ctx context.Context, extractor *Extractor) (*RefreshStat, error) {
	IncrefName := extractor.ir.Name()
	defer func() {
		fmt.Println(`Refresh exist 2`, IncrefName)
	}()
	ret := new(RefreshStat)

	progress, err := extractor.prog.GetProgress(extractor.ir.Name())
	if err != nil {
		if err != progressor.NOT_FOUND_PROGRESS {
			return ret, fmt.Errorf(`GetProgress:%s`, err.Error())
		}
		//TODO warning
		log.Debug(`no progress found,continue`)
		progress = new(progressor.Progress)

	}
	ch, err := extractor.ir.GetChan(ctx, progress)
	if err != nil {
		return ret, fmt.Errorf(`Refresh:%s`, err.Error())
	}

	if ch == nil {
		return ret, fmt.Errorf(`no channel created`)
	}
	foreached := false
	fe, ok := extractor.ir.(ForEached)
	if ok {
		foreached = true
	}
	//save_progress_batch_size
	SaveProgressBatchSize := int64(DEFAULT_PROGESS_BATCH_SIZE)
	if s, ok := extractor.Cfg.ConfigMap[`save_progress_batch_size`]; ok {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 {
			SaveProgressBatchSize = n
		}
	}
	for item := range ch {

		ret.Total++
		//TODO conduct the klib.Message to the EventBus with auto matic routing
		topub, err := ToKmessage(item)
		if err != nil {
			if !extractor.ir.SaveProgressOnFail(err) {
				return ret, fmt.Errorf(`ForEach:%s`, err.Error())
			}
		}
		if foreached {
			_topub, err := fe.ForEach(ctx, item)
			if err != nil {
				ret.Failed++

				if !extractor.ir.SaveProgressOnFail(err) {
					return ret, fmt.Errorf(`ForEach:%s`, err.Error())
				}
				//TODO error log
				continue
			}
			topub = _topub
		}
		MandateHeaders(extractor.ir, topub)
		select {
		case extractor.msgChan <- topub:
			log.Debug(`publised message`)
		case <-ctx.Done(): //!!missing the last item update progress; overlap
			break

		}
		extractor.ir.UpdateProgress(item, progress)
		if SaveProgressBatchSize != 0 && ret.Total >= SaveProgressBatchSize {
			if ret.Total%SaveProgressBatchSize == 0 {
				if err := extractor.prog.SaveProgress(extractor.ir.Name(), progress); err != nil {
					return ret, fmt.Errorf(`SaveProgress:%s`, err.Error())
				}
			}
		}
	}

	return ret, extractor.prog.SaveProgress(extractor.ir.Name(), progress)
}
