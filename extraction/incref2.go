package extraction

import (
	"context"

	"fmt"

	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/klib"
)

func Refresh(ctx context.Context, cfg *confighelper.SectionConfig, IncrefName string) (*RefreshStat, error) {
	defer func() {
		fmt.Println(`Refresh exist`, IncrefName)
	}()
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

	ir := GetIncrefType(IncrefName)
	if ir == nil {
		return nil, fmt.Errorf(`incref [%s] not exist`, IncrefName)
	}
	if err := ir.NewIncref(cfg); err != nil {
		return nil, fmt.Errorf(`NewIncref[%s]:%s`, IncrefName, err.Error())
	}
	ret := new(RefreshStat)
	if err != nil {
		return ret, fmt.Errorf(`NewIncref:%s`, err.Error())
	}
	//clean up
	defer ir.Close()

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
	defer prog.Close()

	if prog == nil {
		return nil, fmt.Errorf(`prog is nil`)
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
