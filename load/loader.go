package load

//Loader is a consumer. not need to implement producer part.
//some how it overlaps with transformers design

import (
	"context"

	"github.com/ws6/calculator/utils/confighelper"

	"fmt"
	"sync"

	"github.com/ws6/klib"
)

type Loader interface {
	//per-instance name
	Name() string //configuration key
	//struct name
	Type() string //event producer type for consumer to use
	NewLoader(*confighelper.SectionConfig) (Loader, error)
	//Transform could generate one or more messages
	Load(context.Context, *klib.Message) error
	Close() error
}

func GetAllTypeNames() []string {
	all := GetAllType()
	ret := []string{}
	for _, t := range all {
		ret = append(ret, t.Type())
	}
	return ret
}

var RegisterType, GetLoaderType, GetAllType = func() (
	func(Loader),
	func(string) Loader,
	func() []Loader,
) {
	cache := make(map[string]Loader)
	var lock = &sync.Mutex{}
	return func(ir Loader) {
			lock.Lock()
			cache[ir.Type()] = ir
			lock.Unlock()
		}, func(name string) Loader {
			lock.Lock()
			found, ok := cache[name]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}, func() []Loader {
			ret := []Loader{}
			lock.Lock()
			defer lock.Unlock()
			for _, ir := range cache {
				ret = append(ret, ir)
			}
			return ret
		}

}()

func LoaderLoop(ctx context.Context, configer *confighelper.SectionConfig, _tr Loader) error {
	tr, err := _tr.NewLoader(configer)
	if err != nil {
		return fmt.Errorf(`NewLoader:%s`, err.Error())
	}
	//_tr.Close()?
	defer tr.Close()

	//init consumer

	cfg, err := configer.Configer.GetSection(configer.SectionName)
	if err != nil {
		return fmt.Errorf(`GetSection:%s`, err.Error())
	}

	//init consumer from each transformer
	consumerSectionName := cfg[`consumer_section`]
	if consumerSectionName == "" {
		return fmt.Errorf(fmt.Sprintf(`[%s::consumer_section] is empty`, configer.SectionName))
	}

	consumerConfig, err := configer.Configer.GetSection(consumerSectionName)
	if err != nil {
		return fmt.Errorf(`get consumerSectionName[%s]:%s`, consumerSectionName, err.Error())
	}

	eventbus, err := klib.NewKlib(consumerConfig)
	if err != nil {
		return fmt.Errorf(`NewKlib:%s`, err.Error())
	}
	//the consumer group designed is from each section
	consumerGroupId := cfg[`consumer_group_id`]
	if consumerGroupId == "" {
		return fmt.Errorf(`%s::consumer_group_id is empty`, configer.SectionName)
	}
	eventbus.SetConsumerGroupId(consumerGroupId)
	//get event bus topic
	messageBusTopic := cfg[`message_bus_topic`]
	if messageBusTopic == "" {
		return fmt.Errorf(`message_bus_topic is empty`)
	}

	eventbus.ConsumeLoop(ctx, messageBusTopic, func(kmsg *klib.Message) error {
		return tr.Load(ctx, kmsg)
	})

	return ctx.Err()

}
