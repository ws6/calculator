package transformation

//transformer.go define a generic interface to initialize and drive each transformer
// transformers are actually a consumer-producer pipe.
// the input is an event bus
// the output is one or multiple message specs bus. it intends to leave the loaders to solve the load issues.
import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/ws6/calculator/utils/confighelper"

	"reflect"
	"runtime"
	"sync"

	"github.com/ws6/klib"
)

const HEADER_ROUTER_KEY = `calculater_transformer_key`

//a consumer-producer pattern
type Transformer interface {
	//per-instance name
	Name() string //configuration key
	//struct name
	Type() string //event producer type for consumer to use
	NewTransformer(*confighelper.SectionConfig) (Transformer, error)
	//Transform could generate one or more messages
	Transform(context.Context, *klib.Message, chan<- *klib.Message) error
	Close() error
}

type AfterTransformFunc func(t Transformer, ctx context.Context, in *klib.Message, out *klib.Message) error

type MessageRouter map[string]chan *klib.Message

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

//TransformLoop
//init a consumer
//init a producer
//call transform function of Transformer
//forward the generated new message(s) to  producer

func TransformLoop(ctx context.Context, configer *confighelper.SectionConfig, _tr Transformer) error {
	tr, err := _tr.NewTransformer(configer)
	if err != nil {
		fmt.Println(`NewTransformer err`)
		return fmt.Errorf(`NewTransformer:%s`, err.Error())
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
		consumerGroupId = eventbus.GetConsumerGroupId() //getting it from ENV if exists or from klib config
	}
	if consumerGroupId == "" {
		return fmt.Errorf(`%s::consumer_group_id is empty`, configer.SectionName)
	}
	eventbus.SetConsumerGroupId(consumerGroupId)
	fmt.Println(`consumerGroupId==>`, consumerGroupId)
	//get event bus topic
	eventBusTopic := cfg[`event_bus_topic`]
	if eventBusTopic == "" {
		return fmt.Errorf(`eventBusTopic is empty`)
	}

	producerConfigSection, err := configer.Configer.String(
		fmt.Sprintf(`%s::producer_section`, configer.SectionName),
	)

	if err != nil || producerConfigSection == "" {

		if _producerConfigSection, _err := configer.Configer.String(
			fmt.Sprintf(`transformers::default_producer_section`),
		); _err == nil {
			producerConfigSection = _producerConfigSection
		}
	}

	if producerConfigSection == "" {
		return fmt.Errorf(`producerConfigSection is empty for %s`, tr.Name())
	}

	producerConfig, err := configer.Configer.GetSection(producerConfigSection)
	if err != nil {
		return fmt.Errorf(`GetSection(producerConfigSection[%s]):%s`, producerConfigSection, err.Error())
	}
	//now lift the producer
	messagebus, err := klib.NewKlib(producerConfig)
	if err != nil {
		return fmt.Errorf(`producer NewKlib:%s`, err.Error())
	}

	if pe, ok := tr.(klib.ProducerErrorHandler); ok {
		logrus.Info(`Installed ProducerErrorHandler for %s`, _tr.Type())
		messagebus.ProducerErrorHandler = pe
	}
	size := 1000
	if n, err := configer.Configer.Int(fmt.Sprintf(`%s::producer_cap`, configer.SectionName)); err == nil {
		if n > 0 {
			size = n
		}
	}
	producerChan := make(chan *klib.Message, size)

	producerTopic, err := configer.Configer.String(fmt.Sprintf(`%s::message_bus_topic`, configer.SectionName))
	if err != nil || producerTopic == "" {
		//use the global one
		fmt.Println(`checking default_message_bus_topic`)
		if _producerTopic, err := configer.Configer.String(
			fmt.Sprintf(`transformers::default_message_bus_topic`)); err == nil {
			producerTopic = _producerTopic
		}
	}
	if producerTopic == "" {
		return fmt.Errorf(`producerTopic is empty:%s`, tr.Name())
	}
	fmt.Println(`producerTopic`, producerTopic)
	go func() {
		if err := messagebus.ProduceChan(ctx, producerTopic, producerChan); err != nil {
			fmt.Println(`ProduceChan`, err.Error())
		}
	}()

	//get afterTransformFuncs
	afterTransformFuns := GetAllAfterTransformFunc(_tr)
	withCallBacks := func(kmsg *klib.Message) error {
		errChan := make(chan error, 2) //do not share with other goroutine
		_producerChan := make(chan *klib.Message, size)
		wgDone := make(chan bool)
		ctx2, cancelFn2 := context.WithCancel(ctx)
		defer cancelFn2()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer close(_producerChan)

			if err := tr.Transform(ctx2, kmsg, _producerChan); err != nil {
				errChan <- err
			}

		}()
		go func() {

			defer wg.Done()
			//install life cycle callbacks - unordered
			for outMsg := range _producerChan {
				for _, cb := range afterTransformFuns {
					//outMsg could get modified after fn call
					if err := cb(tr, ctx2, kmsg, outMsg); err != nil {

						errChan <- fmt.Errorf(`AfterTransformFunc(%s):%s`, GetFunctionName(cb), err.Error())
						cancelFn2()
						return
					}
				}
				select {
				case producerChan <- outMsg: //forwarding or unchanged
					continue
				case <-ctx2.Done():

					errChan <- ctx2.Err()
					return
				}

			}
		}()

		go func() {
			wg.Wait()
			close(wgDone)
		}()
		defer close(errChan)
		select {
		case <-wgDone:
			return nil
		case err := <-errChan:
			return err
		}
		return nil
	}
	withoutCallBacks := func(kmsg *klib.Message) error {

		if err := tr.Transform(ctx, kmsg, producerChan); err != nil {
			return err
		}
		return nil
	}

	haveCallbacks := false
	if len(afterTransformFuns) > 0 {
		haveCallbacks = true
	}

	eventbus.ConsumeLoop(ctx, eventBusTopic, func(kmsg *klib.Message) error {
		if haveCallbacks {

			return withCallBacks(kmsg)
		}

		//withoutcallbacks is not heavy
		return withoutCallBacks(kmsg)
	})

	fmt.Println(`exit...`)

	return ctx.Err()

}

func GetAllTypeNames() []string {
	all := GetAllType()
	ret := []string{}
	for _, t := range all {
		ret = append(ret, t.Type())
	}
	return ret
}

var RegisterType, GetTransformerType, GetAllType, AddAfterTransform, GetAllAfterTransformFunc = func() (
	func(Transformer),
	func(string) Transformer,
	func() []Transformer,
	func(Transformer, AfterTransformFunc),
	func(Transformer) []AfterTransformFunc,
) {
	cache := make(map[string]Transformer)
	afterTransformFuncCache := make(map[string][]AfterTransformFunc)
	var lock = &sync.Mutex{}
	return func(ir Transformer) {
			lock.Lock()
			cache[ir.Type()] = ir
			lock.Unlock()
		}, func(name string) Transformer {
			lock.Lock()
			found, ok := cache[name]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}, func() []Transformer {
			ret := []Transformer{}
			lock.Lock()
			defer lock.Unlock()
			for _, ir := range cache {
				ret = append(ret, ir)
			}
			return ret
		},
		func(t Transformer, f AfterTransformFunc) {
			lock.Lock()
			afterTransformFuncCache[t.Type()] = append(afterTransformFuncCache[t.Type()], f)
			lock.Unlock()
		},
		func(t Transformer) []AfterTransformFunc {
			lock.Lock()
			found, ok := afterTransformFuncCache[t.Type()]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}

}()
