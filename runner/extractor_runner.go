package runner

//runner.go organize compute units
//install configurations

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/utils/confighelper"

	// beego "github.com/beego/beego/v2/server/web"
	"github.com/robfig/cron/v3"
	"github.com/ws6/calculator/utils/config"
)

func GetBinPath() (string, error) {
	e, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(e), nil
}

func getExecutableDir() string {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	return exPath
}

func getDefaultAppConf() string {
	executableDir := getExecutableDir()
	return filepath.Join(executableDir, "conf/app.conf")
}

func GetConfiger() (config.Configer, error) {

	return config.NewConfig("ini", getDefaultAppConf())

}

//RunExtractors TODO add to cron by option
func RunExtractors(ctx context.Context, configer config.Configer) error {
	scheduler := cron.New()

	allTypes := extraction.GetAllTypeNames()
	log.Println(`registered extractors are`, strings.Join(allTypes, ","))
	// extractor.GetAllType()
	//get installed

	installed, err := configer.Strings(`extractors::installed`)
	if err != nil {
		return err
	}
	log.Println(`installed extractors are:`, len(installed), strings.Join(installed, ","))

	mutexes := make([]chan int, len(installed))

	for i := range mutexes {
		mutexes[i] = make(chan int, 1)
		mutexes[i] <- 1
	}
	extractors := []*extraction.Extractor{}

	for _, ins := range installed {
		fmt.Println(ins)
		cfg, err := confighelper.GetSectionHasRef(configer, ins)
		if err != nil {
			log.Fatalf(`installed section[%s]:%s`, ins, err.Error())
			return err
		}
		IncrefName := cfg[`type`]
		if IncrefName == "" {
			return fmt.Errorf(`Section[%s]has no type defined`, ins)
		}

		_cfg, err := extraction.NewIncrefConfigFromConfiger(configer, ins)
		if err != nil {
			log.Fatal(`installed section:`, err.Error())
			return err
		}

		// for k, v := range _cfg.ConfigMap {
		// 	fmt.Println(ins, k, v)
		// }

		extractor, err := extraction.NewExtractor(ctx, _cfg, IncrefName)
		if err != nil {
			return fmt.Errorf(`NewExtractor[%s]:%s`, IncrefName, err.Error())
		}

		defer extractor.Close()

		schedule, err := configer.String(fmt.Sprintf(`extractors::scheduler.%s`, ins))
		if err != nil {
			return fmt.Errorf(`get scheduler:%s`, err.Error())
		}
		extractor.Scheduler = schedule
		extractors = append(extractors, extractor)

	}
	for i, _extractor := range extractors {
		log.Println(`made cofig`, _extractor.Cfg.SectionName)

		//TODO each extractor needs a scheduler if it is there.
		taskFn := func(order int, extractor *extraction.Extractor) func() {

			ch := mutexes[order]
			return func() {
				//TODO add dlock
				if extractor.DistributedLock != nil {
					k := fmt.Sprintf(`%d`, order)
					dmux := extractor.DistributedLock.NewMutex(ctx, k)
					if err := dmux.Lock(); err != nil {
						fmt.Println(`dmux.Lock():`, err.Error())
						return
					}
					defer dmux.Unlock()

				}
				select {
				case n := <-ch:
					//TODO report health and runnning state
					stats, err := extraction.Refresh(ctx, extractor)
					if err != nil {
						log.Fatalf(`incref[%d] failed:%s`, order, err.Error())
						return
					}
					log.Println(order, stats)
					mutexes[order] <- n + 1
				case <-time.After(time.Second * 10):
					log.Println(`it seems another task is runnig`, order)

				}

			}
		}

		scheduler.AddFunc(_extractor.Scheduler, taskFn(i, _extractor))

	}
	scheduler.Start()
	defer scheduler.Stop()

	select {
	case <-ctx.Done():
	}
	return nil
}
