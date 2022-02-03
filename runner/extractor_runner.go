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

	"github.com/beego/beego/v2/core/config"
	beego "github.com/beego/beego/v2/server/web"
	"github.com/robfig/cron/v3"
)

func GetBinPath() (string, error) {
	e, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(e), nil
}

func GetConfiger() (config.Configer, error) {
	return beego.AppConfig, nil
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

	for i, ins := range installed {

		cfg, err := confighelper.GetSectionHasRef(configer, ins)
		if err != nil {
			log.Fatalf(`installed section[%s]:%s`, ins, err.Error())
			return err
		}

		ir := extraction.GetIncrefType(cfg[`type`])
		if ir == nil {
			log.Println(`no incref implemented-`, cfg[`type`])
			continue
		}

		//launch it
		//ctx,  cfg, ir
		_cfg, err := extraction.NewIncrefConfigFromConfiger(configer, ins)
		if err != nil {
			log.Fatal(`installed section:`, err.Error())
			return err
		}
		log.Println(`made cofig`)
		schedule, err := configer.String(fmt.Sprintf(`extractors::scheduler.%s`, ins))
		if err != nil {
			return fmt.Errorf(`get scheduler:%s`, err.Error())
		}

		//TODO each extractor needs a scheduler if it is there.
		taskFn := func(order int) func() {
			log.Println(ins, `schedulers is `, schedule)
			fmt.Println(`in order `, order, len(mutexes))
			ch := mutexes[order]
			return func() {

				select {
				case n := <-ch:
					//TODO report health and runnning state
					stats, err := extraction.Refresh(ctx, _cfg, ir)
					if err != nil {
						log.Fatalf(`incref[%s] failed:%s`, ins, err.Error())
						return
					}
					log.Println(ir.Type(), stats)
					mutexes[order] <- n + 1
				case <-time.After(time.Second * 10):
					log.Println(`it seems another task is runnig`, ir.Type(), order)

				}

			}
		}

		scheduler.AddFunc(schedule, taskFn(i))

	}
	scheduler.Start()
	defer scheduler.Stop()

	select {
	case <-ctx.Done():
	}
	return nil
}
