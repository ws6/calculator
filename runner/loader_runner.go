package runner

import (
	"context"
	"fmt"
	"log"

	"github.com/ws6/calculator/load"

	"strings"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/utils/confighelper"

	"github.com/beego/beego/v2/core/config"
)

func RunLoaders(ctx context.Context, configer config.Configer) error {

	allTypes := load.GetAllTypeNames()
	log.Println(`registered transformers are`, strings.Join(allTypes, ","))
	// extractor.GetAllType()
	//get installed

	installed, err := configer.Strings(`loaders::installed`)
	if err != nil {
		return err
	}
	log.Println(`installed loaders are  :`, len(installed), strings.Join(installed, ","))
	for _, ins := range installed {

		cfg, err := confighelper.GetSectionHasRef(configer, ins)
		if err != nil {
			log.Fatalf(`installed section[%s]:%s`, ins, err.Error())
			return err
		}

		tf := load.GetLoaderType(cfg[`type`])
		if tf == nil {

			return fmt.Errorf(`no loaders type:%s`, cfg[`type`])
		}

		//launch it
		//ctx,  cfg, ir
		_cfg, err := extraction.NewIncrefConfigFromConfiger(configer, ins)
		if err != nil {
			log.Fatal(`installed section:`, err.Error())
			return err
		}
		tfRunner, err := tf.NewLoader(_cfg)
		if err != nil {
			return fmt.Errorf(`NewLoader:%s`, err.Error())
		}
		go func(__cfg *confighelper.SectionConfig, __tf load.Loader) {
			if err := load.LoaderLoop(
				ctx, __cfg, __tf,
			); err != nil {
				log.Println(__tf.Name(), err, `exit...`)
			}
		}(_cfg, tfRunner)

	}

	return nil
}
