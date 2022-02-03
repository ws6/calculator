package runner

import (
	"context"
	"fmt"
	"log"

	"github.com/ws6/calculator/transformation"

	"strings"

	"github.com/ws6/calculator/utils/confighelper"

	"github.com/ws6/calculator/extraction"

	"github.com/beego/beego/v2/core/config"
)

func RunTransformers(ctx context.Context, configer config.Configer) error {

	allTypes := transformation.GetAllTypeNames()
	log.Println(`registered transformers are`, strings.Join(allTypes, ","))
	// extractor.GetAllType()
	//get installed

	installed, err := configer.Strings(`transformers::installed`)
	if err != nil {
		return err
	}
	log.Println(`installed transformers are  :`, len(installed), strings.Join(installed, ","))
	for _, ins := range installed {

		cfg, err := confighelper.GetSectionHasRef(configer, ins)
		if err != nil {
			log.Fatalf(`installed section[%s]:%s`, ins, err.Error())
			return err
		}

		tf := transformation.GetTransformerType(cfg[`type`])
		if tf == nil {

			return fmt.Errorf(`no transformer type:%s`, cfg[`type`])
		}

		//launch it
		//ctx,  cfg, ir
		_cfg, err := extraction.NewIncrefConfigFromConfiger(configer, ins)
		if err != nil {
			log.Fatal(`installed section:`, err.Error())
			return err
		}
		tfRunner, err := tf.NewTransformer(_cfg)
		if err != nil {
			return fmt.Errorf(`NewTransformer:%s`, err.Error())
		}
		go func(__cfg *confighelper.SectionConfig, __tf transformation.Transformer) {
			if err := transformation.TransformLoop(
				ctx, __cfg, __tf,
			); err != nil {
				log.Println(__tf.Name(), err, `exit...`)
			}
		}(_cfg, tfRunner)

	}

	return nil
}
