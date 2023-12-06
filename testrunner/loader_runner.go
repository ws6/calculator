package testrunner

import (
	"context"
	"fmt"

	"github.com/ws6/calculator/load"
	"github.com/ws6/klib"
)

func RunTestLoader(tx0 load.Loader, appconfFileName string, sectionName string, testMsg *klib.Message) error {

	//init config
	transformerCfgr, err := NewCalculatorConfiger(appconfFileName, sectionName)
	if err != nil {
		return err
	}
	//init transformer
	tx1, err := tx0.NewLoader(transformerCfgr)
	if err != nil {
		return fmt.Errorf(`NewTransformer:%s`, err.Error())
	}

	return tx1.Load(context.Background(),
		testMsg,
	)
}
