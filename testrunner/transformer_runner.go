package testrunner

//this package is for testing transformers without kafka services
import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/ws6/calculator/transformation"

	"github.com/ws6/klib"

	"github.com/ws6/calculator/utils/config"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/utils/confighelper"
)

func NewCalculatorConfiger(filename, sectionName string) (*confighelper.SectionConfig, error) {
	cfgr, err := config.NewConfig(`ini`, filename)
	if err != nil {
		return nil, err
	}
	return extraction.NewIncrefConfigFromConfiger(cfgr, sectionName)
}

func RunTestTransformer(tx0 transformation.Transformer, appconfFileName string, sectionName string, testMsg *klib.Message) ([]*klib.Message, error) {
	ret := []*klib.Message{}
	//init config
	transformerCfgr, err := NewCalculatorConfiger(appconfFileName, sectionName)
	if err != nil {
		return nil, err
	}
	//init transformer
	tx1, err := tx0.NewTransformer(transformerCfgr)
	if err != nil {
		return nil, fmt.Errorf(`NewTransformer:%s`, err.Error())
	}

	outputChan := make(chan *klib.Message)
	var errRet error
	go func() {
		defer close(outputChan)
		if err := tx1.Transform(context.Background(),
			testMsg,
			outputChan,
		); err != nil {
			errRet = err
		}

	}()
	for om := range outputChan {
		ret = append(ret, om)
	}

	if errRet != nil {
		return nil, errRet
	}

	return ret, nil
}

func FileToKmessage(testPostageMsgJson string) (*klib.Message, error) {
	testMsg := new(klib.Message)

	body, err := ioutil.ReadFile(testPostageMsgJson)
	if err != nil {
		return nil, err
	}
	testMsg.Value = body
	return testMsg, nil
}

func KmessageToMsi(m *klib.Message) (map[string]interface{}, error) {

	ret := make(map[string]interface{})
	if err := json.Unmarshal(m.Value, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func KmessageToNiceJson(m *klib.Message) (string, error) {
	om, err := KmessageToMsi(m)
	if err != nil {
		return "", nil
	}
	ret, err := json.MarshalIndent(om, "", "  ")
	if err != nil {
		return "", err
	}
	return string(ret), nil
}
