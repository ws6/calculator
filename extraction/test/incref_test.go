package test

import (
	"calculator/extraction/extractor"
	"calculator/extraction/extractor/lablynx"
	"calculator/extraction/progressor"
	"calculator/extraction/progressor/memprogressor"
	"context"

	"testing"
)

func getLabLynxConfiger() (*extractor.IncrefConfig, error) {
	appconf := `lablynx.app.conf`
	return extractor.NewIncrefConfig(appconf, `lablynx_section`)

}

func TestLabLynxIncref(t *testing.T) {
	ll := new(lablynx.LabLynxIncref)
	//set which progressor to use
	cfg, err := getLabLynxConfiger()
	if err != nil {
		t.Fatal(err.Error())
	}
	prog := progressor.GetProgressor(memprogressor.MEM_PROGRESSOR)

	if err := prog.NewProgressor(cfg.Configer); err != nil {
		t.Fatal(err.Error())
	}

	stat, err := extractor.Refresh(context.Background(), cfg, ll)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(`stat`, stat)
	t.Log(prog.GetProgress(ll.Name()))

	stat2, err := extractor.Refresh(context.Background(), cfg, ll)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(`stat2`, stat2)
	t.Log(prog.GetProgress(ll.Name()))

}
