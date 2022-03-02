package main

import (
	"context"

	"github.com/ws6/calculator/extraction"
	"github.com/ws6/calculator/extraction/progressor"
	"github.com/ws6/calculator/runner"
	"github.com/ws6/calculator/utils/confighelper"
)

type ExampleExtractor struct{}

func init() {
	extraction.RegisterType(new(ExampleExtractor))
}

func (self *ExampleExtractor) UpdateProgress(map[string]interface{}, *progressor.Progress) error {
	return nil
}

func (self *ExampleExtractor) Type() string {
	return `ExampleExtractor`
}
func (self *ExampleExtractor) SaveProgressOnFail(e error) bool {
	return true
}

func (self *ExampleExtractor) NewIncref(*confighelper.SectionConfig) (extraction.Incref, error) {
	return new(ExampleExtractor), nil
}

func (self *ExampleExtractor) Name() string {
	return `ExampleExtractor`
}

func (self *ExampleExtractor) Close() error {
	return nil
}
func (self *ExampleExtractor) GetChan(ctx context.Context, p *progressor.Progress) (chan map[string]interface{}, error) {
	return nil, nil
}

func main() {
	runner.Run(context.Background())
}
