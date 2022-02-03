package runner

import (
	"context"
	"log"
	"os"
)

func Run() {
	configer, err := GetConfiger()
	if err != nil {
		log.Fatal(err.Error())
		os.Exit(-1)
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	//TODO add loader
	//non-blocking
	if err := RunLoaders(ctx, configer); err != nil {
		log.Fatal(`RunLoaders runner:%s`, err.Error())
	}

	//non-blocking
	if err := RunTransformers(ctx, configer); err != nil {
		log.Fatal(`RunTransformers runner:%s`, err.Error())
	}

	//block here
	if err := RunExtractors(ctx, configer); err != nil {
		log.Fatal(`RunExtractors runner:%s`, err.Error())
	}
	//TODO install transformers
}
