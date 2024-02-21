package runner

import (
	"context"
	"fmt"
	"log"
	"os"
)

func Run(ctx context.Context) {
	configer, err := GetConfiger()
	if err != nil {
		log.Fatal(err.Error())
		os.Exit(-1)
	}

	//TODO add loader
	//non-blocking
	if err := RunLoaders(ctx, configer); err != nil {
		log.Fatal(`RunLoaders runner:%s`, err.Error())
	}

	//non-blocking
	if err := RunTransformers(ctx, configer); err != nil {
		log.Fatal(`RunTransformers runner:%s`, err.Error())
	}
	fmt.Println(`started transformer`)
	//block here
	if err := RunExtractors(ctx, configer); err != nil {
		log.Fatal(`RunExtractors runner:%s`, err.Error())
	}
	//TODO install transformers
}
