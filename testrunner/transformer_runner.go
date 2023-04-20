package testrunner

import (
	"context"

	"github.com/beego/beego/v2/core/config"
	"github.com/ws6/klib"
)

// RunTransformers  init a transformer and ready for Transform one message
func RunTransformers(ctx context.Context, configer config.Configer, kmsg *klib.Message, messageBus chan<- *klib.Message) error {
	return nil
}
