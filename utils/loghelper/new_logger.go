package loghelper

import (
	"github.com/sirupsen/logrus"
)

func NewLogger() *logrus.Logger {
	return logrus.New()
}
