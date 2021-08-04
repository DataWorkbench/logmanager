package handler

import (
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/logmanager/config"
)

// global options in this package.
var (
	logger      *glog.Logger
	kafkaConfig *config.KafkaConfig
)

type Option func()

func WithLogger(lp *glog.Logger) Option {
	return func() {
		logger = lp
	}
}

func WithKafkaConfig(kc *config.KafkaConfig) Option {
	return func() {
		kafkaConfig = kc
	}
}

func Init(opts ...Option) {
	for _, opt := range opts {
		opt()
	}
}
