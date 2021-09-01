package handler

import (
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/logmanager/config"
)

// global options in this package.
var (
	logger           *glog.Logger
	HdfsServerConfig *config.HdfsConfig
)

type Option func()

func WithLogger(lp *glog.Logger) Option {
	return func() {
		logger = lp
	}
}

func WithHdfsConfig(hc *config.HdfsConfig) Option {
	return func() {
		HdfsServerConfig = hc
	}
}

func Init(opts ...Option) {
	for _, opt := range opts {
		opt()
	}
}
