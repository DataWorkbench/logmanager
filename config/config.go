package config

import (
	"fmt"
	"github.com/DataWorkbench/common/gtrace"
	"io/ioutil"
	"os"
	"time"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/loader"
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

// The config file path used by Load config
var FilePath string

const (
	envPrefix = "LOG_MANAGER"
)

type HdfsConfig struct {
	// NameNode addresses
	Addresses  string `json:"addresses"    yaml:"addresses"    env:"ADDRESSES"     validate:"required"`
	UserName   string `json:"user_name"    yaml:"user_name"    env:"USER_NAME"     validate:"required"`
	BufferSize int32  `json:"buffer_size"  yaml:"buffer_size"  env:"BUFFER_SIZE"   validate:"required"`
}

// Config is the configuration settings for logmanager
type Config struct {
	LogLevel      int8                   `json:"log_level"      yaml:"log_level"      env:"LOG_LEVEL"           validate:"gte=1,lte=5"`
	GRPCServer    *grpcwrap.ServerConfig `json:"grpc_server"    yaml:"grpc_server"    env:"GRPC_SERVER"         validate:"required"`
	GRPCLog       *grpcwrap.LogConfig    `json:"grpc_log"       yaml:"grpc_log"       env:"GRPC_LOG"            validate:"required"`
	MetricsServer *metrics.Config        `json:"metrics_server" yaml:"metrics_server" env:"METRICS_SERVER"      validate:"required"`
	Tracer        *gtrace.Config         `json:"tracer"         yaml:"tracer"         env:"TRACER"              validate:"required"`
	HdfsServer    *HdfsConfig            `json:"hdfs_server"    yaml:"hdfs_server"    env:"HDFS_SERVER"         validate:"required"`
}

func loadFromFile(cfg *Config) (err error) {
	if FilePath == "" {
		return
	}

	fmt.Printf("%s load config from file <%s>\n", time.Now().Format(time.RFC3339Nano), FilePath)

	var b []byte
	b, err = ioutil.ReadFile(FilePath)
	if err != nil && os.IsNotExist(err) {
		return
	}

	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		fmt.Println("parse config file error:", err)
	}
	return
}

// LoadConfig load all configuration from specified file
// Must be set `FilePath` before called
func Load() (cfg *Config, err error) {
	cfg = &Config{}

	_ = loadFromFile(cfg)

	l := loader.New(
		loader.WithPrefix(envPrefix),
		loader.WithTagName("env"),
		loader.WithOverride(true),
	)
	if err = l.Load(cfg); err != nil {
		return
	}

	// output the config content
	fmt.Printf("%s pid=%d the latest configuration: \n", time.Now().Format(time.RFC3339Nano), os.Getpid())
	fmt.Println("")
	b, _ := yaml.Marshal(cfg)
	fmt.Println(string(b))

	validate := validator.New()
	if err = validate.Struct(cfg); err != nil {
		return
	}

	return
}
