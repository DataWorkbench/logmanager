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

type KafkaConfig struct {
	Brokers          string `json:"brokers" yaml:"brokers" env:"BROKERS" validate:"required"`
	GroupId          string `json:"group_id" yaml:"group_id" env:"GROUP_ID" validate:"required"`
	BatchMode        bool   `json:"batch_mode" yaml:"batch_mode" env:"BATCH_MODE" validate:"-"`
	BatchMax         int    `json:"batch_max" yaml:"batch_max" env:"BATCH_MAX" validate:"-"`
	TopicRegexp      string `json:"topic_regexp" yaml:"topic_regexp" env:"TOPIC_REGEXP" validate:"required"`
	RefreshFrequency int    `json:"refresh_frequency" yaml:"refresh_frequency" env:"REFRESH_FREQUENCY" validate:"-"`
	OffsetsInitial   int64  `json:"offsets_initial" yaml:"offsets_initial" env:"OFFSETS_INITIAL" validate:"oneof=-2 -1"`
	BalanceStrategy  string `json:"balance_strategy" yaml:"balance_strategy" env:"BALANCE_STRATEGY,default=sticky" validate:"oneof=sticky range roundRobin"`
}

type EsConfig struct {
	Urls       string `json:"urls" yaml:"urls" env:"URLS" validate:"required"`
	IndexName  string `json:"index_name" yaml:"index_name" env:"INDEX_NAME" validate:"required"`
	ShardsNum  int    `json:"shards_num" yaml:"shards_num" env:"SHARDS_NUM" validate:"required"`
	ReplicaNum int    `json:"replica_num" yaml:"replica_num" env:"REPLICA_NUM" validate:"required"`
}

// Config is the configuration settings for logmanager
type Config struct {
	LogLevel      int8                   `json:"log_level"      yaml:"log_level"      env:"LOG_LEVEL"           validate:"gte=1,lte=5"`
	GRPCServer    *grpcwrap.ServerConfig `json:"grpc_server"    yaml:"grpc_server"    env:"GRPC_SERVER"         validate:"required"`
	MetricsServer *metrics.Config        `json:"metrics_server" yaml:"metrics_server" env:"METRICS_SERVER"      validate:"required"`
	KafkaConfig   *KafkaConfig           `json:"kafka_config"   yaml:"kafka_config"   env:"KAFKA_CONFIG"        validate:"required"`
	EsConfig      *EsConfig              `json:"es_config"      yaml:"es_config"      env:"ES_CONFIG"           validate:"required"`
	Tracer        *gtrace.Config         `json:"tracer"         yaml:"tracer"         env:"TRACER"              validate:"required"`
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
