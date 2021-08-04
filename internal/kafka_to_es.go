package internal

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/kafka"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/logmanager/config"
	"github.com/olivere/elastic/v7"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var mapping = `
{
	"settings":{
		"number_of_shards": %d,
		"number_of_replicas": %d
	},
	"mappings":{
		"properties":{
			"instance_id":{
				"type":"keyword"
			},
			"log_file_name":{
				"type":"keyword"
			},
			"log_entry":{
				"type":"text"
			},
			"created":{
				"type":"long"
			}
		}
	}
}`

type EsLogEntry struct {
	InstanceId  string `json:"instance_id"`
	LogFileName string `json:"log_file_name"`
	LogEntry    string `json:"log_entry"`
	Created     int64  `json:"created"`
}

type DataLoader struct {
	Logger      *glog.Logger
	KafkaConfig *config.KafkaConfig
	EsConfig    *config.EsConfig
	EsClient    *elastic.Client
	EntriesCh   chan []string
	RegexpRule  *regexp.Regexp
	Consumer    *kafka.ConsumerDynamic
	Ctx         context.Context
}

type ConsumerServer struct {
	KafkaDataLoader *DataLoader
}

func InitConsumerServer(ctx context.Context, kafkaConfig *config.KafkaConfig, esConfig *config.EsConfig) *ConsumerServer {
	dataLoader := InitDataLoader(ctx, kafkaConfig, esConfig)
	return &ConsumerServer{
		KafkaDataLoader: dataLoader,
	}
}

func (c *ConsumerServer) ServiceStart() {
	stopCh := make(chan error)
	go func() {
		stopCh <- c.KafkaDataLoader.Start()
	}()

	select {
	case <-c.KafkaDataLoader.Ctx.Done():
		return
	case <-stopCh:
		return
	}
}

func (c *ConsumerServer) ServiceClose() {
	_ = c.KafkaDataLoader.Consumer.Close()
}

func InitDataLoader(ctx context.Context, kafkaConfig *config.KafkaConfig, esConfig *config.EsConfig) *DataLoader {
	logger := glog.FromContext(ctx)
	client, err := InitEsClient(ctx, logger, esConfig.Urls, esConfig.IndexName, esConfig.ShardsNum, esConfig.ReplicaNum)
	if err != nil {
		logger.Error().Msg("Init ES client failed").String("err", err.Error()).Fire()
		panic(err)
	}
	return &DataLoader{
		Ctx:         ctx,
		Logger:      logger,
		KafkaConfig: kafkaConfig,
		EsConfig:    esConfig,
		EsClient:    client,
		EntriesCh:   make(chan []string),
		RegexpRule:  GetLogRegexpRule(),
	}
}

func (d *DataLoader) ConsumeHandler(ctx context.Context, messages []*kafka.ConsumerMessage) (err error) {
	var _data []string
	for _, msg := range messages {
		_data = append(_data, string(msg.Value))
		d.Logger.Debug().Msg("ConsumeHandler: new messages").String("key", string(msg.Key)).String("value", string(msg.Value)).Fire()
	}

	d.Logger.Info().Msg("start to send data to elasticsearch").String("topic", d.KafkaConfig.TopicRegexp).Fire()
	if len(_data) == 0 {
		d.Logger.Warn().Msg("got no log messages from kafka").Fire()
		return
	}

	bulkRequest := d.EsClient.Bulk()
	d.Logger.Info().Msg("receive messages from Kafka consume").Int("count", len(messages)).Fire()
	newEntries := getImplLogEntries(d.Logger, d.RegexpRule, _data)
	cacheCount := len(newEntries)

	for _, entry := range newEntries {
		req := elastic.NewBulkCreateRequest().Index(d.EsConfig.IndexName).Doc(entry)
		bulkRequest = bulkRequest.Add(req)
	}
	err = bulkCreateRequest(ctx, d.Logger, bulkRequest)
	if err != nil {
		return
	}
	d.Logger.Info().Msg("Flush cached log entries into ES").Int("messageCount", cacheCount).Fire()

	return
}

func (d *DataLoader) Start() error {
	_config := &kafka.ConsumerConfig{
		Hosts:            d.KafkaConfig.Brokers,
		RefreshFrequency: time.Second * time.Duration(d.KafkaConfig.RefreshFrequency),
		OffsetsInitial:   d.KafkaConfig.OffsetsInitial,
		BalanceStrategy:  d.KafkaConfig.BalanceStrategy,
	}

	groupID := d.KafkaConfig.GroupId
	consumer, err := kafka.NewConsumerDynamic(d.Ctx, groupID, _config, d.ConsumeHandler,
		kafka.WithBatchMode(d.KafkaConfig.BatchMode),
		kafka.WithBatchMax(d.KafkaConfig.BatchMax),
	)
	if err != nil {
		d.Logger.Error().Msg("Create Kafka ConsumerDynamic failed").Fire()
		return err
	}

	d.Consumer = consumer

	return d.Consumer.Consume([]string{d.KafkaConfig.TopicRegexp})
}

func (d *DataLoader) Close() {
	if d.Consumer == nil {
		return
	}

	err := d.Consumer.Close()
	if err != nil {
		d.Logger.Error().Error("Close ConsumerDynamic failed", err).Fire()
	}
}

func InitEsClient(ctx context.Context, logger *glog.Logger, urls string, indexName string, shardsNum int, replicaNum int) (*elastic.Client, error) {
	esUrls := strings.Split(urls, ",")
	if len(esUrls) <= 0 {
		logger.Error().Msg("invalid es urls config").String("urls", urls).Fire()
		panic("elastic search url invalid")
	}

	client, err := elastic.NewClient(elastic.SetURL(esUrls...))
	if err != nil {
		logger.Error().Msg("create new es client failed").String("urls", urls).Fire()
		panic("create es client failed")
	}

	info, code, err := client.Ping(esUrls[0]).Do(ctx)
	if err != nil {
		logger.Error().Msg("ping es url failed").String("url", esUrls[0]).Fire()
		panic(err)
	}

	connInfo := fmt.Sprintf("ES returned with code %d and version %s", code, info.Version.Number)
	logger.Info().Msg(connInfo).Fire()

	messageMapping := fmt.Sprintf(mapping, shardsNum, replicaNum)

	exists, err := client.IndexExists(indexName).Do(ctx)
	if err != nil {
		logger.Error().Msg("Check Index existed failed").String("Index", indexName).Fire()
		return nil, qerror.QueryEsFailed
	}

	if !exists {
		_, err := client.CreateIndex(indexName).BodyString(messageMapping).Do(ctx)
		if err != nil {
			logger.Error().Msg("Create Index failed").String("Index", indexName).Fire()
			return nil, qerror.QueryEsFailed
		}
	}

	return client, nil
}

func GetLogRegexpRule() *regexp.Regexp {
	return regexp.MustCompile("^(\\[[\\w.-]*])\\s*(\\[[\\w.-]*])\\s*(\\[[\\w.-]*])")
}

func getImplLogEntries(logger *glog.Logger, regexRule *regexp.Regexp, messages []string) []*EsLogEntry {
	var entries []*EsLogEntry
	for _, message := range messages {
		entries = append(entries, implementLogEntry(logger, regexRule, message))
	}
	return entries
}

func implementLogEntry(logger *glog.Logger, regexRule *regexp.Regexp, message string) *EsLogEntry {
	groups := regexRule.FindStringSubmatch(message)
	var instanceID string
	var logFileName string
	var createdTimestamp int64
	var logEntry string
	var err error
	if len(groups) == 4 {
		createdString := getSubString(groups[1])
		createdTimestamp, err = strconv.ParseInt(createdString, 10, 64)
		if err != nil {
			logger.Error().Msg("parse created timestamp failed").
				String("createdString", createdString).
				String("err", err.Error()).Fire()
			createdTimestamp = 0
		}
		instanceID = getSubString(groups[2])
		logFileName = getSubString(groups[3])
		logger.Debug().Msg(fmt.Sprintf("createdTimestamp %d, instance_id %s, log_file %s", createdTimestamp, instanceID, logFileName)).Fire()

		resIndex := regexRule.FindAllStringIndex(message, 1)
		firstGroupIndex := resIndex[0]
		newIndex := firstGroupIndex[1] + 1
		if newIndex <= len(message)-1 {
			logEntry = message[newIndex:]
		}
	}
	return &EsLogEntry{
		InstanceId:  instanceID,
		LogFileName: logFileName,
		Created:     createdTimestamp,
		LogEntry:    logEntry,
	}
}

func getSubString(message string) string {
	length := len([]rune(message))
	if length <= 2 {
		return ""
	}
	return message[1 : length-1]
}

func bulkCreateRequest(ctx context.Context, logger *glog.Logger, bulkRequest *elastic.BulkService) (err error) {
	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		logger.Error().Msg("Bulk Create request failed").Fire()
		return
	}

	if bulkResponse != nil {
		created := bulkResponse.Created()
		logger.Info().Msg("Bulk Create request succeed").Int("count", len(created)).Fire()
	}
	logger.Info().Msg("Bulk Create request over").Fire()
	return
}
