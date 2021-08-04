package internal

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/kafka"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/DataWorkbench/logmanager/config"
	"time"
)

type SingleConsumer struct {
	Logger           *glog.Logger
	Brokers          string
	GroupId          string
	Topic            string
	RefreshFrequency int
	OffsetsInitial   int64
	BalanceStrategy  string
	BatchMax         int
	InstanceId       string
	LogFileName      string
	Stream           logpb.LogManager_GetJobLogServer
}

func CreateSingleConsumer(logger *glog.Logger, kafkaConfig *config.KafkaConfig, offsetsInitial int64, batchMax int32,
	topic, instanceId, groupId, logFileName string, stream logpb.LogManager_GetJobLogServer) *SingleConsumer {
	return &SingleConsumer{
		Logger:           logger,
		Brokers:          kafkaConfig.Brokers,
		GroupId:          groupId,
		Topic:            topic,
		RefreshFrequency: kafkaConfig.RefreshFrequency,
		OffsetsInitial:   offsetsInitial,
		BalanceStrategy:  kafkaConfig.BalanceStrategy,
		BatchMax:         int(batchMax),
		InstanceId:       instanceId,
		LogFileName:      logFileName,
		Stream:           stream,
	}
}

func (k *SingleConsumer) ConsumeHandler(ctx context.Context, messages []*kafka.ConsumerMessage) (err error) {
	_dataMap := make(map[string][]string)
	regexpRule := GetLogRegexpRule()
	validCount := 0
	for _, msg := range messages {
		rawLogEntry := string(msg.Value)
		// filter log with the InstanceId
		groups := regexpRule.FindStringSubmatch(rawLogEntry)
		if len(groups) == 4 {
			instanceID := getSubString(groups[2])
			logFileName := getSubString(groups[3])
			//k.Logger.Debug().Msg(fmt.Sprintf("instance_id %s, log_file %s", instanceID, logFileName)).Fire()

			if instanceID != k.InstanceId {
				continue
			}

			if len(k.LogFileName) != 0 && k.LogFileName != logFileName {
				k.Logger.Debug().Msg(fmt.Sprintf("filter log with logFileName [%s], target file [%s]", logFileName, k.LogFileName)).Fire()
				continue
			}

			resIndex := regexpRule.FindAllStringIndex(rawLogEntry, 1)
			firstGroupIndex := resIndex[0]
			// only return real log to user
			newIndex := firstGroupIndex[1] + 1
			if newIndex <= len(rawLogEntry)-1 {
				logEntry := rawLogEntry[newIndex:]
				if cachedLogEntries, ok := _dataMap[logFileName]; ok {
					cachedLogEntries = append(cachedLogEntries, logEntry)
					_dataMap[logFileName] = cachedLogEntries
				} else {
					_dataMap[logFileName] = []string{logEntry}
				}
			}
			validCount++
		}
	}

	k.Logger.Debug().Msg("Fetch messages from kafka").Int("validCount", validCount).Fire()
	if validCount == 0 {
		return
	}

	resultMap := make(map[string]*logpb.FileLogEntries)
	for fileName, logs := range _dataMap {
		le := &logpb.FileLogEntries{
			LogEntries: logs,
		}
		resultMap[fileName] = le
	}

	streamResp := &logpb.StreamGetJobLogReply{
		FlowId:     k.Topic,
		InstanceId: k.InstanceId,
		LogFileMap: resultMap,
	}
	err = k.Stream.Send(streamResp)
	if err != nil {
		k.Logger.Error().Msg("Send data to Rpc Client failed").Fire()
		return
	}

	return
}

// Consume will exist when cases below:
// 1. err occurs when consuming Kafka messages
// 2. StreamContext cancel
func (k *SingleConsumer) Consume(ctx context.Context) (err error) {
	_config := &kafka.ConsumerConfig{
		Hosts:            k.Brokers,
		RefreshFrequency: time.Second * time.Duration(k.RefreshFrequency),
		OffsetsInitial:   k.OffsetsInitial,
		BalanceStrategy:  k.BalanceStrategy,
	}

	var group *kafka.ConsumerGroup
	defer func() {
		_ = group.Close()
	}()

	group, err = kafka.NewConsumerGroup(ctx, k.GroupId, _config, k.ConsumeHandler,
		kafka.WithBatchMode(true),
		kafka.WithBatchMax(k.BatchMax),
	)
	if err != nil {
		k.Logger.Error().Msg("Create Kafka ConsumerGroup failed").Fire()
		return
	}

	stopCh := make(chan error)

	go func() {
		// blocking until handling data failed or ConsumeGroup is Closed or ctx execute Cancel
		stopCh <- group.Consume([]string{k.Topic})
	}()

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-stopCh:
	}

	k.Logger.Info().Msg("Realtime logs consuming exists...").Fire()
	return
}
