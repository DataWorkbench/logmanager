package handler

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/DataWorkbench/logmanager/internal"
)

func ConsumeJobLog(ctx context.Context, req *logpb.GetJobLogRequest, stream logpb.LogManager_GetJobLogServer) error {
	singleConsumer := internal.CreateSingleConsumer(logger, kafkaConfig,
		req.OffsetsInitial,
		req.BatchMax,
		req.FlowId,
		req.InstanceId,
		req.CustomGroupId,
		req.LogFileName,
		stream,
	)
	taskInfo := fmt.Sprintf("Start to fetch log from kafka, topic [%s] instance_id [%s] logFile [%s] groupID [%s]",
		req.FlowId, req.InstanceId, req.CustomGroupId)
	logger.Info().Msg(taskInfo).Fire()

	return singleConsumer.Consume(ctx)
}
