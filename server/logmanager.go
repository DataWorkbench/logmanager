package server

import (
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/DataWorkbench/logmanager/handler"
)

type LogManagerServer struct {
	logpb.UnimplementedLogManagerServer
}

func (s *LogManagerServer) GetJobLog(req *logpb.GetJobLogRequest, stream logpb.LogManager_GetJobLogServer) error {
	ctx := stream.Context()
	return handler.ConsumeJobLog(ctx, req, stream)
}
