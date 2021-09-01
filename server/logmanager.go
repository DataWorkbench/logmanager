package server

import (
	"context"
	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/DataWorkbench/logmanager/handler"
	"path/filepath"
)

type LogManagerServer struct {
	logpb.UnimplementedLogManagerServer
}

func (s *LogManagerServer) ListHistoryLogFiles(_ context.Context, req *logpb.ListHistLogsRequest) (*logpb.ListHistLogsReply, error) {
	managerName := req.GetManager()
	if managerName != constants.JobManagerName && managerName != constants.TaskManagerName {
		return nil, qerror.InvalidParams.Format(managerName)
	}

	dirPath := filepath.Join("/", req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId(), managerName)
	resp := &logpb.ListHistLogsReply{
		Stat: getFileStatInDir(dirPath),
	}
	return resp, nil
}

func getFileStatInDir(hdfsDirPath string) []*logpb.FileState {
	result := []*logpb.FileState{}
	logFileInfos, err := handler.ListHistoryLogFiles(hdfsDirPath)
	if err == nil {
		for _, JMLogFile := range logFileInfos {
			_info := &logpb.FileState{
				Size:     JMLogFile.Size(),
				FileName: JMLogFile.Name(),
			}
			result = append(result, _info)
		}
	}
	return result
}

func (s *LogManagerServer) DownloadLogFile(req *logpb.DownloadRequest, stream logpb.LogManager_DownloadLogFileServer) error {
	filePath := filepath.Join("/", req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId(), req.GetFileName(), req.GetManager())
	return handler.DownloadLogFile(filePath, stream)
}

func (s *LogManagerServer) UploadLogFile(_ context.Context, req *logpb.UploadFileRequest) (*logpb.UploadFileReply, error) {
	prePath := filepath.Join("/", req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId())
	return handler.UploadLogFile(req.GetServerUrl(), prePath)
}

func (s *LogManagerServer) GetUploadingTaskStat(_ context.Context, req *logpb.TaskStatRequest) (*logpb.TaskStatReply, error) {
	prePath := filepath.Join("/", req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId())
	return handler.CheckUploadingTask(req.GetServerUrl(), prePath)
}
