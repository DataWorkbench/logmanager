package server

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/DataWorkbench/logmanager/handler"
	"github.com/DataWorkbench/logmanager/internal"
	"path/filepath"
)

type LogManagerServer struct {
	logpb.UnimplementedLogManagerServer
}

func (s *LogManagerServer) ListJMHistoryLogFiles(_ context.Context, req *logpb.ListHistLogsRequest) (*logpb.ListJMHistLogsReply, error) {
	JMHdfsDirPath := internal.GetHdfsDirPath(req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId(), constants.JobManagerName)
	resp := &logpb.ListJMHistLogsReply{
		Stat: getFileStatInDir(JMHdfsDirPath),
	}
	return resp, nil
}

// full path for taskManager log files:
// /:space_id/:flow_id/:inst_id/logs/taskmanager/:taskManager_id/:log_file
// so we need to get existed taskManagerIDs first
func (s *LogManagerServer) ListTMHistoryLogFiles(_ context.Context, req *logpb.ListHistLogsRequest) (*logpb.ListTMHistLogsReply, error) {
	TMHdfsDirPath := internal.GetHdfsDirPath(req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId(), constants.TaskManagerName)
	subDirInfos, err := handler.ListHistoryLogFiles(TMHdfsDirPath)
	if err != nil {
		return nil, err
	}

	resultMap := make(map[string]*logpb.TaskLogFiles)
	for _, _dirInfo := range subDirInfos {
		if !_dirInfo.IsDir() {
			continue
		}
		fullSubPath := fmt.Sprintf("%s/%s", TMHdfsDirPath, _dirInfo.Name())
		_logFileInfosUnderTaskManager := getFileStatInDir(fullSubPath)
		resultMap[_dirInfo.Name()] = &logpb.TaskLogFiles{Stat: _logFileInfosUnderTaskManager}
	}

	return &logpb.ListTMHistLogsReply{TaskLogs: resultMap}, nil
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

func (s *LogManagerServer) DownloadJobMgrLogFile(req *logpb.DownloadJobMgrRequest, stream logpb.LogManager_DownloadJobMgrLogFileServer) error {
	hdfsJobMgrFilePath := internal.GetHdfsJobMgrFilePath(req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId(), req.GetFileName())
	return handler.DownloadLogFile(hdfsJobMgrFilePath, stream)
}

func (s *LogManagerServer) DownloadTaskMgrLogFile(req *logpb.DownloadTaskMgrRequest, stream logpb.LogManager_DownloadTaskMgrLogFileServer) error {
	hdfsTaskMgrFilePath := internal.GetHdfsTaskMgrFilePath(req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId(), req.GetTaskManagerId(), req.GetFileName())
	return handler.DownloadLogFile(hdfsTaskMgrFilePath, stream)
}

func (s *LogManagerServer) UploadLogFile(_ context.Context, req *logpb.UploadFileRequest) (*logpb.UploadFileReply, error) {
	prePath := filepath.Join("/", req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId())
	return handler.UploadLogFile(req.GetServerUrl(), prePath)
}

func (s *LogManagerServer) GetUploadingTaskStat(_ context.Context, req *logpb.TaskStatRequest) (*logpb.TaskStatReply, error) {
	prePath := filepath.Join("/", req.GetSpaceId(), req.GetFlowId(), req.GetInstanceId())
	return handler.CheckUploadingTask(req.GetServerUrl(), prePath)
}
