package handler

import (
	"bufio"
	"context"
	"fmt"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/DataWorkbench/logmanager/internal"
	"github.com/colinmarc/hdfs/v2"
	"io"
	"os"
	"path"
)

type FileDataBlock struct {
	Data []byte
	Err  error
}

func ListHistoryLogFiles(dirPath string) ([]os.FileInfo, error) {
	logger.Debug().Msg(fmt.Sprintf("try to list log files in Dir [%s]", dirPath))
	hdfsClient, err := internal.GetClient(HdfsServerConfig)
	if err != nil {
		logger.Error().Error("failed to create HDFS client", err).Fire()
		return nil, err
	}

	defer hdfsClient.Close()
	fileInfos, err := internal.StatFilesInDir(hdfsClient, dirPath)
	if err != nil {
		logger.Error().Msg(fmt.Sprintf("failed to stat files in Dir [%s]", dirPath)).Fire()
		return nil, err
	}

	return fileInfos, nil
}

func DownloadLogFile(filePath string, stream logpb.LogManager_DownloadJobMgrLogFileServer) (err error) {
	logger.Debug().Msg(fmt.Sprintf("try to Download file [%s]", filePath)).Fire()
	hdfsClient, err := internal.GetClient(HdfsServerConfig)
	if err != nil {
		logger.Error().Error("failed to create HDFS client", err).Fire()
		return
	}

	defer hdfsClient.Close()
	fileInfo, err := internal.StatFile(hdfsClient, filePath)
	if err != nil {
		return
	}

	fSize := fileInfo.Size()

	blockCh := make(chan FileDataBlock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		logger.Debug().String("begin to upload file", filePath).Int("fileSize", int(fSize)).Fire()
		downloadFileFromHdfs(ctx, hdfsClient, filePath, blockCh)
		logger.Debug().String("uploading over, file", filePath).Fire()
	}()

	for {
		blockData, ok := <-blockCh
		if !ok {
			logger.Info().Msg(fmt.Sprintf("download file [%s] completed", filePath)).Fire()
			return
		}

		if blockData.Err != nil {
			err = blockData.Err
			logger.Error().Error("download hdfs file failed", err).Fire()
			return
		}

		err = stream.Send(&logpb.FileContent{
			FileData: blockData.Data,
			FileSize: fSize,
		})
		if err != nil {
			logger.Error().Error("stream Send data failed", err).Fire()
			return
		}
	}
}

func downloadFileFromHdfs(ctx context.Context, client *hdfs.Client, filePath string, blockCh chan<- FileDataBlock) {
	defer close(blockCh)
	f, err := client.Open(filePath)
	if err != nil {
		blockCh <- FileDataBlock{
			Err: err,
		}
		return
	}
	defer func() {
		_ = f.Close()
	}()

	bufReader := bufio.NewReader(f)
	buffer := make([]byte, HdfsServerConfig.BufferSize)

	for {
		_count, err := bufReader.Read(buffer)
		_data := make([]byte, _count)
		if _count != 0 {
			copy(_data, buffer[:_count])
		}

		if err != nil {
			logger.Warn().String("Read file data from HDFS failed", err.Error()).Fire()
			rErr := err
			if err == io.EOF {
				rErr = nil
			}

			_dataBlock := FileDataBlock{
				Data: _data,
				Err:  rErr,
			}
			select {
			case <-ctx.Done():
			case blockCh <- _dataBlock:
				logger.Debug().Int("upload file data byte count (Err occur)", len(_data)).Fire()
			}

			return
		}

		_block := FileDataBlock{
			Data: _data,
		}
		select {
		case <-ctx.Done():
			return
		case blockCh <- _block:
			logger.Debug().Int("upload file data byte count", len(_data)).Fire()
		}
	}
}

// try to download log file from baseServerURL (flink web restful)
// and upload file to destPrePath in HDFS
func UploadLogFile(baseServerURL, destPrePath string) (*logpb.UploadFileReply, error) {
	logger.Debug().Msg(fmt.Sprintf("try to Download file to store in [%s]", destPrePath)).Fire()
	// try to get log files from Flink web server
	tErr := uploadTaskManagerLogFile(baseServerURL, destPrePath)
	jErr := uploadJobManagerLogFile(baseServerURL, destPrePath)
	if tErr != nil {
		return nil, tErr
	}

	if jErr != nil {
		return nil, jErr
	}

	return &logpb.UploadFileReply{
		Status: logpb.TaskStatus_Started,
	}, nil

}

func uploadJobManagerLogFile(baseServerURL, destPrePath string) (err error) {
	apiURL := internal.GetJobManagerLogsURL(baseServerURL)
	fileToUpload, err := internal.SelectLogFileToUpload(logger, apiURL)
	if err != nil {
		logger.Error().Error("failed to select log file to Upload", err).Fire()
		return
	}

	fileName := fileToUpload.Name
	if fileName == "" {
		logger.Warn().Msg(fmt.Sprintf("no valid file found for [%s]", apiURL)).Fire()
		return
	}

	finalFileURL := internal.GetJobManagerLogFileURL(baseServerURL, fileName)
	finalDestPath := GetJobManagerFilePathInHDFS(destPrePath, fileName)
	go func() {
		_ = saveFile(finalFileURL, finalDestPath)
	}()

	return
}

func uploadTaskManagerLogFile(baseServerURL string, destPrePath string) (err error) {
	taskManagerIDs, err := internal.GetTaskManagerIDs(logger, baseServerURL)
	if err != nil {
		return
	}

	if taskManagerIDs == nil || len(taskManagerIDs) == 0 {
		logger.Warn().String("No valid TaskManagers found", baseServerURL).Fire()
		return
	}

	for _, _taskManagerID := range taskManagerIDs {
		apiURL := internal.GetTaskManagerLogsURL(baseServerURL, _taskManagerID)
		fileToUpload, err := internal.SelectLogFileToUpload(logger, apiURL)
		if err != nil {
			logger.Error().Error("failed to select log file to Upload", err).Fire()
			continue
		}

		fileName := fileToUpload.Name
		if fileName == "" {
			logger.Warn().Msg(fmt.Sprintf("no valid file found for [%s]", apiURL)).Fire()
			continue
		}

		finalFileURL := internal.GetTaskManagerLogFileURL(baseServerURL, _taskManagerID, fileName)
		finalDestPath := GetTaskManagerFilePathInHDFS(destPrePath, fileName, _taskManagerID)
		go func() {
			_ = saveFile(finalFileURL, finalDestPath)
		}()
	}

	return
}

func saveFile(fileURL, destFullPath string) (err error) {
	logger.Info().Msg(fmt.Sprintf("begin to save file from [%s] to [%s]", fileURL, destFullPath)).Fire()
	hdfsClient, err := internal.GetClient(HdfsServerConfig)
	if err != nil {
		logger.Error().Error("failed to create HDFS client", err).Fire()
		return
	}

	defer hdfsClient.Close()

	hdfsDirPath := path.Dir(destFullPath)
	err = hdfsClient.MkdirAll(hdfsDirPath, 0755)
	if err != nil {
		logger.Error().Msg(fmt.Sprintf("create dir [%s] on HDFS failed, [%s]", hdfsDirPath, err.Error())).Fire()
		return
	}

	hdfsWriter, err := hdfsClient.Create(destFullPath)
	if err != nil {
		if os.IsExist(err) {
			logger.Info().Msg(fmt.Sprintf("[%s] exist, try to remove and recreate it..", destFullPath)).Fire()
			err = hdfsClient.Remove(destFullPath)
			if err != nil {
				logger.Error().Msg(fmt.Sprintf("remove file [%s] failed", destFullPath)).Fire()
				return
			}

			hdfsWriter, err = hdfsClient.Create(destFullPath)
			if err != nil {
				logger.Error().Msg(fmt.Sprintf("recreate [%s] failed", destFullPath)).Fire()
				return
			}
			logger.Info().Msg(fmt.Sprintf("file [%s] recreated", destFullPath)).Fire()
		} else {
			logger.Error().Error("failed to open HDFS file", err).Fire()
			return
		}
	}

	defer hdfsWriter.Close()
	err = internal.DownloadSelectedFile(logger, fileURL, hdfsWriter)
	if err != nil {
		logger.Error().Msg(fmt.Sprintf("download file [%s] failed, %s", fileURL, err.Error())).Fire()
		return
	}

	_ = hdfsWriter.Flush()
	logger.Info().Msg(fmt.Sprintf("save file from [%s] to [%s] successfully!", fileURL, destFullPath)).Fire()
	return
}

func CheckUploadingTask(baseServerURL, destPrePath string) (*logpb.TaskStatReply, error) {
	logger.Debug().Msg(fmt.Sprintf("begin to check file [%s] Size", destPrePath)).Fire()
	jobManagerCompleted, err := CheckJobManagerLogFile(baseServerURL, destPrePath)
	if err != nil {
		return nil, err
	}

	if !jobManagerCompleted {
		return &logpb.TaskStatReply{Completed: false}, nil
	}

	taskManagerCompleted, err := CheckTaskManagerLogFiles(baseServerURL, destPrePath)
	if err != nil {
		return nil, err
	}

	if !taskManagerCompleted {
		return &logpb.TaskStatReply{Completed: false}, nil
	}

	return &logpb.TaskStatReply{Completed: true}, nil
}

func CheckJobManagerLogFile(baseServerURL, destPrePath string) (isCompleted bool, err error) {
	apiURL := internal.GetJobManagerLogsURL(baseServerURL)
	fileToUpload, err := internal.SelectLogFileToUpload(logger, apiURL)
	if err != nil {
		logger.Error().Error("failed to select log file to Upload", err).Fire()
		return
	}

	fileName := fileToUpload.Name
	if fileName == "" {
		logger.Warn().Msg(fmt.Sprintf("no valid file found for [%s]", apiURL)).Fire()
		return
	}

	finalDestPath := GetJobManagerFilePathInHDFS(destPrePath, fileName)
	isCompleted, err = compareFileSize(fileToUpload.Size, finalDestPath)
	return
}

func CheckTaskManagerLogFiles(baseServerURL, destPrePath string) (bool, error) {
	taskManagerIDs, err := internal.GetTaskManagerIDs(logger, baseServerURL)
	if err != nil {
		return false, err
	}

	for _, _taskManagerID := range taskManagerIDs {
		apiURL := internal.GetTaskManagerLogsURL(baseServerURL, _taskManagerID)
		fileToUpload, err := internal.SelectLogFileToUpload(logger, apiURL)
		if err != nil {
			logger.Error().Error("failed to select log file to Upload", err).Fire()
			return false, err
		}

		fileName := fileToUpload.Name
		if fileName == "" {
			logger.Warn().Msg(fmt.Sprintf("no valid file found for [%s]", apiURL)).Fire()
			return false, nil
		}

		finalDestPath := GetTaskManagerFilePathInHDFS(destPrePath, fileName, _taskManagerID)
		isCompleted, err := compareFileSize(fileToUpload.Size, finalDestPath)
		if err != nil || !isCompleted {
			return false, err
		}
	}

	return true, nil
}

func GetJobManagerFilePathInHDFS(destPreDirPath, fileName string) string {
	return fmt.Sprintf("%s/logs/jobmanager/%s", destPreDirPath, fileName)
}

func GetTaskManagerFilePathInHDFS(destPreDirPath, fileName, taskManagerID string) string {
	return fmt.Sprintf("%s/logs/taskmanager/%s/%s", destPreDirPath, taskManagerID, fileName)
}

func compareFileSize(srcFileSize int64, destFullPath string) (bool, error) {
	logger.Debug().Msg(fmt.Sprintf("try to get file size of [%s]", destFullPath)).Fire()
	hdfsClient, err := internal.GetClient(HdfsServerConfig)
	if err != nil {
		logger.Error().Error("failed to create HDFS client", err).Fire()
		return false, err
	}

	defer hdfsClient.Close()

	hdfsFileInfo, err := hdfsClient.Stat(destFullPath)
	if os.IsNotExist(err) {
		logger.Info().Msg(fmt.Sprintf("file [%s] not exits", destFullPath)).Fire()
		return false, nil
	}

	if err != nil {
		logger.Error().Error("failed to Stat HDFS file", err).Fire()
		return false, err
	}

	logger.Info().Msg(fmt.Sprintf("src file size [%d] destFile size [%d]", srcFileSize, hdfsFileInfo.Size()))
	if hdfsFileInfo.Size() == srcFileSize {
		return true, nil
	}
	return false, nil
}
