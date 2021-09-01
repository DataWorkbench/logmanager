package internal

import (
	"encoding/json"
	"fmt"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/glog"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type FileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type LogFileDir struct {
	Logs []FileInfo `json:"logs"`
}

// baseServerURL format [http://ip:port]
// e.g. "http://127.0.0.1:8081"
func GetTaskManagerIDs(logger *glog.Logger, baseServerURL string) ([]string, error) {
	apiURL := GetTaskManagersURL(baseServerURL)
	resp, err := http.Get(apiURL)
	if err != nil {
		logger.Error().Error("fail to get taskManagers info", err).Fire()
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error().Error("status for flink api", qerror.RequestForFlinkFailed.Format(apiURL)).Fire()
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error().Error("read response from flink api failed", err).Fire()
		return nil, err
	}

	var taskManagerInfo map[string]interface{}
	err = json.Unmarshal(body, &taskManagerInfo)
	if err != nil {
		logger.Error().Error("failed to Unmarshal Json", err).Fire()
		return nil, err
	}

	if taskManagerInfo["taskmanagers"] != nil {
		respTaskManagers := taskManagerInfo["taskmanagers"].([]interface{})
		taskManagerIDs := []string{}
		for _, info := range respTaskManagers {
			id := info.(map[string]interface{})["id"]
			taskManagerIDs = append(taskManagerIDs, id.(string))
		}
		return taskManagerIDs, nil
	}

	return []string{}, nil
}

// select the log to upload if there are many Rolling log files
func SelectLogFileToUpload(logger *glog.Logger, apiURL string) (file FileInfo, err error) {
	resp, err := http.Get(apiURL)
	if err != nil {
		logger.Error().Error("failed to query api", qerror.RequestForFlinkFailed.Format(apiURL)).Fire()
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error().Error("status for flink api", qerror.RequestForFlinkFailed.Format(apiURL)).Fire()
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error().Error("read flink restful api resp data failed", err).Fire()
		return
	}

	var logsInDir LogFileDir
	err = json.Unmarshal(body, &logsInDir)
	if err != nil {
		logger.Error().Error("parse flink api resp.Body failed", err).Fire()
		return
	}

	tmpJobManagerLogFiles := []FileInfo{}
	for _, fileInfo := range logsInDir.Logs {
		logger.Info().Msg(fmt.Sprintf("Got LogFileName [%s] Size [%d]", fileInfo.Name, fileInfo.Size)).Fire()
		tmpJobManagerLogFiles = append(tmpJobManagerLogFiles, fileInfo)
	}

	return selectLogFile(tmpJobManagerLogFiles), nil
}

// Now we select the latest log file
func selectLogFile(files []FileInfo) FileInfo {
	for _, file := range files {
		if strings.HasSuffix(file.Name, "log") {
			return file
		}
	}
	return FileInfo{}
}

func DownloadSelectedFile(logger *glog.Logger, fileURL string, writer io.Writer) (err error) {
	downloadResp, err := http.Get(fileURL)
	if err != nil {
		logger.Error().Error("failed to dowload log file from flink", err).Fire()
		return
	}

	defer downloadResp.Body.Close()

	writtenCount, err := io.Copy(writer, downloadResp.Body)
	if err != nil {
		logger.Error().Error("write data to HDFS file failed", err).Fire()
		return
	}

	logger.Info().Msg(fmt.Sprintf("file [%s] had written [%d] bytes into hdfs", writtenCount, writtenCount)).Fire()
	return
}

func GetTaskManagersURL(baseServerURL string) string {
	return fmt.Sprintf("%s/taskmanagers", baseServerURL)
}

func GetTaskManagerLogsURL(baseServerURL, taskManagerID string) string {
	return fmt.Sprintf("%s/taskmanagers/%s/logs", baseServerURL, taskManagerID)
}

func GetTaskManagerLogFileURL(baseServerURL, taskManagerID, fileName string) string {
	return fmt.Sprintf("%s/taskmanagers/%s/logs/%s", baseServerURL, taskManagerID, fileName)
}

func GetJobManagerLogsURL(baseServerURL string) string {
	return fmt.Sprintf("%s/jobmanager/logs", baseServerURL)
}

func GetJobManagerLogFileURL(baseServerURL, fileName string) string {
	return fmt.Sprintf("%s/jobmanager/logs/%s", baseServerURL, fileName)
}
