package internal

import (
	"fmt"
	"github.com/DataWorkbench/logmanager/config"
	"github.com/colinmarc/hdfs/v2"
	"os"
	"strings"
)

func GetClient(hdfsConfig *config.HdfsConfig) (*hdfs.Client, error) {
	nameNodesAddr := strings.Split(hdfsConfig.Addresses, ",")
	options := hdfs.ClientOptions{
		Addresses:           nameNodesAddr,
		User:                hdfsConfig.UserName,
		UseDatanodeHostname: false,
	}
	client, err := hdfs.NewClient(options)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func StatFilesInDir(client *hdfs.Client, dirPath string) ([]os.FileInfo, error) {
	fileInfos, err := client.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	return fileInfos, nil
}

func StatFile(client *hdfs.Client, filePath string) (os.FileInfo, error) {
	fileInfo, err := client.Stat(filePath)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func GetHdfsDirPath(space_id, flow_id, inst_id, managerName string) string {
	return fmt.Sprintf("/%s/%s/%s/logs/%s", space_id, flow_id, inst_id, managerName)
}

func GetHdfsJobMgrFilePath(space_id, flow_id, inst_id, fileName string) string {
	return fmt.Sprintf("/%s/%s/%s/logs/jobmanager/%s", space_id, flow_id, inst_id, fileName)
}

func GetHdfsTaskMgrFilePath(space_id, flow_id, inst_id, taskManagerID, fileName string) string {
	return fmt.Sprintf("/%s/%s/%s/logs/taskmanager/%s/%s", space_id, flow_id, inst_id, taskManagerID, fileName)
}
