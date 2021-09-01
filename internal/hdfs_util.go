package internal

import (
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
