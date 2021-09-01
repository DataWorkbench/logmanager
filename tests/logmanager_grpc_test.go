package tests

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/gtrace"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUploadLogFiles(t *testing.T) {
	address := "127.0.0.1:11001"
	lp := glog.NewDefault()
	ctx := glog.WithContext(context.Background(), lp)
	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address: address,
	})

	grpcwrap.SetLogger(lp, &grpcwrap.LogConfig{
		Level:     2,
		Verbosity: 99,
	})

	require.Nil(t, err, "%+v", err)

	client := logpb.NewLogManagerClient(conn)
	logger := glog.NewDefault()
	worker := idgenerator.New("")
	reqId, _ := worker.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)

	ctx = gtrace.ContextWithId(ctx, reqId)
	ctx = glog.WithContext(ctx, ln)

	_, err = client.UploadLogFile(ctx, &logpb.UploadFileRequest{
		SpaceId:    "TestSpaceID",
		FlowId:     "TestFlowID",
		InstanceId: "InstanceID",
		ServerUrl:  "http://139.198.28.249:8081",
	})

	require.Nil(t, err, "%+v", err)
}

func TestCheckUploadingState(t *testing.T) {
	address := "127.0.0.1:11001"
	lp := glog.NewDefault()
	ctx := glog.WithContext(context.Background(), lp)
	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address: address,
	})

	grpcwrap.SetLogger(lp, &grpcwrap.LogConfig{
		Level:     2,
		Verbosity: 99,
	})

	require.Nil(t, err, "%+v", err)

	client := logpb.NewLogManagerClient(conn)
	logger := glog.NewDefault()
	worker := idgenerator.New("")
	reqId, _ := worker.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)

	ctx = gtrace.ContextWithId(ctx, reqId)
	ctx = glog.WithContext(ctx, ln)

	resp, err := client.GetUploadingTaskStat(ctx, &logpb.TaskStatRequest{
		SpaceId:    "TestSpaceID",
		FlowId:     "TestFlowID",
		InstanceId: "InstanceID",
		ServerUrl:  "http://139.198.28.249:8081",
	})

	fmt.Println(resp)
	require.Nil(t, err, "%+v", err)
}
