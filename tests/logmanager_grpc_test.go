package tests

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPushLogStream(t *testing.T) {
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
	worker := idgenerator.New("")
	reqId, _ := worker.Take()

	ln := lp.Clone()
	ln.WithFields().AddString("rid", reqId)
	ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)
	reqStreamData := &logpb.GetJobLogRequest{
		FlowId:         "0804_test_topic",
		InstanceId:     "myInstanceID",
		LogFileName:    "",
		CustomGroupId:  "my_name_7",
		OffsetsInitial: -2,
		BatchMax:       512,
	}

	ctx, cancelFunc := context.WithCancel(ctx)
	res, err := client.GetJobLog(ctx, reqStreamData)
	require.Nil(t, err, "%+v", err)

	stopCh := make(chan struct{})
	go func() {
		for {
			resp, err := res.Recv()
			if err != nil {
				fmt.Printf("Recv err %s", err.Error())
				break
			}
			validCount := 0
			dataMap := resp.LogFileMap
			for _, value := range dataMap {
				validCount += len(value.LogEntries)
			}
			fmt.Printf("server pushed log data, count: %d\n", validCount)
		}
		stopCh <- struct{}{}
	}()

	time.Sleep(10 * time.Second)
	cancelFunc()
	<-stopCh
	fmt.Println("testing exits..")
}
