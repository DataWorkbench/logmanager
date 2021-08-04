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
	res, err := client.GetJobLog(ctx, reqStreamData)
	fmt.Printf("reply for GetJobLog: [%v]", res)
	require.Nil(t, err, "%+v", err)

	stopCh := make(chan struct{})
	dataCh := make(chan struct{})
	go func() {
		time.Sleep(60 * time.Second)
		stopCh <- struct{}{}
	}()

	go func() {
		for {
			resp, err := res.Recv()
			require.Nil(t, err, "%+v", err)
			validCount := 0
			dataMap := resp.LogFileMap
			for _, value := range dataMap {
				validCount += len(value.LogEntries)
			}
			fmt.Printf("server pushed log data, count: %d", validCount)
			dataCh <- struct{}{}
		}
	}()

	for {
		select {
		case <-stopCh:
			fmt.Println("testing exits..")
			return
		case <-dataCh:
			fmt.Println("continue to wait for server stream data")
		}
	}

}
