package server

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/gtrace"
	"github.com/DataWorkbench/gproto/pkg/logpb"
	"google.golang.org/grpc"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/glog"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/metrics"

	"github.com/DataWorkbench/logmanager/config"
	"github.com/DataWorkbench/logmanager/handler"
)

// Start for start the http server
func Start() (err error) {
	fmt.Printf("%s pid=%d program_build_info: %s\n",
		time.Now().Format(time.RFC3339Nano), os.Getpid(), buildinfo.JSONString)

	var cfg *config.Config

	cfg, err = config.Load()
	if err != nil {
		return
	}

	// init parent logger
	lp := glog.NewDefault().WithLevel(glog.Level(cfg.LogLevel))
	ctx := glog.WithContext(context.Background(), lp)

	var (
		rpcServer    *grpcwrap.Server
		metricServer *metrics.Server
		tracer       gtrace.Tracer
		tracerCloser io.Closer
	)

	defer func() {
		rpcServer.GracefulStop()
		_ = metricServer.Shutdown(ctx)
		if tracerCloser != nil {
			_ = tracerCloser.Close()
		}
		_ = lp.Close()
	}()

	tracer, tracerCloser, err = gtrace.New(cfg.Tracer)
	if err != nil {
		return
	}
	ctx = gtrace.ContextWithTracer(ctx, tracer)

	// init grpc.Server
	grpcwrap.SetLogger(lp, cfg.GRPCLog)
	rpcServer, err = grpcwrap.NewServer(ctx, cfg.GRPCServer)
	if err != nil {
		return
	}

	// Init handler.
	handler.Init(
		handler.WithLogger(lp),
		handler.WithHdfsConfig(cfg.HdfsServer),
	)

	// Register rpc server.
	rpcServer.Register(func(s *grpc.Server) {
		logpb.RegisterLogManagerServer(s, &LogManagerServer{})
	})

	// handle signal
	sigGroup := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM}
	sigChan := make(chan os.Signal, len(sigGroup))
	signal.Notify(sigChan, sigGroup...)

	blockChan := make(chan struct{})

	// run grpc server
	go func() {
		err = rpcServer.ListenAndServe()
		blockChan <- struct{}{}
	}()

	// init prometheus server
	metricServer, err = metrics.NewServer(ctx, cfg.MetricsServer)
	if err != nil {
		return err
	}

	go func() {
		if err := metricServer.ListenAndServe(); err != nil {
			return
		}
	}()

	go func() {
		sig := <-sigChan
		lp.Info().String("receive system signal", sig.String()).Fire()
		blockChan <- struct{}{}
	}()

	<-blockChan
	return
}
