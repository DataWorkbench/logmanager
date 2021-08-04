module github.com/DataWorkbench/logmanager

go 1.15

require (
	github.com/DataWorkbench/common v0.0.0-20210804081408-9cafedee3de6
	github.com/DataWorkbench/glog v0.0.0-20210809050640-4960fd6de6ab
	github.com/DataWorkbench/gproto v0.0.0-20210811093804-86762ac04a76
	github.com/DataWorkbench/loader v0.0.0-20201119073611-6f210eb11a8c
	github.com/go-playground/validator/v10 v10.7.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/olivere/elastic/v7 v7.0.26
	github.com/prometheus/common v0.29.0 // indirect
	github.com/prometheus/procfs v0.7.1 // indirect
	github.com/segmentio/kafka-go v0.4.17 // indirect
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/net v0.0.0-20210716203947-853a461950ff // indirect
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	google.golang.org/genproto v0.0.0-20210722135532-667f2b7c528f // indirect
	google.golang.org/grpc v1.39.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

replace github.com/DataWorkbench/common => ../common

replace github.com/DataWorkbench/logmanager => ../logmanager

replace github.com/DataWorkbench/gproto => ../gproto
