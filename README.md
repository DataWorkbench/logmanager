# DataWorkbench 开源项目结构规范 Golang 语言版本

本项目是 DataWorkbench 开源项目结构规范的 Golang 语言版本。

## 使用方式

- `git clone` 到本地后修改
- 使用 [`Use this template`](https://github.com/DataWorkbench/project-layout-golang/generate) 直接创建项目

## 规范

- 必须使用 Go Modules，不得存在 vendor 目录
- 必须将 `go.mod` 和 `go.sum` 加入到版本管理工具
- 默认使用 [Apache 2.0 许可证](https://www.apache.org/licenses/LICENSE-2.0)，使用其他许可证的项目需使用正确的 LICENSE 文本

## 结构

- cmd: 项目构建出来供用户使用的二进制
- docs: 项目设计文档和用户文档
- internal: 私有应用和库代码
  - internal/cmd: 仅供内部开发使用的二进制，包括代码生成工具等
- pkg: 公开的库代码，相对独立，与本项目无强耦合逻辑，可被其他应用导入
- test: 外部测试应用和测试数据

## 样例

### 简单的库

```
.
├── _lib_.go
├── _lib_test_.go
├── CHANGELOG.md
├── go.mod
├── go.sum
├── LICENSE
├── Makefile
└── README.md
```

一个比较简单的不分 package 的库可以没有 `cmd` 等目录

### 复杂的库

```
.
├── docs
│   └── README.md
├── pkg
│   └── README.md
├── test
│   └── README.md
├── _package_a_
├── _package_b_
├── CHANGELOG.md
├── go.mod
├── LICENSE
├── Makefile
└── README.md
```

复杂的库的包需要直接放在根目录下，根据实际情况决定是否需要 `pkg` 和 `test` 目录

### 应用

```
.
├── cmd
│   ├── _app_name_
│   │   └── main.go
│   └── README.md
├── docs
│   └── README.md
├── internal
│   ├── cmd
│   │   └── _tool_name_
│   │       └── main.go
│   ├── pkg
│   └── README.md
├── pkg
│   └── README.md
├── test
│   └── README.md
├── CHANGELOG.md
├── go.mod
├── LICENSE
├── Makefile
└── README.md
```

应用应当将二进制放在 `cmd` 目录下，仅内部使用的组件应放在 `internal/cmd` 下。
