package cmds

import (
	"fmt"
	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/logmanager/config"
	"github.com/DataWorkbench/logmanager/server"
	"github.com/spf13/cobra"
	"os"
)

var (
	versionFlag bool
)

var root = &cobra.Command{
	Use:   "logmanager",
	Short: "DataWorkBench Log Manager",
	Long:  "DataWorkBench Log Manager",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if versionFlag {
			fmt.Println(buildinfo.MultiString)
		}
		_ = cmd.Help()
	},
}

var start = &cobra.Command{
	Use:   "start",
	Short: "Command to start server",
	Long:  "Command to start server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if err := server.Start(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "start server failed %v\n", err)
			os.Exit(1)
		}
	},
}

func Execute() {
	root.AddCommand(start)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	root.Flags().BoolVarP(
		&versionFlag, "version", "v", false, "show the version",
	)

	start.Flags().StringVarP(
		&config.FilePath, "config", "c", "", "path of config file",
	)
}
