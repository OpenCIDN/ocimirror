package main

import (
	"os"

	"github.com/OpenCIDN/cidn/pkg/cmd/runner"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/klog/v2"
)

func main() {
	ctx := genericapiserver.SetupSignalContext()
	cmd := runner.NewRunnerCommand(ctx)
	err := cli.RunNoErrOutput(cmd)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}
}
