package main

import (
	"os"

	"github.com/OpenCIDN/cidn/pkg/cmd/apiserver"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
)

func main() {
	ctx := genericapiserver.SetupSignalContext()
	cmd := apiserver.NewServerCommand(ctx)
	code := cli.Run(cmd)
	os.Exit(code)
}
