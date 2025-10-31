package main

import (
	"log/slog"
	"os"

	"github.com/OpenCIDN/ocimirror/internal/signals"
	"github.com/spf13/cobra"
)

func main() {
	ctx := signals.SetupSignalContext()
	err := NewCommand().ExecuteContext(ctx)
	if err != nil {
		slog.Error("execute failed", "error", err)
		os.Exit(1)
	}
}

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ocimirror",
		Short: "OCI image registry mirror and proxy",
		Long:  `ocimirror is an OCI (Open Container Initiative) image registry mirror and proxy, providing a transparent caching layer for container images.`,
	}

	cmd.AddCommand(NewGatewayCommand())
	cmd.AddCommand(NewAgentCommand())
	cmd.AddCommand(NewAuthCommand())

	return cmd
}
