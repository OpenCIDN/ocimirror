package main

import (
	"testing"
)

func TestNewCommand(t *testing.T) {
	cmd := NewCommand()
	if cmd == nil {
		t.Fatal("NewCommand returned nil")
	}

	if cmd.Use != "sync" {
		t.Errorf("Expected command use to be 'sync', got '%s'", cmd.Use)
	}

	if cmd.Short == "" {
		t.Error("Command short description is empty")
	}

	// Check required flags are defined
	flags := cmd.Flags()
	
	imagesFlag := flags.Lookup("images")
	if imagesFlag == nil {
		t.Error("Expected --images flag to be defined")
	}

	storageURLFlag := flags.Lookup("storage-url")
	if storageURLFlag == nil {
		t.Error("Expected --storage-url flag to be defined")
	}

	kubeconfigFlag := flags.Lookup("kubeconfig")
	if kubeconfigFlag == nil {
		t.Error("Expected --kubeconfig flag to be defined")
	}

	destinationFlag := flags.Lookup("destination")
	if destinationFlag == nil {
		t.Error("Expected --destination flag to be defined")
	}

	userFlag := flags.Lookup("user")
	if userFlag == nil {
		t.Error("Expected --user flag to be defined")
	}
}
