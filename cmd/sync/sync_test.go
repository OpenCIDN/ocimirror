package main

import (
	"testing"
)

func TestNewCommand(t *testing.T) {
	cmd := NewCommand()
	
	// Check command name
	if cmd.Use != "sync" {
		t.Errorf("expected command use to be 'sync', got '%s'", cmd.Use)
	}
	
	// Check command has required flags
	requiredFlags := []string{"image", "storage-url"}
	for _, flagName := range requiredFlags {
		flag := cmd.Flags().Lookup(flagName)
		if flag == nil {
			t.Errorf("expected flag '%s' to exist", flagName)
			continue
		}
	}
	
	// Check optional flags exist
	optionalFlags := []string{"kubeconfig", "master", "user", "retry", "retry-interval", "timeout", "insecure-skip-tls-verify"}
	for _, flagName := range optionalFlags {
		flag := cmd.Flags().Lookup(flagName)
		if flag == nil {
			t.Errorf("expected flag '%s' to exist", flagName)
		}
	}
}
