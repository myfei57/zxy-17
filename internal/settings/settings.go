// Package settings defines the runtime configuration of a TaskFlow process.
package settings

import (
	"fmt"
	"os"
	"path/filepath"
)

// Settings holds the values needed to start one TaskFlow process.
type Settings struct {
	HTTPAddr       string
	DataDir        string
	NodeID         string
	Concurrency    int
	NamespaceCount int
	MaxAttempts    int
}

// Default returns a development-friendly settings object.
func Default() Settings {
	return Settings{
		HTTPAddr:       "127.0.0.1:7791",
		DataDir:        filepath.Join(os.TempDir(), "taskflow"),
		NodeID:         "node-a",
		Concurrency:    16,
		NamespaceCount: 4,
		MaxAttempts:    3,
	}
}

// Validate checks that the settings can produce a runnable cluster.
func (s Settings) Validate() error {
	if s.DataDir == "" {
		return fmt.Errorf("taskflow: data dir is required")
	}
	if s.NodeID == "" {
		return fmt.Errorf("taskflow: node id is required")
	}
	if s.Concurrency < 1 {
		return fmt.Errorf("taskflow: concurrency must be positive")
	}
	if s.NamespaceCount < 1 {
		return fmt.Errorf("taskflow: namespace count must be positive")
	}
	if s.MaxAttempts < 1 {
		return fmt.Errorf("taskflow: max attempts must be positive")
	}
	return nil
}
