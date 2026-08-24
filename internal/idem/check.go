// Package idem tracks idempotency keys for duplicate submission.
package idem

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Manager stores consumed idempotency keys.
type Manager struct {
	dir string
}

// NewManager creates an idempotency manager under dir.
func NewManager(dir string) *Manager {
	return &Manager{dir: dir}
}

func (m *Manager) path(key string) string {
	return filepath.Join(m.dir, key+".json")
}

// Check reports whether a key is already consumed.
func (m *Manager) Check(key string) (bool, error) {
	_, err := os.Stat(m.path(key))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Consume durably marks a key as used.
func (m *Manager) Consume(key string, taskID string) error {
	path := m.path(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(map[string]string{"key": key, "task": taskID})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ErrConsumed is returned when an idempotency key is already used.
var ErrConsumed = fmt.Errorf("idem: key already consumed")
