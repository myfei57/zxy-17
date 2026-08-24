// Package lease manages executor leases for running tasks.
package lease

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// State is the lifecycle state of a lease.
type State string

const (
	Granted State = "granted"
	Expired State = "expired"
)

// Lease binds a task to an executor until it is renewed or expires.
type Lease struct {
	TaskID   string `json:"task_id"`
	Executor string `json:"executor"`
	State    State  `json:"state"`
	Until    int64  `json:"until"`
}

// Manager persists leases for a namespace.
type Manager struct {
	mu      sync.Mutex
	path    string
	leases  map[string]*Lease
	ttlNano int64
}

// NewManager creates a lease manager with the given ttl.
func NewManager(path string, ttlNano int64) *Manager {
	return &Manager{path: path, leases: make(map[string]*Lease), ttlNano: ttlNano}
}

// Load reads the persisted leases, if any.
func (m *Manager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("lease: read %s: %w", m.path, err)
	}
	var entries map[string]*Lease
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("lease: parse %s: %w", m.path, err)
	}
	m.leases = entries
	return nil
}

// Grant creates a lease for a task.
func (m *Manager) Grant(taskID string, executor string) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := nowNanos()
	lease := &Lease{TaskID: taskID, Executor: executor, State: Granted, Until: now + m.ttlNano}
	m.leases[taskID] = lease
	if err := m.save(); err != nil {
		return nil, err
	}
	return lease, nil
}

// Get returns the lease of a task.
func (m *Manager) Get(taskID string) (*Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[taskID]
	if !ok {
		return nil, false
	}
	copy := *l
	return &copy, true
}

// Release ends the lease of a task and persists the change.
func (m *Manager) Release(taskID string, executor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.leases[taskID]
	if !ok {
		return fmt.Errorf("lease: no lease for %s", taskID)
	}
	if existing.Executor != executor {
		return fmt.Errorf("lease: task %s leased to %s", taskID, existing.Executor)
	}
	delete(m.leases, taskID)
	return m.save()
}

func (m *Manager) save() error {
	if err := os.MkdirAll(filepath.Dir(m.path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(m.leases)
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}

func nowNanos() int64 {
	return timeNow()
}
