// Package quota limits the number of concurrently running tasks.
package quota

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Manager tracks the concurrency budget and appends a durable ledger.
type Manager struct {
	mu         sync.Mutex
	limit      int
	active     int
	ledgerPath string
}

// NewManager creates a concurrency quota manager with a durable ledger.
func NewManager(limit int, ledgerPath string) (*Manager, error) {
	if limit < 1 {
		return nil, fmt.Errorf("quota: limit must be positive")
	}
	if ledgerPath == "" {
		return nil, fmt.Errorf("quota: ledger path required")
	}
	if err := os.MkdirAll(filepath.Dir(ledgerPath), 0o755); err != nil {
		return nil, fmt.Errorf("quota: mkdir ledger: %w", err)
	}
	return &Manager{limit: limit, ledgerPath: ledgerPath}, nil
}

// Check rejects a new lease when the budget is exhausted.
func (m *Manager) Check() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active >= m.limit {
		return fmt.Errorf("quota: concurrency limit %d reached", m.limit)
	}
	return nil
}

func (m *Manager) append(kind string, active int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	line := fmt.Sprintf("%s %d %d\n", kind, active, time.Now().UnixNano())
	file, err := os.OpenFile(m.ledgerPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(line); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

// Acquire consumes one slot of the budget.
func (m *Manager) Acquire() error {
	m.mu.Lock()
	if m.active >= m.limit {
		m.mu.Unlock()
		return fmt.Errorf("quota: concurrency limit %d reached", m.limit)
	}
	m.active++
	active := m.active
	m.mu.Unlock()
	return m.append("acquire", active)
}

// Release returns one slot to the budget.
func (m *Manager) Release() error {
	m.mu.Lock()
	if m.active == 0 {
		m.mu.Unlock()
		return fmt.Errorf("quota: no active slot to release")
	}
	m.active--
	active := m.active
	m.mu.Unlock()
	return m.append("release", active)
}

// Active returns the number of active leases.
func (m *Manager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// Limit returns the configured limit.
func (m *Manager) Limit() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.limit
}
