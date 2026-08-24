package lease

import (
	"fmt"

	"taskflow/internal/record"
)

// Renew extends a lease after a heartbeat is durably recorded.
func (m *Manager) Renew(taskID string, executor string, records *record.Store) error {
	m.mu.Lock()
	existing, ok := m.leases[taskID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("lease: no lease for %s", taskID)
	}
	if existing.Executor != executor {
		m.mu.Unlock()
		return fmt.Errorf("lease: task %s leased to %s", taskID, existing.Executor)
	}
	m.mu.Unlock()

	m.mu.Lock()
	current, ok := m.leases[taskID]
	if !ok || current.Executor != executor {
		m.mu.Unlock()
		return fmt.Errorf("lease: task %s lease changed during renew", taskID)
	}
	current.Until = nowNanos() + m.ttlNano
	if err := m.save(); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	op, err := records.AppendHeartbeat(taskID, executor)
	if err != nil {
		return err
	}
	if err := records.Commit(op.Seq); err != nil {
		return err
	}
	return nil
}
