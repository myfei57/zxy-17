package lease

import (
	"fmt"

	"taskflow/internal/task"
)

// Reap marks expired leases and returns the reclaimed task ids.
func (m *Manager) Reap(now int64) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var reclaimed []string
	for taskID, lease := range m.leases {
		if lease.IsExpired(now) {
			lease.State = Expired
			reclaimed = append(reclaimed, taskID)
		}
	}
	return reclaimed, nil
}

// PersistReclaim persists the reclaimed lease states.
func (m *Manager) PersistReclaim() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.save()
}

// ReleaseReclaimed hands reclaimed tasks back to the ready state.
func ReleaseReclaimed(store *task.Store, reclaimed []string) error {
	for _, id := range reclaimed {
		if err := store.UpdateState(id, task.Ready); err != nil {
			return fmt.Errorf("lease: re-ready %s: %w", id, err)
		}
	}
	return nil
}
