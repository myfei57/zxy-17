package dag

import (
	"taskflow/internal/task"
)

// Ready reports whether a task's dependencies all finished successfully.
func (g *Graph) Ready(store *task.Store, taskID string) (bool, error) {
	deps := g.Dependencies(taskID)
	g.mu.Lock()
	if g.snapshot == nil {
		g.snapshot = make(map[string]task.State)
	}
	snapshot := g.snapshot
	g.mu.Unlock()
	for _, depID := range deps {
		state, ok := snapshot[depID]
		if !ok {
			state, ok = task.StateOf(store, depID)
			if !ok {
				return false, nil
			}
			g.mu.Lock()
			g.snapshot[depID] = state
			g.mu.Unlock()
		}
		if state != task.Succeeded {
			return false, nil
		}
	}
	return true, nil
}
