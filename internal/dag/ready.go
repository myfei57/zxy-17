package dag

import (
	"taskflow/internal/task"
)

// Ready reports whether a task's dependencies all finished successfully.
func (g *Graph) Ready(store *task.Store, taskID string) (bool, error) {
	deps := g.Dependencies(taskID)
	for _, depID := range deps {
		state, ok := task.StateOf(store, depID)
		if !ok {
			return false, nil
		}
		if state != task.Succeeded {
			return false, nil
		}
	}
	return true, nil
}
