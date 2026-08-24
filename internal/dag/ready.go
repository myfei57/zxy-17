package dag

import (
	"taskflow/internal/task"
)

// Ready reports whether a task's dependencies all finished successfully.
//
// It reads the current state of each dependency from the store on every call
// rather than trusting a snapshot taken earlier. A dependency may transition
// to Succeeded between calls (most notably when an upstream completes and
// wakes its dependents), so a cached state would either block a now-ready
// downstream or release it before the upstream's result has settled. Reading
// live state ensures a downstream is dispatched only once its upstreams have
// actually reached Succeeded.
func (g *Graph) Ready(store *task.Store, taskID string) (bool, error) {
	for _, depID := range g.Dependencies(taskID) {
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
