// Package scheduler dispatches ready tasks to executors.
package scheduler

import (
	"fmt"
	"sync"

	"taskflow/internal/dag"
	"taskflow/internal/lease"
	"taskflow/internal/quota"
	"taskflow/internal/record"
	"taskflow/internal/task"
)

// Scheduler owns the ready queue of a namespace.
type Scheduler struct {
	mu      sync.Mutex
	store   *task.Store
	graph   *dag.Graph
	records *record.Store
	leases  *lease.Manager
	quota   *quota.Manager
	ready   []string
}

// New creates a scheduler for a namespace.
func New(store *task.Store, graph *dag.Graph, records *record.Store, leases *lease.Manager, quota *quota.Manager) *Scheduler {
	return &Scheduler{store: store, graph: graph, records: records, leases: leases, quota: quota}
}

// Enqueue adds a task id to the ready queue.
func (s *Scheduler) Enqueue(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = append(s.ready, taskID)
	return nil
}

// ReadyCount returns the number of queued tasks.
func (s *Scheduler) ReadyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ready)
}

func (s *Scheduler) pop() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ready) == 0 {
		return "", false
	}
	id := s.ready[0]
	s.ready = s.ready[1:]
	return id, true
}

// MarkReady moves a pending task into the ready state.
func (s *Scheduler) MarkReady(id string) error {
	t, ok := s.store.Get(id)
	if !ok {
		return fmt.Errorf("scheduler: unknown task %s", id)
	}
	if t.State != task.Pending {
		return nil
	}
	ready, err := s.graph.Ready(s.store, id)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("scheduler: task %s dependencies not satisfied", id)
	}
	return s.store.UpdateState(id, task.Ready)
}
