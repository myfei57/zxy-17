package scheduler

import (
	"fmt"

	"taskflow/internal/audit"
	"taskflow/internal/lease"
	"taskflow/internal/record"
	"taskflow/internal/task"
)

// Dispatch hands the next ready task to an executor.
func (s *Scheduler) Dispatch(auditLogger *audit.Logger, executor string) (*lease.Lease, error) {
	id, ok := s.pop()
	if !ok {
		return nil, nil
	}
	t, ok := s.store.Get(id)
	if !ok {
		return nil, fmt.Errorf("scheduler: unknown task %s", id)
	}
	if err := s.store.UpdateState(id, task.Running); err != nil {
		return nil, err
	}
	op, err := s.records.Append(record.Record{
		TaskID:    id,
		Namespace: t.Namespace,
		State:     task.Running,
		Executor:  executor,
	})
	if err != nil {
		return nil, err
	}
	if err := s.records.Commit(op.Seq); err != nil {
		return nil, err
	}
	gr, err := s.leases.Grant(id, executor)
	if err != nil {
		return nil, err
	}
	_ = auditLogger.Note("dispatch", t.Namespace, id, executor)
	return gr, nil
}
