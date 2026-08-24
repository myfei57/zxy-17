package cluster

import (
	"fmt"

	"taskflow/internal/idem"
	"taskflow/internal/record"
	"taskflow/internal/retry"
	"taskflow/internal/settings"
	"taskflow/internal/shard"
	"taskflow/internal/task"
)

// AddDependency records that from must finish before to starts.
func (c *Cluster) AddDependency(namespace string, from string, to string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	if err := rt.Graph.AddEdge(from, to); err != nil {
		return err
	}
	return c.Audit.Note("depends", namespace, to, from)
}

// Complete finishes a running task with the given terminal state and releases
// its lease and quota slot, then wakes any dependents whose deps are satisfied.
func (c *Cluster) Complete(namespace string, id string, executor string, stateName string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	state, err := terminalState(stateName)
	if err != nil {
		return err
	}
	current, ok := rt.Leases.Get(id)
	if !ok || current.Executor != executor {
		return fmt.Errorf("cluster: task %s not leased to %s", id, executor)
	}
	attempt := rt.Records.NextAttempt(id) - 1
	if attempt < 1 {
		attempt = 1
	}
	op, err := rt.Records.Append(record.Record{
		TaskID:    id,
		Namespace: namespace,
		Attempt:   attempt,
		State:     state,
		Executor:  executor,
	})
	if err != nil {
		return err
	}
	if err := rt.Records.Commit(op.Seq); err != nil {
		return err
	}
	if err := rt.Tasks.UpdateState(id, state); err != nil {
		return err
	}
	if err := rt.Leases.Release(id, executor); err != nil {
		return err
	}
	if err := rt.Quota.Release(); err != nil {
		return err
	}
	if err := c.Audit.Note("complete", namespace, id, string(state)); err != nil {
		return err
	}
	if state == task.Succeeded {
		return c.releaseDependents(rt, id)
	}
	return nil
}

func terminalState(name string) (task.State, error) {
	switch name {
	case string(task.Succeeded):
		return task.Succeeded, nil
	case string(task.Failed):
		return task.Failed, nil
	default:
		return "", fmt.Errorf("cluster: unsupported terminal state %s", name)
	}
}

func (c *Cluster) releaseDependents(rt *NamespaceRuntime, from string) error {
	for _, depID := range rt.Graph.Dependents(from) {
		ready, err := rt.Graph.Ready(rt.Tasks, depID)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		if err := rt.Tasks.UpdateState(depID, task.Ready); err != nil {
			return err
		}
		if err := rt.Scheduler.Enqueue(depID); err != nil {
			return err
		}
	}
	return nil
}

// Renew extends the lease of a running task after a durable heartbeat.
func (c *Cluster) Renew(namespace string, id string, executor string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	if err := rt.Leases.Renew(id, executor, rt.Records); err != nil {
		return err
	}
	return c.Audit.Note("renew", namespace, id, executor)
}

// Retry schedules the next attempt of a failed task, deduplicated by cursor.
func (c *Cluster) Retry(namespace string, id string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	t, ok := rt.Tasks.Get(id)
	if !ok {
		return fmt.Errorf("cluster: unknown task %s", id)
	}
	if t.State != task.Failed {
		return fmt.Errorf("cluster: task %s is not failed", id)
	}
	policy := retry.NewPolicy(c.Cfg.MaxAttempts)
	cursorPath := settings.CursorPath(c.Cfg.DataDir, namespace, id)
	next, err := retry.Next(rt.Records, id, policy, cursorPath)
	if err != nil {
		return err
	}
	if next == 0 {
		return fmt.Errorf("cluster: retry cap reached for %s", id)
	}
	op, err := rt.Records.Append(record.Record{
		TaskID:    id,
		Namespace: namespace,
		Attempt:   next,
		State:     task.Ready,
		Executor:  "",
	})
	if err != nil {
		return err
	}
	if err := rt.Records.Commit(op.Seq); err != nil {
		return err
	}
	if err := retry.Commit(cursorPath, next-1); err != nil {
		return err
	}
	if err := rt.Tasks.UpdateState(id, task.Ready); err != nil {
		return err
	}
	if err := rt.Scheduler.Enqueue(id); err != nil {
		return err
	}
	return c.Audit.Note("retry", namespace, id, fmt.Sprintf("attempt %d", next))
}

// SubmitShard durably records one shard result under an idempotency key, then
// advances the shard completion cursor. The result is committed before the
// cursor moves, so a crash cannot leave the cursor past a shard whose result
// was never written.
func (c *Cluster) SubmitShard(namespace string, id string, key string, shardNo int, total int, data string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	if _, ok := rt.Tasks.Get(id); !ok {
		return fmt.Errorf("cluster: unknown task %s", id)
	}
	consumed, err := rt.Idem.Check(key)
	if err != nil {
		return err
	}
	if consumed {
		return idem.ErrConsumed
	}
	sizes, err := shard.Split(total, total)
	if err != nil {
		return err
	}
	if shardNo < 0 || shardNo >= len(sizes) {
		return fmt.Errorf("cluster: shard %d out of range", shardNo)
	}
	result := record.ShardResult{
		TaskID: id,
		Shard:  shardNo,
		Total:  total,
		Data:   data,
	}
	cursorPath := settings.CursorPath(c.Cfg.DataDir, namespace, id+".shards")
	op, err := rt.Records.AppendShardResult(result)
	if err != nil {
		return err
	}
	if err := rt.Records.Commit(op.Seq); err != nil {
		return err
	}
	if err := shard.Complete(cursorPath, shardNo, result); err != nil {
		return err
	}
	if err := rt.Idem.Consume(key, id); err != nil {
		return err
	}
	return c.Audit.Note("shard", namespace, id, fmt.Sprintf("shard %d/%d", shardNo, total))
}
