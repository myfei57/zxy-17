package cluster

import (
	"fmt"

	"taskflow/internal/audit"
	"taskflow/internal/console"
	"taskflow/internal/lease"
	"taskflow/internal/settings"
	"taskflow/internal/task"
)

// Namespaces lists the namespace registry for the console.
func (c *Cluster) Namespaces() []console.NamespaceInfo {
	var out []console.NamespaceInfo
	for _, id := range c.NS.IDs() {
		n, _ := c.NS.Get(id)
		out = append(out, console.NamespaceInfo{ID: n.ID, Name: n.Name, Owner: n.Owner})
	}
	return out
}

// Tasks lists the tasks of a namespace.
func (c *Cluster) Tasks(namespace string) []console.TaskInfo {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return nil
	}
	var out []console.TaskInfo
	for _, t := range rt.Tasks.All(namespace) {
		out = append(out, console.TaskInfo{ID: t.ID, Name: t.Name, State: string(t.State), Attempts: lastAttempt(rt, t.ID)})
	}
	return out
}

// Task returns one task.
func (c *Cluster) Task(namespace string, id string) (console.TaskInfo, bool) {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return console.TaskInfo{}, false
	}
	t, ok := rt.Tasks.Get(id)
	if !ok {
		return console.TaskInfo{}, false
	}
	return console.TaskInfo{ID: t.ID, Name: t.Name, State: string(t.State), Attempts: lastAttempt(rt, t.ID)}, true
}

func lastAttempt(rt *NamespaceRuntime, taskID string) int {
	n := rt.Records.NextAttempt(taskID)
	if n < 1 {
		return 0
	}
	return n - 1
}

// CreateTask registers a task definition.
func (c *Cluster) CreateTask(namespace string, id string, name string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	op, err := rt.Tasks.Create(&task.Task{ID: id, Namespace: namespace, Name: name, State: task.Pending})
	if err != nil {
		return err
	}
	if err := rt.Tasks.Commit(op.Seq); err != nil {
		return err
	}
	return c.Audit.Note("create", namespace, id, name)
}

// MarkReady moves a pending task into the ready queue.
func (c *Cluster) MarkReady(namespace string, id string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	if err := rt.Scheduler.MarkReady(id); err != nil {
		return err
	}
	return rt.Scheduler.Enqueue(id)
}

// Dispatch hands the next ready task to an executor.
func (c *Cluster) Dispatch(namespace string, executor string) error {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	if err := rt.Quota.Check(); err != nil {
		return err
	}
	gr, err := rt.Scheduler.Dispatch(c.Audit, executor)
	if err != nil {
		return err
	}
	if gr == nil {
		return nil
	}
	return rt.Quota.Acquire()
}

// ReadyCount returns the number of queued tasks.
func (c *Cluster) ReadyCount(namespace string) int {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return 0
	}
	return rt.Scheduler.ReadyCount()
}

// Leases lists the leases of a namespace.
func (c *Cluster) Leases(namespace string) []console.LeaseInfo {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return nil
	}
	var out []console.LeaseInfo
	for _, t := range rt.Tasks.All(namespace) {
		if l, ok := rt.Leases.Get(t.ID); ok {
			out = append(out, console.LeaseInfo{TaskID: l.TaskID, Executor: l.Executor, State: string(l.State), Until: l.Until})
		}
	}
	return out
}

// Reap reclaims expired leases and returns their count.
func (c *Cluster) Reap(namespace string) (int, error) {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return 0, fmt.Errorf("cluster: unknown namespace %s", namespace)
	}
	reclaimed, err := rt.Leases.Reap(nowNanos())
	if err != nil {
		return 0, err
	}
	if err := lease.ReleaseReclaimed(rt.Tasks, reclaimed); err != nil {
		return 0, err
	}
	for range reclaimed {
		if err := rt.Quota.Release(); err != nil {
			return 0, err
		}
	}
	for _, id := range reclaimed {
		if err := rt.Scheduler.Enqueue(id); err != nil {
			return 0, err
		}
	}
	if err := rt.Leases.PersistReclaim(); err != nil {
		return 0, err
	}
	_ = c.Audit.Note("reap", namespace, "", fmt.Sprintf("%d reclaimed", len(reclaimed)))
	return len(reclaimed), nil
}

// Records lists the execution records of a namespace.
func (c *Cluster) Records(namespace string) []console.RecordInfo {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return nil
	}
	// Execution records are replayed from the journal by the store; expose the
	// attempt counters as a lightweight record view for the console.
	var out []console.RecordInfo
	for _, t := range rt.Tasks.All(namespace) {
		last := lastAttempt(rt, t.ID)
		for attempt := 1; attempt <= last; attempt++ {
			out = append(out, console.RecordInfo{TaskID: t.ID, Attempt: attempt, State: string(t.State), Executor: ""})
		}
	}
	return out
}

// AuditEntries returns the newest audit events.
func (c *Cluster) AuditEntries(limit int) []audit.Event {
	return c.Audit.Entries(limit)
}

// AuditCount returns the number of events recorded in this process.
func (c *Cluster) AuditCount() int {
	return c.Audit.Count()
}

// AuditCounts returns event counts grouped by type.
func (c *Cluster) AuditCounts() map[string]int {
	return c.Audit.Counts()
}

// AuditEntriesByType returns audit events of one type.
func (c *Cluster) AuditEntriesByType(kind string) []audit.Event {
	return c.Audit.FilterByType(kind)
}

// Quota returns the concurrency budget of a namespace.
func (c *Cluster) Quota(namespace string) console.QuotaInfo {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return console.QuotaInfo{}
	}
	return console.QuotaInfo{Limit: rt.Quota.Limit(), Active: rt.Quota.Active()}
}

// TaskCount returns the number of tasks defined in a namespace.
func (c *Cluster) TaskCount(namespace string) int {
	rt, ok := c.Runtime[namespace]
	if !ok {
		return 0
	}
	return rt.Tasks.Count()
}

// RecordDir returns the record directory of a namespace.
func (c *Cluster) RecordDir(namespace string) string {
	return settings.RecordDir(c.Cfg.DataDir, namespace)
}

// RenameNamespace changes the display name of a namespace.
func (c *Cluster) RenameNamespace(id string, name string) error {
	if err := c.NS.Rename(id, name); err != nil {
		return err
	}
	return c.Audit.Note("rename", id, "", name)
}

// TransferNamespace moves a namespace to another owner.
func (c *Cluster) TransferNamespace(id string, owner string) error {
	if err := c.NS.Transfer(id, owner); err != nil {
		return err
	}
	return c.Audit.Note("transfer", id, "", owner)
}

// NamespaceOwner returns the owning node of a namespace.
func (c *Cluster) NamespaceOwner(id string) (string, bool) {
	return c.NS.OwnerOf(id)
}

func nowNanos() int64 {
	return leaseNow()
}
