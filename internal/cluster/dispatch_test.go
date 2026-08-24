package cluster

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"taskflow/internal/settings"
	"taskflow/internal/task"
)

// newTestCluster builds a single-namespace cluster with a small concurrency
// budget so quota pressure is easy to produce.
func newTestCluster(t *testing.T, concurrency int) *Cluster {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "taskflow")
	cfg := settings.Settings{
		HTTPAddr:       "127.0.0.1:0",
		DataDir:        dir,
		NodeID:         "test-node",
		Concurrency:    concurrency,
		NamespaceCount: 1,
		MaxAttempts:    3,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// firstNamespace returns the single namespace id of a test cluster.
func firstNamespace(c *Cluster) string {
	ids := c.NS.IDs()
	if len(ids) != 1 {
		panic(fmt.Sprintf("expected 1 namespace, got %d", len(ids)))
	}
	return ids[0]
}

// createReadyTask registers a pending task and moves it into the ready queue.
func createReadyTask(t *testing.T, c *Cluster, namespace, id string) {
	t.Helper()
	if err := c.CreateTask(namespace, id, id); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	if err := c.MarkReady(namespace, id); err != nil {
		t.Fatalf("markready %s: %v", id, err)
	}
}

// TestDispatchRespectsQuota drives more dispatches than the concurrency limit
// and asserts that no more leases than the limit are ever outstanding. Under
// the original (buggy) ordering the lease was granted before the quota was
// checked, so an over-budget dispatch still issued a lease.
func TestDispatchRespectsQuota(t *testing.T) {
	const limit = 4
	const tasks = limit + 8 // far more ready tasks than slots

	c := newTestCluster(t, limit)
	ns := firstNamespace(c)

	for i := 0; i < tasks; i++ {
		createReadyTask(t, c, ns, fmt.Sprintf("t-%02d", i))
	}

	rt := c.Runtime[ns]
	// Track the high-water mark of concurrent leases.
	var inFlight int32
	var maxInFlight int32

	var wg sync.WaitGroup
	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			executor := fmt.Sprintf("exec-%d", i)
			for {
				// Each goroutine keeps dispatching until it gets no task.
				leased, err := tryDispatch(t, c, ns, executor)
				if err != nil || !leased {
					return
				}
				cur := atomic.AddInt32(&inFlight, 1)
				for {
					old := atomic.LoadInt32(&maxInFlight)
					if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
						break
					}
				}
				// Simulate completion so the slot recovers; with the buggy
				// ordering some leases were never matched by a quota slot, so
				// Complete's Release would underflow.
				taskID := leasedTask(t, c, ns, executor)
				if err := c.Complete(ns, taskID, executor, string(task.Succeeded)); err != nil {
					t.Errorf("complete %s: %v", taskID, err)
					return
				}
				atomic.AddInt32(&inFlight, -1)
			}
		}(i)
	}
	wg.Wait()

	if got := int(atomic.LoadInt32(&maxInFlight)); got > limit {
		t.Fatalf("concurrency exceeded limit: max %d > limit %d", got, limit)
	}
	if got := rt.Quota.Active(); got != 0 {
		t.Fatalf("quota not drained after all tasks completed: active=%d", got)
	}
}

// tryDispatch attempts one dispatch and reports whether a lease was issued.
func tryDispatch(t *testing.T, c *Cluster, ns, executor string) (bool, error) {
	t.Helper()
	before := c.ReadyCount(ns)
	err := c.Dispatch(ns, executor)
	after := c.ReadyCount(ns)
	leased := after < before
	if err != nil && !errors.Is(err, nil) {
		// Quota-exhausted is an expected, non-fatal outcome here: it is the
		// signal that the guard fired before a lease was granted.
		if after > before {
			t.Fatalf("dispatch failed but ready queue grew: %v", err)
		}
		return false, err
	}
	return leased, nil
}

// leasedTask returns the task id currently leased to executor.
func leasedTask(t *testing.T, c *Cluster, ns, executor string) string {
	t.Helper()
	for _, l := range c.Leases(ns) {
		if l.Executor == executor {
			return l.TaskID
		}
	}
	t.Fatalf("no lease found for executor %s", executor)
	return ""
}

// TestDispatchOverBudgetNoLease asserts the core fix directly: when the
// concurrency budget is exhausted, Dispatch returns an error and, crucially,
// does not issue a lease or take the task out of the ready queue.
func TestDispatchOverBudgetNoLease(t *testing.T) {
	const limit = 1
	c := newTestCluster(t, limit)
	ns := firstNamespace(c)

	// Two ready tasks, only one slot.
	createReadyTask(t, c, ns, "t-a")
	createReadyTask(t, c, ns, "t-b")

	// First dispatch consumes the single slot.
	if err := c.Dispatch(ns, "exec-1"); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if got := c.Quota(ns).Active; got != limit {
		t.Fatalf("after first dispatch active=%d, want %d", got, limit)
	}
	leases := c.Leases(ns)
	if len(leases) != 1 {
		t.Fatalf("expected 1 lease after first dispatch, got %d", len(leases))
	}

	// Second dispatch must be rejected because the budget is exhausted.
	err := c.Dispatch(ns, "exec-2")
	if err == nil {
		t.Fatal("second dispatch succeeded despite exhausted quota")
	}

	// No second lease may have been issued.
	if got := len(c.Leases(ns)); got != 1 {
		t.Fatalf("quota rejection still issued a lease: got %d leases, want 1", got)
	}
	// The second task must still be ready (not moved to running) and still
	// queued for a later dispatch.
	tb, ok := c.Task(ns, "t-b")
	if !ok {
		t.Fatal("task t-b missing")
	}
	if tb.State != string(task.Ready) {
		t.Fatalf("task t-b state=%q, want %q (must remain ready)", tb.State, task.Ready)
	}
	if got := c.ReadyCount(ns); got != 1 {
		t.Fatalf("ready count=%d, want 1 (t-b must remain queued)", got)
	}
}
