package verifycase

import (
	"os"
	"path/filepath"
	"testing"

	"taskflow/internal/cluster"
	"taskflow/internal/settings"
	"taskflow/internal/task"
)

func TestLeaseReclaimKeepsTaskReschedulable(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "data", "leases", "ns-01.json")
	if err := os.MkdirAll(filepath.Dir(leasePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leasePath, []byte(`{"t1":{"task_id":"t1","executor":"exec","state":"granted","until":1}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cl, err := cluster.Build(settings.Settings{
		DataDir: filepath.Join(dir, "data"), NodeID: "node-a", Concurrency: 4, NamespaceCount: 1, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	if err := cl.CreateTask("ns-01", "t1", "task"); err != nil {
		t.Fatal(err)
	}
	rt := cl.Runtime["ns-01"]
	if err := rt.Tasks.UpdateState("t1", task.Running); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(leasePath); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(leasePath, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Reap("ns-01"); err == nil {
		t.Fatal("reap must fail when the reclaim cannot be persisted")
	}
	tsk, _ := rt.Tasks.Get("t1")
	if tsk.State == task.Ready {
		t.Fatal("task must not be re-ready before the reclaim is persisted")
	}
	if rt.Scheduler.ReadyCount() != 0 {
		t.Fatal("task must not be enqueued before the reclaim is persisted")
	}
}
