package verifycase

import (
	"path/filepath"
	"testing"

	"taskflow/internal/cluster"
	"taskflow/internal/settings"
	"taskflow/internal/task"
)

func TestQuotaRejectsBeforeLeaseGrant(t *testing.T) {
	dir := t.TempDir()
	cl, err := cluster.Build(settings.Settings{
		DataDir: filepath.Join(dir, "data"), NodeID: "node-a", Concurrency: 1, NamespaceCount: 1, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	if err := cl.CreateTask("ns-01", "t1", "task"); err != nil {
		t.Fatal(err)
	}
	if err := cl.CreateTask("ns-01", "t2", "task"); err != nil {
		t.Fatal(err)
	}
	if err := cl.MarkReady("ns-01", "t1"); err != nil {
		t.Fatal(err)
	}
	if err := cl.MarkReady("ns-01", "t2"); err != nil {
		t.Fatal(err)
	}
	if err := cl.Dispatch("ns-01", "exec-a"); err != nil {
		t.Fatal(err)
	}
	rt := cl.Runtime["ns-01"]
	if err := cl.Dispatch("ns-01", "exec-b"); err == nil {
		t.Fatal("second dispatch must be rejected when the concurrency quota is full")
	}
	if _, ok := rt.Leases.Get("t2"); ok {
		t.Fatal("over-quota task must not receive a lease")
	}
	st, _ := rt.Tasks.Get("t2")
	if st.State == task.Running {
		t.Fatal("over-quota task must not be marked running")
	}
}
