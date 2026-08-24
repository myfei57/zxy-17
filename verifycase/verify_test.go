package verifycase

import (
	"path/filepath"
	"testing"

	"taskflow/internal/audit"
	"taskflow/internal/dag"
	"taskflow/internal/lease"
	"taskflow/internal/quota"
	"taskflow/internal/record"
	"taskflow/internal/scheduler"
	"taskflow/internal/task"
)

func TestSchedulerRunningAfterRecordDurable(t *testing.T) {
	dir := t.TempDir()
	st, err := task.NewStore(task.Options{DataDir: filepath.Join(dir, "data"), MetaDir: filepath.Join(dir, "meta")})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rs, err := record.NewStore(record.Options{DataDir: filepath.Join(dir, "records"), MetaDir: filepath.Join(dir, "rmeta")})
	if err != nil {
		t.Fatal(err)
	}
	lm := lease.NewManager(filepath.Join(dir, "leases.json"), 60_000_000_000)
	qm, err := quota.NewManager(4, filepath.Join(dir, "quota.ledger"))
	if err != nil {
		t.Fatal(err)
	}
	graph := dag.NewGraph()
	sched := scheduler.New(st, graph, rs, lm, qm)
	if _, err := st.Create(&task.Task{ID: "t1", Namespace: "ns", Name: "task", State: task.Pending}); err != nil {
		t.Fatal(err)
	}
	if err := sched.Enqueue("t1"); err != nil {
		t.Fatal(err)
	}
	auditLogger := audit.NewLogger(filepath.Join(dir, "audit.log"))
	rs.Close()
	if _, err := sched.Dispatch(auditLogger, "exec"); err == nil {
		t.Fatal("dispatch must fail when the execution record is not durable")
	}
	got, _ := st.Get("t1")
	if got.State == task.Running {
		t.Fatal("task must not be marked running before the execution record is durable")
	}
}
