package verifycase

import (
	"path/filepath"
	"testing"

	"taskflow/internal/dag"
	"taskflow/internal/task"
)

func TestDagReadinessUsesCurrentTaskState(t *testing.T) {
	dir := t.TempDir()
	st, err := task.NewStore(task.Options{DataDir: filepath.Join(dir, "data"), MetaDir: filepath.Join(dir, "meta")})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Create(&task.Task{ID: "a", Namespace: "ns", Name: "upstream", State: task.Succeeded}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(&task.Task{ID: "b", Namespace: "ns", Name: "downstream", State: task.Pending}); err != nil {
		t.Fatal(err)
	}
	graph := dag.NewGraph()
	if err := graph.AddEdge("a", "b"); err != nil {
		t.Fatal(err)
	}
	ready1, err := graph.Ready(st, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !ready1 {
		t.Fatal("b must be ready while a is succeeded")
	}
	if err := st.UpdateState("a", task.Running); err != nil {
		t.Fatal(err)
	}
	ready2, err := graph.Ready(st, "b")
	if err != nil {
		t.Fatal(err)
	}
	if ready2 {
		t.Fatal("readiness must use the current task state, not a stale snapshot")
	}
}
