package dag

import (
	"os"
	"path/filepath"
	"testing"

	"taskflow/internal/task"
)

// newStore builds an isolated file-backed task store for tests.
func newStore(t *testing.T) *task.Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ns")
	meta := filepath.Join(t.TempDir(), "meta")
	if err := os.MkdirAll(meta, 0o755); err != nil {
		t.Fatalf("MkdirAll meta: %v", err)
	}
	s, err := task.NewStore(task.Options{DataDir: dir, MetaDir: meta})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func createTask(t *testing.T, s *task.Store, id string, state task.State) {
	t.Helper()
	op, err := s.Create(&task.Task{ID: id, State: state})
	if err != nil {
		t.Fatalf("Create %s: %v", id, err)
	}
	if err := s.Commit(op.Seq); err != nil {
		t.Fatalf("Commit %s: %v", id, err)
	}
}

// Ready must reflect the live state of a dependency, not a value cached from
// an earlier lookup. This is the regression for the stale-snapshot readiness
// bug: a downstream was dispatched (or blocked) using the upstream's state as
// observed before it settled, so it saw empty upstream data.
func TestReadyReadsLiveState(t *testing.T) {
	s := newStore(t)
	g := NewGraph()
	if err := g.AddEdge("upstream", "downstream"); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	// Upstream is registered but still running. The downstream must not be
	// ready yet.
	createTask(t, s, "upstream", task.Running)
	createTask(t, s, "downstream", task.Pending)
	if ready, err := g.Ready(s, "downstream"); err != nil || ready {
		t.Fatalf("downstream should not be ready while upstream is running, got ready=%v err=%v", ready, err)
	}

	// Upstream settles to Succeeded. A subsequent readiness check must observe
	// the new state without needing the graph to be rebuilt.
	if err := s.UpdateState("upstream", task.Succeeded); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	ready, err := g.Ready(s, "downstream")
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	if !ready {
		t.Fatalf("downstream should be ready once upstream reaches Succeeded")
	}
}

// Ready must report false again if a dependency leaves the Succeeded state,
// proving the value is never pinned in a snapshot.
func TestReadyReflectsStateChanges(t *testing.T) {
	s := newStore(t)
	g := NewGraph()
	if err := g.AddEdge("upstream", "downstream"); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	createTask(t, s, "upstream", task.Succeeded)
	createTask(t, s, "downstream", task.Pending)

	if ready, _ := g.Ready(s, "downstream"); !ready {
		t.Fatalf("downstream should be ready")
	}
	if err := s.UpdateState("upstream", task.Failed); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	if ready, _ := g.Ready(s, "downstream"); ready {
		t.Fatalf("downstream should not be ready after upstream fails")
	}
}

// Ready reports false for a dependency that does not exist in the store.
func TestReadyMissingDependency(t *testing.T) {
	s := newStore(t)
	g := NewGraph()
	if err := g.AddEdge("ghost", "downstream"); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	createTask(t, s, "downstream", task.Pending)
	if ready, err := g.Ready(s, "downstream"); err != nil || ready {
		t.Fatalf("downstream should not be ready for a missing dependency, got ready=%v err=%v", ready, err)
	}
}

// Ready reports true for a task with no dependencies.
func TestReadyNoDependencies(t *testing.T) {
	s := newStore(t)
	g := NewGraph()
	createTask(t, s, "lonely", task.Pending)
	if ready, err := g.Ready(s, "lonely"); err != nil || !ready {
		t.Fatalf("task with no deps should be ready, got ready=%v err=%v", ready, err)
	}
}
