package verifycase

import (
	"path/filepath"
	"testing"

	"taskflow/internal/cluster"
	"taskflow/internal/settings"
)

func TestIdempotencyKeyAfterResultDurable(t *testing.T) {
	dir := t.TempDir()
	cl, err := cluster.Build(settings.Settings{
		DataDir: filepath.Join(dir, "data"), NodeID: "node-a", Concurrency: 4, NamespaceCount: 1, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	if err := cl.CreateTask("ns-01", "t2", "task"); err != nil {
		t.Fatal(err)
	}
	rt := cl.Runtime["ns-01"]
	rt.Records.Close()
	if err := cl.SubmitShard("ns-01", "t2", "k-1", 0, 2, "part0"); err == nil {
		t.Fatal("shard submit must fail when the shard result is not durable")
	}
	consumed, err := rt.Idem.Check("k-1")
	if err != nil {
		t.Fatal(err)
	}
	if consumed {
		t.Fatal("idempotency key must not be consumed before the result is durable")
	}
}
