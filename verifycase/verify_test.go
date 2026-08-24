package verifycase

import (
	"path/filepath"
	"testing"

	"taskflow/internal/cluster"
	"taskflow/internal/record"
	"taskflow/internal/settings"
)

func TestRetryCursorAfterAttemptDurable(t *testing.T) {
	dir := t.TempDir()
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
	if err := cl.MarkReady("ns-01", "t1"); err != nil {
		t.Fatal(err)
	}
	if err := cl.Dispatch("ns-01", "exec"); err != nil {
		t.Fatal(err)
	}
	if err := cl.Complete("ns-01", "t1", "exec", "failed"); err != nil {
		t.Fatal(err)
	}
	rt := cl.Runtime["ns-01"]
	rt.Records.Close()
	if err := cl.Retry("ns-01", "t1"); err == nil {
		t.Fatal("retry must fail when the new attempt record is not durable")
	}
	cursor, err := record.ReadCursor(settings.CursorPath(filepath.Join(dir, "data"), "ns-01", "t1"))
	if err != nil {
		t.Fatal(err)
	}
	if cursor != 0 {
		t.Fatalf("dedup cursor advanced to %d although the retry attempt was not durable", cursor)
	}
}
