package verifycase

import (
	"path/filepath"
	"testing"

	"taskflow/internal/lease"
	"taskflow/internal/record"
)

func TestLeaseRenewAfterHeartbeatDurable(t *testing.T) {
	dir := t.TempDir()
	rs, err := record.NewStore(record.Options{DataDir: filepath.Join(dir, "records"), MetaDir: filepath.Join(dir, "rmeta")})
	if err != nil {
		t.Fatal(err)
	}
	lm := lease.NewManager(filepath.Join(dir, "leases.json"), 60_000_000_000)
	if _, err := lm.Grant("t1", "exec"); err != nil {
		t.Fatal(err)
	}
	before, _ := lm.Get("t1")
	rs.Close()
	if err := lm.Renew("t1", "exec", rs); err == nil {
		t.Fatal("renew must fail when the heartbeat is not durable")
	}
	after, _ := lm.Get("t1")
	if after.Until != before.Until {
		t.Fatal("lease must not be extended before the heartbeat is durable")
	}
}
