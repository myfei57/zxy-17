package settings

import "path/filepath"

// TaskDir returns the on-disk directory owned by one namespace.
func TaskDir(root string, namespace string) string {
	return filepath.Join(root, "namespaces", namespace)
}

// MetaDir returns the directory used for durability watermarks.
func MetaDir(root string, namespace string) string {
	return filepath.Join(root, "meta", namespace)
}

// RecordDir returns the directory that stores execution records.
func RecordDir(root string, namespace string) string {
	return filepath.Join(root, "records", namespace)
}

// LeasePath returns the file that persists the lease ledger.
func LeasePath(root string, namespace string) string {
	return filepath.Join(root, "leases", namespace+".json")
}

// AuditPath returns the audit log file shared by the cluster.
func AuditPath(root string) string {
	return filepath.Join(root, "audit.log")
}

// QuotaLedgerPath returns the durable quota ledger of one namespace.
func QuotaLedgerPath(root string, namespace string) string {
	return filepath.Join(root, "quota", namespace+".ledger")
}

// CursorPath returns the file recording the retry or shard cursor of one task.
func CursorPath(root string, namespace string, taskID string) string {
	return filepath.Join(root, "cursors", namespace, taskID+".json")
}

// IdemPath returns the directory that stores idempotency keys.
func IdemPath(root string, namespace string) string {
	return filepath.Join(root, "idem", namespace)
}
