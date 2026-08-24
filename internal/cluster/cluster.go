// Package cluster wires namespaces, tasks, scheduling, leases and the console.
package cluster

import (
	"fmt"
	"os"
	"sync"

	"taskflow/internal/audit"
	"taskflow/internal/console"
	"taskflow/internal/dag"
	"taskflow/internal/idem"
	"taskflow/internal/lease"
	"taskflow/internal/ns"
	"taskflow/internal/quota"
	"taskflow/internal/record"
	"taskflow/internal/scheduler"
	"taskflow/internal/settings"
	"taskflow/internal/task"
)

// NamespaceRuntime bundles the per-namespace components.
type NamespaceRuntime struct {
	Tasks     *task.Store
	Graph     *dag.Graph
	Records   *record.Store
	Leases    *lease.Manager
	Quota     *quota.Manager
	Idem      *idem.Manager
	Scheduler *scheduler.Scheduler
}

// Cluster is the assembled TaskFlow process.
type Cluster struct {
	Cfg     settings.Settings
	NS      *ns.Registry
	Audit   *audit.Logger
	Runtime map[string]*NamespaceRuntime
	Server  *console.Server
	mu      sync.Mutex
}

// Build creates the cluster and its on-disk state.
func Build(cfg settings.Settings) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("cluster: mkdir data: %w", err)
	}
	reg := ns.NewRegistry()
	for _, n := range ns.BuildInitial(cfg.NamespaceCount, cfg.NodeID) {
		if err := reg.Register(n); err != nil {
			return nil, err
		}
	}
	auditLogger := audit.NewLogger(settings.AuditPath(cfg.DataDir))
	c := &Cluster{
		Cfg:     cfg,
		NS:      reg,
		Audit:   auditLogger,
		Runtime: make(map[string]*NamespaceRuntime),
	}
	for _, id := range reg.IDs() {
		rt, err := buildNamespaceRuntime(cfg, id)
		if err != nil {
			return nil, err
		}
		c.Runtime[id] = rt
	}
	c.Server = console.NewServer(c)
	return c, nil
}

func buildNamespaceRuntime(cfg settings.Settings, id string) (*NamespaceRuntime, error) {
	dataDir := settings.TaskDir(cfg.DataDir, id)
	metaDir := settings.MetaDir(cfg.DataDir, id)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return nil, err
	}
	taskStore, err := task.NewStore(task.Options{DataDir: dataDir, MetaDir: metaDir})
	if err != nil {
		return nil, err
	}
	recordDir := settings.RecordDir(cfg.DataDir, id)
	recordMeta := settings.MetaDir(cfg.DataDir, id+"-records")
	if err := os.MkdirAll(recordMeta, 0o755); err != nil {
		return nil, err
	}
	recordStore, err := record.NewStore(record.Options{DataDir: recordDir, MetaDir: recordMeta})
	if err != nil {
		return nil, err
	}
	leaseManager := lease.NewManager(settings.LeasePath(cfg.DataDir, id), 60_000_000_000)
	if err := leaseManager.Load(); err != nil {
		return nil, err
	}
	quotaManager, err := quota.NewManager(cfg.Concurrency, settings.QuotaLedgerPath(cfg.DataDir, id))
	if err != nil {
		return nil, err
	}
	graph := dag.NewGraph()
	idemManager := idem.NewManager(settings.IdemPath(cfg.DataDir, id))
	sched := scheduler.New(taskStore, graph, recordStore, leaseManager)
	return &NamespaceRuntime{
		Tasks:     taskStore,
		Graph:     graph,
		Records:   recordStore,
		Leases:    leaseManager,
		Quota:     quotaManager,
		Idem:      idemManager,
		Scheduler: sched,
	}, nil
}

// Start serves the console until the process exits.
func (c *Cluster) Start() error {
	_ = c.Audit.Note("start", "", "", c.Cfg.HTTPAddr)
	return c.Server.Start(c.Cfg.HTTPAddr)
}

// Close flushes every namespace store.
func (c *Cluster) Close() error {
	for _, id := range c.NS.IDs() {
		if rt := c.Runtime[id]; rt != nil {
			if err := rt.Tasks.Close(); err != nil {
				return err
			}
			if err := rt.Records.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}
