// Package console exposes the TaskFlow HTTP API and embedded control pages.
package console

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"taskflow/internal/audit"
	"taskflow/internal/web"
)

// Backend is the cluster surface the console renders.
type Backend interface {
	Namespaces() []NamespaceInfo
	RenameNamespace(id string, name string) error
	TransferNamespace(id string, owner string) error
	NamespaceOwner(id string) (string, bool)
	Tasks(namespace string) []TaskInfo
	Task(namespace string, id string) (TaskInfo, bool)
	TaskCount(namespace string) int
	CreateTask(namespace string, id string, name string) error
	MarkReady(namespace string, id string) error
	AddDependency(namespace string, from string, to string) error
	Dispatch(namespace string, executor string) error
	Complete(namespace string, id string, executor string, state string) error
	Renew(namespace string, id string, executor string) error
	Retry(namespace string, id string) error
	SubmitShard(namespace string, id string, key string, shard int, total int, data string) error
	ReadyCount(namespace string) int
	Leases(namespace string) []LeaseInfo
	Reap(namespace string) (int, error)
	Records(namespace string) []RecordInfo
	RecordDir(namespace string) string
	AuditEntries(limit int) []audit.Event
	AuditEntriesByType(kind string) []audit.Event
	AuditCount() int
	AuditCounts() map[string]int
	Quota(namespace string) QuotaInfo
}

// Server serves the console pages and JSON API.
type Server struct {
	backend Backend
}

// NewServer creates a console server backed by a cluster.
func NewServer(backend Backend) *Server {
	return &Server{backend: backend}
}

// Handler builds the chi router.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/", s.handleIndex)
	r.Get("/tasks", s.handlePage(web.TasksHTML))
	r.Get("/schedule", s.handlePage(web.ScheduleHTML))
	r.Get("/records", s.handlePage(web.RecordsHTML))
	r.Get("/audit", s.handlePage(web.AuditHTML))
	r.Get("/api/health", s.handleHealth)
	r.Get("/api/namespaces", s.handleNamespaces)
	r.Get("/api/namespaces/{id}/owner", s.handleNamespaceOwner)
	r.Post("/api/namespaces/{id}/rename", s.handleRenameNamespace)
	r.Post("/api/namespaces/{id}/transfer", s.handleTransferNamespace)
	r.Get("/api/tasks", s.handleTasks)
	r.Get("/api/tasks/count", s.handleTaskCount)
	r.Get("/api/tasks/{id}", s.handleTask)
	r.Post("/api/tasks", s.handleCreateTask)
	r.Post("/api/tasks/{id}/ready", s.handleMarkReady)
	r.Post("/api/tasks/{id}/depends", s.handleAddDependency)
	r.Post("/api/tasks/{id}/complete", s.handleComplete)
	r.Post("/api/tasks/{id}/retry", s.handleRetry)
	r.Post("/api/dispatch", s.handleDispatch)
	r.Post("/api/renew", s.handleRenew)
	r.Post("/api/shard", s.handleSubmitShard)
	r.Get("/api/leases", s.handleLeases)
	r.Post("/api/reap", s.handleReap)
	r.Get("/api/records", s.handleRecords)
	r.Get("/api/records/dir", s.handleRecordDir)
	r.Get("/api/audit", s.handleAudit)
	r.Get("/api/quota", s.handleQuota)
	return r
}

// Start serves the console until the process exits.
func (s *Server) Start(addr string) error {
	return http.ListenAndServe(addr, s.Handler())
}
