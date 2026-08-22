package console

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NamespaceInfo describes one namespace for the console.
type NamespaceInfo struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

// TaskInfo describes one task for the console.
type TaskInfo struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	State    string `json:"state"`
	Attempts int    `json:"attempts"`
}

// LeaseInfo describes one lease for the console.
type LeaseInfo struct {
	TaskID   string `json:"task_id"`
	Executor string `json:"executor"`
	State    string `json:"state"`
	Until    int64  `json:"until"`
}

// RecordInfo describes one execution record for the console.
type RecordInfo struct {
	Seq      uint64 `json:"seq"`
	TaskID   string `json:"task_id"`
	Attempt  int    `json:"attempt"`
	State    string `json:"state"`
	Executor string `json:"executor"`
}

// QuotaInfo describes the concurrency budget.
type QuotaInfo struct {
	Limit  int `json:"limit"`
	Active int `json:"active"`
}

type createTaskRequest struct {
	Namespace string `json:"namespace"`
	ID        string `json:"id"`
	Name      string `json:"name"`
}

type namespaceRequest struct {
	Namespace string `json:"namespace"`
}

type dispatchRequest struct {
	Namespace string `json:"namespace"`
	Executor  string `json:"executor"`
}

type renameRequest struct {
	Name string `json:"name"`
}

type transferRequest struct {
	Owner string `json:"owner"`
}

type dependRequest struct {
	From string `json:"from"`
}

type completeRequest struct {
	Namespace string `json:"namespace"`
	Executor  string `json:"executor"`
	State     string `json:"state"`
}

type renewRequest struct {
	Namespace string `json:"namespace"`
	TaskID    string `json:"task_id"`
	Executor  string `json:"executor"`
}

type shardRequest struct {
	Namespace string `json:"namespace"`
	TaskID    string `json:"task_id"`
	Key       string `json:"key"`
	Shard     int    `json:"shard"`
	Total     int    `json:"total"`
	Data      string `json:"data"`
}

func (s *Server) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.backend.Namespaces())
}

func (s *Server) handleNamespaceOwner(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	owner, ok := s.backend.NamespaceOwner(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "namespace not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"owner": owner})
}

func (s *Server) handleRenameNamespace(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req renameRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.RenameNamespace(id, req.Name); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "renamed"})
}

func (s *Server) handleTransferNamespace(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req transferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.TransferNamespace(id, req.Owner); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "transferred"})
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	writeJSON(w, http.StatusOK, s.backend.Tasks(namespace))
}

func (s *Server) handleTaskCount(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	writeJSON(w, http.StatusOK, map[string]int{"count": s.backend.TaskCount(namespace)})
}

func (s *Server) handleTask(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	id := chi.URLParam(r, "id")
	info, ok := s.backend.Task(namespace, id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.CreateTask(req.Namespace, req.ID, req.Name); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "created"})
}

func (s *Server) handleMarkReady(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	id := chi.URLParam(r, "id")
	if err := s.backend.MarkReady(namespace, id); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleAddDependency(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	id := chi.URLParam(r, "id")
	var req dependRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.AddDependency(namespace, req.From, id); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "depends"})
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	id := chi.URLParam(r, "id")
	var req completeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.Complete(namespace, id, req.Executor, req.State); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	id := chi.URLParam(r, "id")
	if err := s.backend.Retry(namespace, id); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "retried"})
}

func (s *Server) handleDispatch(w http.ResponseWriter, r *http.Request) {
	var req dispatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.Dispatch(req.Namespace, req.Executor); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "dispatched"})
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	var req renewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.Renew(req.Namespace, req.TaskID, req.Executor); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "renewed"})
}

func (s *Server) handleSubmitShard(w http.ResponseWriter, r *http.Request) {
	var req shardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.backend.SubmitShard(req.Namespace, req.TaskID, req.Key, req.Shard, req.Total, req.Data); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "shard"})
}

func (s *Server) handleLeases(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	writeJSON(w, http.StatusOK, s.backend.Leases(namespace))
}

func (s *Server) handleReap(w http.ResponseWriter, r *http.Request) {
	var req namespaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	n, err := s.backend.Reap(req.Namespace)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"reclaimed": n})
}

func (s *Server) handleRecords(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	writeJSON(w, http.StatusOK, s.backend.Records(namespace))
}

func (s *Server) handleRecordDir(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	writeJSON(w, http.StatusOK, map[string]string{"dir": s.backend.RecordDir(namespace)})
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	payload := map[string]any{
		"count":   s.backend.AuditCount(),
		"by_type": s.backend.AuditCounts(),
	}
	if kind := r.URL.Query().Get("type"); kind != "" {
		payload["entries"] = s.backend.AuditEntriesByType(kind)
	} else {
		payload["entries"] = s.backend.AuditEntries(100)
	}
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) handleQuota(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	writeJSON(w, http.StatusOK, s.backend.Quota(namespace))
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
