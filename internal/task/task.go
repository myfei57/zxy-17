// Package task implements the file-backed task definition store.
package task

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// State is the lifecycle state of a task run.
type State string

const (
	Pending   State = "pending"
	Ready     State = "ready"
	Running   State = "running"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Skipped   State = "skipped"
)

// Task is one scheduled unit of work.
type Task struct {
	ID         string `json:"id"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	State      State  `json:"state"`
	Attempts   int    `json:"attempts"`
	Timeout    int64  `json:"timeout"`
	Dependents int    `json:"dependents"`
	Seq        uint64 `json:"seq"`
}

// Op is one journaled mutation.
type Op struct {
	Seq        uint64 `json:"seq"`
	Kind       string `json:"kind"`
	ID         string `json:"id"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	State      State  `json:"state"`
	Attempts   int    `json:"attempts"`
	Timeout    int64  `json:"timeout"`
	Dependents int    `json:"dependents"`
}

// Options controls where a store keeps its files.
type Options struct {
	DataDir        string
	MetaDir        string
	TombstonePath  string
	DeleteMetaPath string
}

// Store is a namespace-local file-backed task store.
type Store struct {
	opts       Options
	mu         sync.Mutex
	seq        uint64
	committed  uint64
	index      map[string]*Task
	journal    *os.File
	tombstones *os.File
}

// NewStore opens or recreates a store at the given paths.
func NewStore(opts Options) (*Store, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("task: data dir required")
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("task: mkdir %s: %w", opts.DataDir, err)
	}
	journal, err := os.OpenFile(filepath.Join(opts.DataDir, "journal.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("task: open journal: %w", err)
	}
	tombstonePath := opts.TombstonePath
	if tombstonePath == "" {
		tombstonePath = filepath.Join(opts.DataDir, "tombstone.log")
	}
	if err := os.MkdirAll(filepath.Dir(tombstonePath), 0o755); err != nil {
		journal.Close()
		return nil, fmt.Errorf("task: mkdir tombstones: %w", err)
	}
	tombstones, err := os.OpenFile(tombstonePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		journal.Close()
		return nil, fmt.Errorf("task: open tombstones: %w", err)
	}
	s := &Store{
		opts:       opts,
		index:      make(map[string]*Task),
		journal:    journal,
		tombstones: tombstones,
	}
	if err := s.recover(); err != nil {
		journal.Close()
		tombstones.Close()
		return nil, err
	}
	return s, nil
}

// Close flushes and closes the store files.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.journal.Sync(); err != nil {
		return err
	}
	if err := s.journal.Close(); err != nil {
		return err
	}
	return s.tombstones.Close()
}

// Create registers a new task definition.
func (s *Store) Create(t *Task) (Op, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t == nil || t.ID == "" {
		return Op{}, fmt.Errorf("task: empty task")
	}
	s.seq++
	op := Op{
		Seq:        s.seq,
		Kind:       "set",
		ID:         t.ID,
		Namespace:  t.Namespace,
		Name:       t.Name,
		State:      t.State,
		Attempts:   t.Attempts,
		Timeout:    t.Timeout,
		Dependents: t.Dependents,
	}
	line, err := json.Marshal(op)
	if err != nil {
		return Op{}, err
	}
	if _, err := s.journal.Write(append(line, '\n')); err != nil {
		return Op{}, fmt.Errorf("task: journal write: %w", err)
	}
	copy := *t
	copy.Seq = op.Seq
	s.index[t.ID] = &copy
	return op, nil
}

// UpdateState changes a task state in the index (durability via Commit).
func (s *Store) UpdateState(id string, state State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.index[id]
	if !ok {
		return fmt.Errorf("task: unknown task %s", id)
	}
	t.State = state
	return nil
}

// Commit durably records that every journal op up to seq is committed.
func (s *Store) Commit(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.seq {
		return fmt.Errorf("task: commit seq %d beyond journal %d", seq, s.seq)
	}
	if seq <= s.committed {
		return nil
	}
	if err := s.journal.Sync(); err != nil {
		return fmt.Errorf("task: sync journal: %w", err)
	}
	if err := writeMetaAtomic(s.metaPath("commit.meta"), metaSeq{Seq: seq}); err != nil {
		return fmt.Errorf("task: write commit meta: %w", err)
	}
	s.committed = seq
	return nil
}

func (s *Store) metaPath(name string) string {
	if s.opts.MetaDir == "" {
		return filepath.Join(s.opts.DataDir, name)
	}
	return filepath.Join(s.opts.MetaDir, name)
}

// Get returns the task with the given id.
func (s *Store) Get(id string) (*Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.index[id]
	if !ok {
		return nil, false
	}
	copy := *t
	return &copy, true
}

// All lists the tasks of a namespace, sorted by id.
func (s *Store) All(namespace string) []*Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Task
	for _, t := range s.index {
		if t.Namespace == namespace {
			copy := *t
			out = append(out, &copy)
		}
	}
	sortTasks(out)
	return out
}

// Count returns the number of tasks in the index.
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.index)
}

func sortTasks(tasks []*Task) {
	for i := 1; i < len(tasks); i++ {
		for j := i; j > 0 && tasks[j].ID < tasks[j-1].ID; j-- {
			tasks[j], tasks[j-1] = tasks[j-1], tasks[j]
		}
	}
}
