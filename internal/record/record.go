// Package record persists execution records, heartbeats and shard results.
package record

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"taskflow/internal/task"
)

// Record is one execution attempt of a task.
type Record struct {
	Seq       uint64     `json:"seq"`
	TaskID    string     `json:"task_id"`
	Namespace string     `json:"namespace"`
	Attempt   int        `json:"attempt"`
	State     task.State `json:"state"`
	Executor  string     `json:"executor"`
}

// Heartbeat is a durable executor heartbeat.
type Heartbeat struct {
	Seq      uint64 `json:"seq"`
	TaskID   string `json:"task_id"`
	Executor string `json:"executor"`
}

// ShardResult is the durable result of one shard.
type ShardResult struct {
	Seq    uint64 `json:"seq"`
	TaskID string `json:"task_id"`
	Shard  int    `json:"shard"`
	Total  int    `json:"total"`
	Data   string `json:"data"`
}

// Store appends execution records and shard results for a namespace.
type Store struct {
	opts      Options
	mu        sync.Mutex
	seq       uint64
	committed uint64
	attempts  map[string]int
	journal   *os.File
}

// Options controls where a record store keeps its files.
type Options struct {
	DataDir string
	MetaDir string
}

// NewStore opens or recreates a record store.
func NewStore(opts Options) (*Store, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("record: data dir required")
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("record: mkdir: %w", err)
	}
	journal, err := os.OpenFile(filepath.Join(opts.DataDir, "record.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("record: open journal: %w", err)
	}
	s := &Store{opts: opts, attempts: make(map[string]int), journal: journal}
	if err := s.recover(); err != nil {
		journal.Close()
		return nil, err
	}
	return s, nil
}

// Close flushes and closes the record store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.journal.Sync(); err != nil {
		return err
	}
	return s.journal.Close()
}

func (s *Store) metaPath(name string) string {
	if s.opts.MetaDir == "" {
		return filepath.Join(s.opts.DataDir, name)
	}
	return filepath.Join(s.opts.MetaDir, name)
}
