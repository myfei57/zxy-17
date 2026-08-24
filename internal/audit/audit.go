// Package audit records a durable, queryable log of scheduling events.
package audit

import "sync"

// Event is one audit record.
type Event struct {
	ID        string `json:"id"`
	At        int64  `json:"at"`
	Type      string `json:"type"`
	Namespace string `json:"namespace,omitempty"`
	Key       string `json:"key,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// Logger appends events to a shared file.
type Logger struct {
	mu    sync.Mutex
	path  string
	count int
}

// NewLogger creates an audit logger writing to path.
func NewLogger(path string) *Logger {
	return &Logger{path: path}
}

// Count returns the number of events recorded so far in this process.
func (l *Logger) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}
