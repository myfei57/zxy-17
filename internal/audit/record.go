package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Record appends one event to the audit file.
func (l *Logger) Record(ev Event) error {
	if ev.ID == "" {
		ev.ID = uuid.NewString()
	}
	if ev.At == 0 {
		ev.At = time.Now().UnixNano()
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("audit: encode: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return fmt.Errorf("audit: mkdir: %w", err)
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("audit: open: %w", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		file.Close()
		return fmt.Errorf("audit: write: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("audit: sync: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("audit: close: %w", err)
	}
	l.count++
	return nil
}

// Note is a convenience wrapper for simple events.
func (l *Logger) Note(kind string, namespace string, key string, detail string) error {
	return l.Record(Event{Type: kind, Namespace: namespace, Key: key, Detail: detail})
}

// Entries returns the most recent events, newest first.
func (l *Logger) Entries(limit int) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	data, err := os.ReadFile(l.path)
	if err != nil {
		return nil
	}
	var events []Event
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var ev Event
		if json.Unmarshal([]byte(line), &ev) == nil {
			events = append(events, ev)
		}
	}
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events
}

// FilterByType returns events whose type matches the given kind.
func (l *Logger) FilterByType(kind string) []Event {
	var out []Event
	for _, ev := range l.Entries(0) {
		if ev.Type == kind {
			out = append(out, ev)
		}
	}
	return out
}

// Counts returns the number of events per type.
func (l *Logger) Counts() map[string]int {
	counts := map[string]int{}
	for _, ev := range l.Entries(0) {
		counts[ev.Type]++
	}
	return counts
}
