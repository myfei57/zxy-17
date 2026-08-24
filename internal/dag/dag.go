// Package dag models task dependencies and readiness.
package dag

import (
	"fmt"
	"sync"

	"taskflow/internal/task"
)

// Edge is a dependency from one task to another.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Graph tracks dependency edges for a namespace.
type Graph struct {
	mu       sync.Mutex
	edges    map[string][]string
	reverse  map[string][]string
	snapshot map[string]task.State
}

// NewGraph returns an empty dependency graph.
func NewGraph() *Graph {
	return &Graph{
		edges:   make(map[string][]string),
		reverse: make(map[string][]string),
	}
}

// AddEdge records that From must finish before To starts.
func (g *Graph) AddEdge(from string, to string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if from == to {
		return fmt.Errorf("dag: self dependency %s", from)
	}
	g.edges[to] = append(g.edges[to], from)
	g.reverse[from] = append(g.reverse[from], to)
	return nil
}

// Dependencies returns the tasks that To depends on.
func (g *Graph) Dependencies(to string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.edges[to]...)
}

// Dependents returns the tasks that depend on From.
func (g *Graph) Dependents(from string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.reverse[from]...)
}
