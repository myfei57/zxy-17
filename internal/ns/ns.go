// Package ns models task namespaces and their ownership.
package ns

import (
	"fmt"
	"sort"
	"sync"
)

// Namespace is one isolated scheduling scope.
type Namespace struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

// Registry tracks the namespaces of the cluster.
type Registry struct {
	mu    sync.Mutex
	items map[string]*Namespace
}

// NewRegistry returns an empty namespace registry.
func NewRegistry() *Registry {
	return &Registry{items: make(map[string]*Namespace)}
}

// Register adds a namespace to the registry.
func (r *Registry) Register(n *Namespace) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n == nil || n.ID == "" {
		return fmt.Errorf("ns: cannot register an empty namespace")
	}
	if _, exists := r.items[n.ID]; exists {
		return fmt.Errorf("ns: namespace %s already registered", n.ID)
	}
	r.items[n.ID] = n
	return nil
}

// Get returns the namespace with the given id.
func (r *Registry) Get(id string) (*Namespace, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.items[id]
	return n, ok
}

// IDs lists all namespace ids, sorted.
func (r *Registry) IDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.items))
	for id := range r.items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// BuildInitial creates the default namespaces for the cluster.
func BuildInitial(count int, owner string) []*Namespace {
	names := []string{"billing", "etl", "report", "cleanup"}
	out := make([]*Namespace, 0, count)
	for i := 0; i < count; i++ {
		name := names[i%len(names)]
		if i >= len(names) {
			name = fmt.Sprintf("service-%d", i+1)
		}
		out = append(out, &Namespace{ID: fmt.Sprintf("ns-%02d", i+1), Name: name, Owner: owner})
	}
	return out
}
