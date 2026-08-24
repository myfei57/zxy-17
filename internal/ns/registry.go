package ns

import "fmt"

// Rename changes the display name of a namespace.
func (r *Registry) Rename(id string, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.items[id]
	if !ok {
		return fmt.Errorf("ns: unknown namespace %s", id)
	}
	if name == "" {
		return fmt.Errorf("ns: name cannot be empty")
	}
	n.Name = name
	return nil
}

// OwnerOf returns the owning node of a namespace.
func (r *Registry) OwnerOf(id string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.items[id]
	if !ok {
		return "", false
	}
	return n.Owner, true
}

// Transfer moves a namespace to a new owner.
func (r *Registry) Transfer(id string, owner string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.items[id]
	if !ok {
		return fmt.Errorf("ns: unknown namespace %s", id)
	}
	if owner == "" {
		return fmt.Errorf("ns: owner cannot be empty")
	}
	n.Owner = owner
	return nil
}
