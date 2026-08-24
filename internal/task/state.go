package task

// StateOf returns the current state of a task.
func StateOf(store *Store, id string) (State, bool) {
	t, ok := store.Get(id)
	if !ok {
		return "", false
	}
	return t.State, true
}
