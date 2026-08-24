package retry

import (
	"taskflow/internal/record"
)

// Next returns the next attempt number when the retry policy allows it.
// It reads the dedup cursor and the durable attempt counter without writing.
func Next(store *record.Store, taskID string, policy *Policy, cursorPath string) (int, error) {
	cursor, err := record.ReadCursor(cursorPath)
	if err != nil {
		return 0, err
	}
	last := store.NextAttempt(taskID) - 1
	if uint64(last) <= cursor {
		return 0, nil
	}
	if uint64(last) < cursor {
		return 0, record.CursorError(cursorPath, cursor, uint64(last))
	}
	next := last + 1
	if !policy.Allowed(next) {
		return 0, nil
	}
	return next, nil
}

// Commit durably advances the retry cursor after the next attempt is recorded.
func Commit(cursorPath string, processed int) error {
	return record.WriteCursor(cursorPath, uint64(processed))
}
