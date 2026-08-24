package record

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type cursor struct {
	Seq uint64 `json:"seq"`
}

// ReadCursor returns the durable cursor of a namespace.
func ReadCursor(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var c cursor
	if err := json.Unmarshal(data, &c); err != nil {
		return 0, err
	}
	return c.Seq, nil
}

// WriteCursor durably advances the cursor.
func WriteCursor(path string, seq uint64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(cursor{Seq: seq})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// CursorError is returned when a cursor move would be out of order.
func CursorError(path string, got uint64, want uint64) error {
	return fmt.Errorf("record: cursor %s at %d cannot move to %d", path, got, want)
}
