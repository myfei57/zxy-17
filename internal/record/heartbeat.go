package record

import (
	"encoding/json"
	"fmt"
)

// AppendHeartbeat buffers a durable heartbeat in the journal.
func (s *Store) AppendHeartbeat(taskID string, executor string) (Heartbeat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	hb := Heartbeat{Seq: s.seq, TaskID: taskID, Executor: executor}
	line, err := json.Marshal(hb)
	if err != nil {
		return Heartbeat{}, err
	}
	if _, err := s.journal.Write(append(line, '\n')); err != nil {
		return Heartbeat{}, fmt.Errorf("record: heartbeat write: %w", err)
	}
	return hb, nil
}
