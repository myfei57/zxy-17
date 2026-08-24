package record

import (
	"encoding/json"
	"fmt"
)

// AppendShardResult buffers a durable shard result in the journal.
func (s *Store) AppendShardResult(r ShardResult) (ShardResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	r.Seq = s.seq
	line, err := json.Marshal(r)
	if err != nil {
		return ShardResult{}, err
	}
	if _, err := s.journal.Write(append(line, '\n')); err != nil {
		return ShardResult{}, fmt.Errorf("record: shard result write: %w", err)
	}
	return r, nil
}
