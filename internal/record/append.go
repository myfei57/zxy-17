package record

import (
	"encoding/json"
	"fmt"
	"os"
)

// Append buffers an execution record in the journal.
func (s *Store) Append(r Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	r.Seq = s.seq
	if r.Attempt == 0 {
		r.Attempt = s.attempts[r.TaskID] + 1
	}
	s.attempts[r.TaskID] = r.Attempt
	line, err := json.Marshal(r)
	if err != nil {
		return Record{}, err
	}
	if _, err := s.journal.Write(append(line, '\n')); err != nil {
		return Record{}, fmt.Errorf("record: journal write: %w", err)
	}
	return r, nil
}

// Commit durably records that every journal op up to seq is committed.
func (s *Store) Commit(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.seq {
		return fmt.Errorf("record: commit seq %d beyond journal %d", seq, s.seq)
	}
	if seq <= s.committed {
		return nil
	}
	if err := s.journal.Sync(); err != nil {
		return fmt.Errorf("record: sync journal: %w", err)
	}
	if err := writeMetaAtomic(s.metaPath("commit.meta"), metaSeq{Seq: seq}); err != nil {
		return fmt.Errorf("record: write commit meta: %w", err)
	}
	s.committed = seq
	return nil
}

// NextAttempt returns the next attempt number for a task.
func (s *Store) NextAttempt(taskID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[taskID] + 1
}

type metaSeq struct {
	Seq uint64 `json:"seq"`
}

func writeMetaAtomic(path string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := osWriteFile(tmp, data); err != nil {
		return err
	}
	return osRename(tmp, path)
}

func osWriteFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

func osRename(from string, to string) error {
	return os.Rename(from, to)
}
