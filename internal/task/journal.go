package task

import (
	"encoding/json"
	"os"
	"strings"
)

func readJournal(path string) ([]Op, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ops []Op
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var op Op
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func (s *Store) recover() error {
	committed, err := readMetaSeq(s.metaPath("commit.meta"))
	if err != nil {
		return err
	}
	s.committed = committed
	ops, err := readJournal(s.journal.Name())
	if err != nil {
		return err
	}
	for _, op := range ops {
		if op.Seq > committed || op.Kind != "set" {
			continue
		}
		if op.Seq > s.seq {
			s.seq = op.Seq
		}
		s.index[op.ID] = &Task{
			ID:         op.ID,
			Namespace:  op.Namespace,
			Name:       op.Name,
			State:      op.State,
			Attempts:   op.Attempts,
			Timeout:    op.Timeout,
			Dependents: op.Dependents,
			Seq:        op.Seq,
		}
	}
	return nil
}
