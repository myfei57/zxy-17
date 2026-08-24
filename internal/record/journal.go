package record

import (
	"encoding/json"
	"os"
	"strings"
)

func (s *Store) recover() error {
	committed, err := readMetaSeq(s.metaPath("commit.meta"))
	if err != nil {
		return err
	}
	s.committed = committed
	data, err := os.ReadFile(s.journal.Name())
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record Record
		if json.Unmarshal([]byte(line), &record) == nil {
			if record.Seq <= committed {
				if record.Attempt > s.attempts[record.TaskID] {
					s.attempts[record.TaskID] = record.Attempt
				}
				if record.Seq > s.seq {
					s.seq = record.Seq
				}
			}
			continue
		}
		var hb Heartbeat
		if json.Unmarshal([]byte(line), &hb) == nil {
			if hb.Seq > s.seq && hb.Seq <= committed {
				s.seq = hb.Seq
			}
			continue
		}
		var sr ShardResult
		if json.Unmarshal([]byte(line), &sr) == nil {
			if sr.Seq > s.seq && sr.Seq <= committed {
				s.seq = sr.Seq
			}
		}
	}
	return nil
}

func readMetaSeq(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var m metaSeq
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, err
	}
	return m.Seq, nil
}
