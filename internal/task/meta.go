package task

import (
	"encoding/json"
	"os"
)

type metaSeq struct {
	Seq uint64 `json:"seq"`
}

type tombstone struct {
	Seq uint64 `json:"seq"`
	ID  string `json:"id"`
}

func writeMetaAtomic(path string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := syncPath(tmp); err != nil {
		return err
	}
	return os.Rename(tmp, path)
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

func syncPath(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
