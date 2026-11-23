package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/hmzckd/quorumkv/internal/raft"
)

const (
	stateFile = "state"
	logFile   = "log"
)

type Storage struct {
	dir        string
	log        *os.File
	offsets    []int64
	firstIndex uint64
	size       int64
}

type Loaded struct {
	HardState    raft.HardState
	Entries      []raft.Entry
	CutBytes     int64
	CutReason    string
	StateMissing bool
}

func Open(dir string) (*Storage, Loaded, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, Loaded{}, err
	}

	var loaded Loaded
	raw, err := os.ReadFile(filepath.Join(dir, stateFile))
	switch {
	case errors.Is(err, os.ErrNotExist):
		loaded.StateMissing = true
	case err != nil:
		return nil, Loaded{}, err
	default:
		if err := json.Unmarshal(raw, &loaded.HardState); err != nil {
			return nil, Loaded{}, fmt.Errorf("state file is damaged: %w", err)
		}
	}

	path := filepath.Join(dir, logFile)
	lf, err := ReadLog(path)
	if err != nil {
		return nil, Loaded{}, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, Loaded{}, err
	}
	if lf.ValidBytes < lf.TotalBytes {
		if err := f.Truncate(lf.ValidBytes); err != nil {
			f.Close()
			return nil, Loaded{}, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, Loaded{}, err
		}
		loaded.CutBytes = lf.TotalBytes - lf.ValidBytes
		loaded.CutReason = lf.Problem
	}

	s := &Storage{dir: dir, log: f, offsets: lf.Offsets, size: lf.ValidBytes}
	if len(lf.Entries) > 0 {
		s.firstIndex = lf.Entries[0].Index
	}
	loaded.Entries = lf.Entries
	return s, loaded, nil
}

func (s *Storage) SaveHardState(hs raft.HardState) error {
	data, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	return writeFileAtomic(s.dir, stateFile, data)
}

func (s *Storage) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index

	if len(s.offsets) > 0 && first <= s.lastIndex() {
		if first < s.firstIndex {
			return fmt.Errorf("cannot rewrite index %d before first index %d", first, s.firstIndex)
		}
		keep := first - s.firstIndex
		s.size = s.offsets[keep]
		s.offsets = s.offsets[:keep]
		if err := s.log.Truncate(s.size); err != nil {
			return err
		}
	}
	if len(s.offsets) == 0 {
		s.firstIndex = first
	} else if first != s.lastIndex()+1 {
		return fmt.Errorf("append index %d after last index %d leaves a gap", first, s.lastIndex())
	}

	var buf []byte
	offsets := make([]int64, 0, len(entries))
	for i, e := range entries {
		if e.Index != first+uint64(i) {
			return fmt.Errorf("entries are not contiguous at index %d", e.Index)
		}
		rec, err := encodeRecord(e)
		if err != nil {
			return err
		}
		offsets = append(offsets, s.size+int64(len(buf)))
		buf = append(buf, rec...)
	}
	if _, err := s.log.WriteAt(buf, s.size); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.size += int64(len(buf))
	s.offsets = append(s.offsets, offsets...)
	return nil
}

func (s *Storage) Close() error {
	return s.log.Close()
}

func (s *Storage) lastIndex() uint64 {
	return s.firstIndex + uint64(len(s.offsets)) - 1
}

func writeFileAtomic(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
