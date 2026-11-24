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
	stateFile    = "state"
	logFile      = "log"
	snapshotFile = "snapshot"
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
	Snapshot     *raft.Snapshot
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

	snap, err := readSnapshot(dir)
	if err != nil {
		return nil, Loaded{}, err
	}
	loaded.Snapshot = snap

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

	if snap != nil && len(lf.Entries) > 0 && s.firstIndex <= snap.Index {
		if err := s.dropUpTo(*snap, lf.Entries); err != nil {
			s.Close()
			return nil, Loaded{}, err
		}
		loaded.Entries = entriesAfter(*snap, lf.Entries)
	}
	if snap != nil && len(loaded.Entries) > 0 && loaded.Entries[0].Index != snap.Index+1 {
		s.Close()
		return nil, Loaded{}, fmt.Errorf("log starts at %d but snapshot ends at %d", loaded.Entries[0].Index, snap.Index)
	}
	return s, loaded, nil
}

func (s *Storage) SaveSnapshot(snap raft.Snapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(s.dir, snapshotFile, data); err != nil {
		return err
	}
	if len(s.offsets) == 0 || s.firstIndex > snap.Index {
		return nil
	}
	lf, err := ReadLog(filepath.Join(s.dir, logFile))
	if err != nil {
		return err
	}
	return s.dropUpTo(snap, lf.Entries)
}

func entriesAfter(snap raft.Snapshot, entries []raft.Entry) []raft.Entry {
	for i, e := range entries {
		if e.Index == snap.Index {
			if e.Term != snap.Term {
				return nil
			}
			return entries[i+1:]
		}
	}
	return nil
}

func (s *Storage) dropUpTo(snap raft.Snapshot, entries []raft.Entry) error {
	keep := entriesAfter(snap, entries)
	start := s.size
	if len(keep) > 0 {
		start = s.offsets[keep[0].Index-s.firstIndex]
	}

	raw, err := os.ReadFile(filepath.Join(s.dir, logFile))
	if err != nil {
		return err
	}
	kept := raw[start:s.size]
	if err := writeFileAtomic(s.dir, logFile, kept); err != nil {
		return err
	}
	if err := s.log.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, logFile), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	s.log = f

	offsets := make([]int64, len(keep))
	for i := range keep {
		offsets[i] = s.offsets[keep[i].Index-s.firstIndex] - start
	}
	s.offsets = offsets
	s.size = int64(len(kept))
	s.firstIndex = snap.Index + 1
	return nil
}

func readSnapshot(dir string) (*raft.Snapshot, error) {
	raw, err := os.ReadFile(filepath.Join(dir, snapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap raft.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, fmt.Errorf("snapshot file is damaged: %w", err)
	}
	return &snap, nil
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
