package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hmzckd/quorumkv/internal/raft"
)

func entries(term uint64, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Term: term, Index: i, Data: []byte{'v', byte('0' + i%10)}})
	}
	return out
}

func open(t *testing.T, dir string) (*Storage, Loaded) {
	t.Helper()
	s, loaded, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, loaded
}

func reopen(t *testing.T, s *Storage, dir string) (*Storage, Loaded) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return open(t, dir)
}

func indexes(es []raft.Entry) []uint64 {
	var out []uint64
	for _, e := range es {
		out = append(out, e.Index)
	}
	return out
}

func TestFreshDirectoryIsCreatedAndEmpty(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data", "n1")
	_, loaded := open(t, dir)

	if !loaded.StateMissing || len(loaded.Entries) != 0 || loaded.HardState != (raft.HardState{}) {
		t.Fatalf("loaded = %+v, want empty", loaded)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("data dir not created: %v", err)
	}
}

func TestHardStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir)
	if err := s.SaveHardState(raft.HardState{Term: 7, VotedFor: "n2"}); err != nil {
		t.Fatal(err)
	}

	_, loaded := reopen(t, s, dir)

	if loaded.HardState != (raft.HardState{Term: 7, VotedFor: "n2"}) {
		t.Fatalf("hard state = %+v, want term 7 vote n2", loaded.HardState)
	}
}

func TestLeftoverTempFileIsIgnored(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir)
	if err := s.SaveHardState(raft.HardState{Term: 3}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.tmp"), []byte(`{"term":9,"vot`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, loaded := reopen(t, s, dir)

	if loaded.HardState.Term != 3 {
		t.Fatalf("term = %d, want 3: a crash before rename must keep the old state", loaded.HardState.Term)
	}
}

func TestEntriesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir)
	if err := s.Append(entries(1, 1, 3)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(1, 4, 5)); err != nil {
		t.Fatal(err)
	}

	_, loaded := reopen(t, s, dir)

	if got := indexes(loaded.Entries); len(got) != 5 || got[4] != 5 {
		t.Fatalf("indexes = %v, want 1..5", got)
	}
	if string(loaded.Entries[2].Data) != "v3" {
		t.Errorf("entry 3 data = %q", loaded.Entries[2].Data)
	}
}

func TestAppendReplacesConflictingSuffix(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir)
	if err := s.Append(entries(1, 1, 5)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(2, 3, 3)); err != nil {
		t.Fatal(err)
	}

	_, loaded := reopen(t, s, dir)

	if got := indexes(loaded.Entries); len(got) != 3 {
		t.Fatalf("indexes = %v, want 1..3", got)
	}
	if loaded.Entries[2].Term != 2 {
		t.Errorf("entry 3 term = %d, want 2", loaded.Entries[2].Term)
	}
}

func TestAppendRejectsGap(t *testing.T) {
	s, _ := open(t, t.TempDir())
	if err := s.Append(entries(1, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(1, 5, 5)); err == nil {
		t.Fatal("expected an error for a gap after index 2")
	}
}

func TestDamagedTailIsCutOff(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir)
	if err := s.Append(entries(1, 1, 3)); err != nil {
		t.Fatal(err)
	}
	s.Close()

	path := filepath.Join(dir, "log")
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	t.Logf("log was %d bytes, cut 3 bytes to simulate power loss in the middle of a write", info.Size())

	s, loaded := open(t, dir)
	t.Logf("on open: kept entries %v, cut %d bytes (%s)", indexes(loaded.Entries), loaded.CutBytes, loaded.CutReason)

	if got := indexes(loaded.Entries); len(got) != 2 {
		t.Fatalf("indexes = %v, want 1..2", got)
	}
	if loaded.CutBytes == 0 {
		t.Fatal("CutBytes = 0, want the half record removed")
	}

	if err := s.Append(entries(2, 3, 3)); err != nil {
		t.Fatalf("append after repair: %v", err)
	}
	_, loaded = reopen(t, s, dir)
	if got := indexes(loaded.Entries); len(got) != 3 || loaded.CutBytes != 0 {
		t.Fatalf("after repair: indexes %v, cut %d", got, loaded.CutBytes)
	}
}

func TestFlippedByteIsCaughtByChecksum(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir)
	if err := s.Append(entries(1, 1, 3)); err != nil {
		t.Fatal(err)
	}
	second := s.offsets[1]
	s.Close()

	path := filepath.Join(dir, "log")
	data, _ := os.ReadFile(path)
	data[second+headerSize+2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, loaded := open(t, dir)

	if got := indexes(loaded.Entries); len(got) != 1 {
		t.Fatalf("indexes = %v, want only 1", got)
	}
	if loaded.CutReason == "" {
		t.Error("CutReason is empty, want a checksum mismatch")
	}
}

func TestStorageFeedsRaft(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir)
	if err := s.SaveHardState(raft.HardState{Term: 2, VotedFor: "n1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(2, 1, 4)); err != nil {
		t.Fatal(err)
	}
	_, loaded := reopen(t, s, dir)

	r, err := raft.New(raft.Config{ID: "n1", Peers: []string{"n1", "n2", "n3"}, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: newRand()}, raft.State{HardState: loaded.HardState, Entries: loaded.Entries})
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}
	if st := r.Status(); st.Term != 2 || st.LastIndex != 4 {
		t.Fatalf("status = %+v, want term 2 and last index 4", st)
	}
}
