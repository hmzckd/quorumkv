package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hmzckd/quorumkv/internal/raft"
	"github.com/hmzckd/quorumkv/internal/storage"
)

func TestDumpShowsStateEntriesAndDamage(t *testing.T) {
	dir := t.TempDir()
	s, _, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.SaveHardState(raft.HardState{Term: 3, VotedFor: "n2"})
	s.Append([]raft.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2, Data: []byte(`{"op":"put"}`)}})
	s.Close()
	f, _ := os.OpenFile(filepath.Join(dir, "log"), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 0, 9, 1})
	f.Close()

	var out strings.Builder
	if err := dump(&out, dir); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{`term=3 votedFor="n2"`, "(no-op)", `{"op":"put"}`, "damaged tail: 5 bytes"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}
