package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hmzckd/quorumkv/internal/raft"
	"github.com/hmzckd/quorumkv/internal/storage"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: qkdump <data-dir>")
		os.Exit(2)
	}
	if err := dump(os.Stdout, os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "qkdump:", err)
		os.Exit(1)
	}
}

func dump(w io.Writer, dir string) error {
	raw, err := os.ReadFile(filepath.Join(dir, "state"))
	switch {
	case errors.Is(err, os.ErrNotExist):
		fmt.Fprintln(w, "state: (none)")
	case err != nil:
		return err
	default:
		var hs raft.HardState
		if err := json.Unmarshal(raw, &hs); err != nil {
			return fmt.Errorf("state: %w", err)
		}
		fmt.Fprintf(w, "state: term=%d votedFor=%q\n", hs.Term, hs.VotedFor)
	}

	lf, err := storage.ReadLog(filepath.Join(dir, "log"))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "log: %d entries, %d bytes\n", len(lf.Entries), lf.TotalBytes)
	for _, e := range lf.Entries {
		data := string(e.Data)
		if data == "" {
			data = "(no-op)"
		}
		fmt.Fprintf(w, "  %6d  term %-4d %s\n", e.Index, e.Term, data)
	}
	if lf.ValidBytes < lf.TotalBytes {
		fmt.Fprintf(w, "damaged tail: %d bytes after byte %d (%s); the node will cut them on start\n",
			lf.TotalBytes-lf.ValidBytes, lf.ValidBytes, lf.Problem)
	}
	return nil
}
