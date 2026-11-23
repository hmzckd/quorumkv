package raft

import "fmt"

func (r *Raft) lastIndex() uint64 {
	return r.offset + uint64(len(r.log))
}

func (r *Raft) lastTerm() uint64 {
	t, _ := r.termAt(r.lastIndex())
	return t
}

func (r *Raft) termAt(i uint64) (uint64, bool) {
	switch {
	case i == r.offset:
		return r.offsetTerm, true
	case i < r.offset || i > r.lastIndex():
		return 0, false
	default:
		return r.log[i-r.offset-1].Term, true
	}
}

func (r *Raft) entriesFrom(from uint64, max int) []Entry {
	if from > r.lastIndex() {
		return nil
	}
	start := from - r.offset - 1
	end := uint64(len(r.log))
	if max > 0 && end-start > uint64(max) {
		end = start + uint64(max)
	}
	out := make([]Entry, end-start)
	copy(out, r.log[start:end])
	return out
}

func (r *Raft) appendEntry(e Entry) {
	if e.Index != r.lastIndex()+1 {
		panic(fmt.Sprintf("append index %d after last index %d", e.Index, r.lastIndex()))
	}
	r.log = append(r.log, e)
	r.markUnstable(e.Index)
}

func (r *Raft) truncateFrom(i uint64) {
	if i <= r.commitIndex {
		panic(fmt.Sprintf("%s: refusing to truncate committed entry %d (commit %d)", r.id, i, r.commitIndex))
	}
	r.log = r.log[:i-r.offset-1]
	r.markUnstable(i)
}

func (r *Raft) markUnstable(i uint64) {
	if r.unstableFrom == 0 || i < r.unstableFrom {
		r.unstableFrom = i
	}
}

func (r *Raft) isUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != r.lastTerm() {
		return lastTerm > r.lastTerm()
	}
	return lastIndex >= r.lastIndex()
}

func (r *Raft) LogTerm(i uint64) (uint64, bool) {
	return r.termAt(i)
}
