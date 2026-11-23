package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/hmzckd/quorumkv/internal/raft"
)

const (
	headerSize    = 8
	maxRecordSize = 64 << 20
)

func encodeRecord(e raft.Entry) ([]byte, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[headerSize:], payload)
	return buf, nil
}

type LogFile struct {
	Entries    []raft.Entry
	Offsets    []int64
	ValidBytes int64
	TotalBytes int64
	Problem    string
}

func ReadLog(path string) (LogFile, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return LogFile{}, nil
	}
	if err != nil {
		return LogFile{}, err
	}

	lf := LogFile{TotalBytes: int64(len(data))}
	var pos int64
	for pos < int64(len(data)) {
		rest := data[pos:]
		if len(rest) < headerSize {
			lf.Problem = fmt.Sprintf("incomplete header at byte %d", pos)
			break
		}
		size := int64(binary.BigEndian.Uint32(rest[0:4]))
		sum := binary.BigEndian.Uint32(rest[4:8])
		if size > maxRecordSize || int64(len(rest)) < headerSize+size {
			lf.Problem = fmt.Sprintf("incomplete record at byte %d", pos)
			break
		}
		payload := rest[headerSize : headerSize+size]
		if crc32.ChecksumIEEE(payload) != sum {
			lf.Problem = fmt.Sprintf("checksum mismatch at byte %d", pos)
			break
		}
		var e raft.Entry
		if err := json.Unmarshal(payload, &e); err != nil {
			lf.Problem = fmt.Sprintf("unreadable record at byte %d", pos)
			break
		}
		if n := len(lf.Entries); n > 0 && e.Index != lf.Entries[n-1].Index+1 {
			lf.Problem = fmt.Sprintf("index %d after %d at byte %d", e.Index, lf.Entries[n-1].Index, pos)
			break
		}
		lf.Entries = append(lf.Entries, e)
		lf.Offsets = append(lf.Offsets, pos)
		pos += headerSize + size
	}
	lf.ValidBytes = pos
	return lf, nil
}
