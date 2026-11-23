package kv

import (
	"encoding/json"
	"fmt"
)

type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (c Command) Encode() []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return data
}

type Store struct {
	data map[string]string
}

func New() *Store {
	return &Store{data: map[string]string{}}
}

func (s *Store) Apply(entry []byte) error {
	if len(entry) == 0 {
		return nil
	}
	var cmd Command
	if err := json.Unmarshal(entry, &cmd); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
	return nil
}

func (s *Store) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Len() int {
	return len(s.data)
}
