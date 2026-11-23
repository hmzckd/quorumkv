package kv

import "testing"

func TestApplyPutAndDelete(t *testing.T) {
	s := New()
	steps := []Command{
		{Op: OpPut, Key: "x", Value: "1"},
		{Op: OpPut, Key: "y", Value: "2"},
		{Op: OpPut, Key: "x", Value: "3"},
		{Op: OpDelete, Key: "y"},
		{Op: OpDelete, Key: "missing"},
	}
	for _, c := range steps {
		if err := s.Apply(c.Encode()); err != nil {
			t.Fatalf("apply %+v: %v", c, err)
		}
	}
	if v, ok := s.Get("x"); !ok || v != "3" {
		t.Errorf("x = %q, %v; want 3", v, ok)
	}
	if _, ok := s.Get("y"); ok {
		t.Error("y still exists after delete")
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d, want 1", s.Len())
	}
}

func TestApplyingTwiceGivesTheSameResult(t *testing.T) {
	once, twice := New(), New()
	put := Command{Op: OpPut, Key: "x", Value: "5"}.Encode()
	once.Apply(put)
	twice.Apply(put)
	twice.Apply(put)

	a, _ := once.Get("x")
	b, _ := twice.Get("x")
	if a != b || once.Len() != twice.Len() {
		t.Fatalf("put is not idempotent: %q vs %q", a, b)
	}
}

func TestNoopEntryChangesNothing(t *testing.T) {
	s := New()
	if err := s.Apply(nil); err != nil || s.Len() != 0 {
		t.Fatalf("no-op: err %v, len %d", err, s.Len())
	}
}

func TestBadCommandsAreRejected(t *testing.T) {
	s := New()
	for _, raw := range []string{"{", `{"op":"increment","key":"x"}`} {
		if err := s.Apply([]byte(raw)); err == nil {
			t.Errorf("Apply(%s) = nil, want an error", raw)
		}
	}
}
