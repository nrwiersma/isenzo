package util

import "testing"

func TestNewPool(t *testing.T) {
	p := NewPool(10)

	if p == nil {
		t.Fatal("expected pool, got nil")
	}

	if cap(p.pool) != 10 {
		t.Fatalf("expected pool size 10; got %d", len(p.pool))
	}
}

func TestPool_Get(t *testing.T) {
	p := NewPool(10)

	item := p.Get()
	if item != nil {
		t.Fatalf("expected nil item; got %v", item)
	}

	p.pool <- 3

	item = p.Get()
	if item != 3 {
		t.Fatalf("expected 3; got %v", item)
	}
}

func TestPool_GetWithNew(t *testing.T) {
	p := NewPool(10)
	p.New = func() interface{} { return "foo" }

	item := p.Get()
	if item != "foo" {
		t.Fatalf("expected 10 item; got %v", item)
	}
}

func TestPool_Put(t *testing.T) {
	p := NewPool(1)

	p.Put(3)

	if len(p.pool) != 1 {
		t.Fatal("expected pool to contain item; got none")
	}

	p.Put(3)
	err := recover()
	if err != nil {
		t.Fatalf("expected full pool to discard; got %v", err)
	}
}
