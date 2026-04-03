package cache

import (
	"fmt"
	"testing"
	"time"
)

func TestEngine_LRUPolicy(t *testing.T) {
	e, err := NewEngine(PolicyLRU, 100, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.Put("k1", []byte("v1"), 0)
	e.Put("k2", []byte("v2"), 5*time.Second)

	v, ok := e.Get("k1")
	if !ok || string(v) != "v1" {
		t.Fatalf("expected v1, got %s (found=%v)", v, ok)
	}

	e.Delete("k1")
	if _, ok := e.Get("k1"); ok {
		t.Error("k1 should be gone after delete")
	}
}

func TestEngine_LFUPolicy(t *testing.T) {
	e, err := NewEngine(PolicyLFU, 100, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.Put("k", []byte("v"), 0)
	v, ok := e.Get("k")
	if !ok || string(v) != "v" {
		t.Fatalf("LFU get failed: %v", ok)
	}
}

func TestEngine_InvalidPolicy(t *testing.T) {
	_, err := NewEngine("random", 100, time.Second)
	if err == nil {
		t.Error("expected error for unknown policy")
	}
}

func TestEngine_TTLWithEviction(t *testing.T) {
	e, err := NewEngine(PolicyLRU, 10, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.Put("ephemeral", []byte("data"), 30*time.Millisecond)
	e.Put("permanent", []byte("data"), 0)

	time.Sleep(80 * time.Millisecond)

	if _, ok := e.Get("ephemeral"); ok {
		t.Error("ephemeral key should have expired")
	}
	if _, ok := e.Get("permanent"); !ok {
		t.Error("permanent key should still exist")
	}
}

func TestEngine_Stats(t *testing.T) {
	e, _ := NewEngine(PolicyLRU, 100, time.Second)
	defer e.Close()

	for i := 0; i < 10; i++ {
		e.Put(fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	for i := 0; i < 5; i++ {
		e.Get(fmt.Sprintf("k%d", i)) // hits
	}
	for i := 10; i < 15; i++ {
		e.Get(fmt.Sprintf("k%d", i)) // misses
	}

	stats := e.Stats()
	if stats["hits"].(int64) != 5 {
		t.Errorf("expected 5 hits, got %v", stats["hits"])
	}
	if stats["misses"].(int64) != 5 {
		t.Errorf("expected 5 misses, got %v", stats["misses"])
	}
}

func TestEngine_Flush(t *testing.T) {
	e, _ := NewEngine(PolicyLRU, 100, time.Second)
	defer e.Close()

	for i := 0; i < 50; i++ {
		e.Put(fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	e.Flush()

	stats := e.Stats()
	if stats["size"].(int) != 0 {
		t.Errorf("expected size=0 after flush, got %v", stats["size"])
	}
}
