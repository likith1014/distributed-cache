package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLRU_BasicGetPut(t *testing.T) {
	c := NewLRU(10)

	c.Put("key1", []byte("value1"), 0)
	c.Put("key2", []byte("value2"), 0)

	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if string(val) != "value1" {
		t.Fatalf("expected 'value1', got '%s'", val)
	}

	_, ok = c.Get("nonexistent")
	if ok {
		t.Fatal("expected nonexistent key to not be found")
	}
}

func TestLRU_Eviction(t *testing.T) {
	c := NewLRU(3)

	c.Put("a", []byte("1"), 0)
	c.Put("b", []byte("2"), 0)
	c.Put("c", []byte("3"), 0)

	// Access "a" to make it recently used
	c.Get("a")

	// Adding "d" should evict "b" (least recently used)
	c.Put("d", []byte("4"), 0)

	if _, ok := c.Get("b"); ok {
		t.Error("expected 'b' to be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("expected 'a' to still exist")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("expected 'c' to still exist")
	}
	if _, ok := c.Get("d"); !ok {
		t.Error("expected 'd' to exist")
	}

	_, _, evictions := c.Stats()
	if evictions != 1 {
		t.Fatalf("expected 1 eviction, got %d", evictions)
	}
}

func TestLRU_TTLExpiry(t *testing.T) {
	c := NewLRU(10)

	c.Put("short", []byte("val"), 50*time.Millisecond)
	c.Put("long", []byte("val"), 10*time.Second)

	// Both exist immediately
	if _, ok := c.Get("short"); !ok {
		t.Fatal("expected 'short' to exist before TTL")
	}

	time.Sleep(100 * time.Millisecond)

	// Short TTL should be expired now
	if _, ok := c.Get("short"); ok {
		t.Error("expected 'short' to be expired")
	}
	// Long TTL should still exist
	if _, ok := c.Get("long"); !ok {
		t.Error("expected 'long' to still exist")
	}
}

func TestLRU_Update(t *testing.T) {
	c := NewLRU(10)

	c.Put("key", []byte("v1"), 0)
	c.Put("key", []byte("v2"), 0)

	val, ok := c.Get("key")
	if !ok {
		t.Fatal("key should exist")
	}
	if string(val) != "v2" {
		t.Fatalf("expected 'v2', got '%s'", val)
	}
	if c.Len() != 1 {
		t.Fatalf("expected len=1, got %d", c.Len())
	}
}

func TestLRU_Delete(t *testing.T) {
	c := NewLRU(10)
	c.Put("key", []byte("val"), 0)

	deleted := c.Delete("key")
	if !deleted {
		t.Fatal("expected Delete to return true")
	}
	if _, ok := c.Get("key"); ok {
		t.Fatal("expected key to be gone after Delete")
	}
	if c.Delete("key") {
		t.Fatal("expected second Delete to return false")
	}
}

func TestLRU_HitRate(t *testing.T) {
	c := NewLRU(10)
	c.Put("k", []byte("v"), 0)

	c.Get("k")      // hit
	c.Get("k")      // hit
	c.Get("miss1")  // miss
	c.Get("miss2")  // miss

	hits, misses, _ := c.Stats()
	if hits != 2 {
		t.Fatalf("expected 2 hits, got %d", hits)
	}
	if misses != 2 {
		t.Fatalf("expected 2 misses, got %d", misses)
	}
	if c.HitRate() != 0.5 {
		t.Fatalf("expected hit rate 0.5, got %.2f", c.HitRate())
	}
}

func TestLRU_ConcurrentAccess(t *testing.T) {
	c := NewLRU(1000)
	var wg sync.WaitGroup
	workers := 50
	opsPerWorker := 100

	// Concurrent writes
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				c.Put(key, []byte("value"), 0)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				c.Get(key)
			}
		}(i)
	}

	wg.Wait()
	// No race conditions — test passes with -race flag
}

func TestLRU_Flush(t *testing.T) {
	c := NewLRU(10)
	for i := 0; i < 5; i++ {
		c.Put(fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	if c.Len() != 5 {
		t.Fatalf("expected 5 items before flush")
	}
	c.Flush()
	if c.Len() != 0 {
		t.Fatalf("expected 0 items after flush")
	}
}

func TestLRU_CapacityOne(t *testing.T) {
	c := NewLRU(1)
	c.Put("a", []byte("1"), 0)
	c.Put("b", []byte("2"), 0)

	if _, ok := c.Get("a"); ok {
		t.Error("'a' should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("'b' should exist")
	}
}
