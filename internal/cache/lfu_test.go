package cache

import (
	"fmt"
	"testing"
	"time"
)

func TestLFU_BasicGetPut(t *testing.T) {
	c := NewLFU(10)

	c.Put("a", []byte("val-a"), 0)
	c.Put("b", []byte("val-b"), 0)

	val, ok := c.Get("a")
	if !ok || string(val) != "val-a" {
		t.Fatalf("expected val-a, got %s (found=%v)", val, ok)
	}
}

func TestLFU_EvictsLeastFrequent(t *testing.T) {
	c := NewLFU(3)

	c.Put("a", []byte("1"), 0)
	c.Put("b", []byte("2"), 0)
	c.Put("c", []byte("3"), 0)

	// Access "a" and "b" more than "c"
	c.Get("a")
	c.Get("a")
	c.Get("b")

	// Adding "d" should evict "c" (freq=1, least frequent)
	c.Put("d", []byte("4"), 0)

	if _, ok := c.Get("c"); ok {
		t.Error("'c' should have been evicted (least frequent)")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("'a' should still exist")
	}
	if _, ok := c.Get("d"); !ok {
		t.Error("'d' should exist")
	}
}

func TestLFU_Update(t *testing.T) {
	c := NewLFU(10)
	c.Put("key", []byte("v1"), 0)
	c.Put("key", []byte("v2"), 0)

	val, ok := c.Get("key")
	if !ok || string(val) != "v2" {
		t.Fatalf("expected v2, got %s", val)
	}
}

func TestLFU_TTLExpiry(t *testing.T) {
	c := NewLFU(10)
	c.Put("short", []byte("v"), 30*time.Millisecond)
	c.Put("long", []byte("v"), 10*time.Second)

	time.Sleep(80 * time.Millisecond)

	if _, ok := c.Get("short"); ok {
		t.Error("short TTL key should be expired")
	}
	if _, ok := c.Get("long"); !ok {
		t.Error("long TTL key should still exist")
	}
}

func TestLFU_Delete(t *testing.T) {
	c := NewLFU(10)
	c.Put("key", []byte("val"), 0)

	if !c.Delete("key") {
		t.Error("Delete should return true")
	}
	if _, ok := c.Get("key"); ok {
		t.Error("key should be gone after Delete")
	}
	if c.Delete("key") {
		t.Error("second Delete should return false")
	}
}

func TestLFU_FrequencyTracking(t *testing.T) {
	c := NewLFU(5)
	keys := []string{"k1", "k2", "k3", "k4", "k5"}
	for _, k := range keys {
		c.Put(k, []byte("v"), 0)
	}

	// Access k1 many times — it should survive eviction
	for i := 0; i < 100; i++ {
		c.Get("k1")
	}

	// Fill cache to force eviction
	for i := 0; i < 10; i++ {
		c.Put(fmt.Sprintf("new-%d", i), []byte("v"), 0)
	}

	// k1 (highest frequency) should survive
	if _, ok := c.Get("k1"); !ok {
		t.Error("k1 (highest frequency) should not have been evicted")
	}
}

func TestLFU_ZeroCapacity(t *testing.T) {
	c := NewLFU(0)
	c.Put("key", []byte("val"), 0) // should not panic
	if _, ok := c.Get("key"); ok {
		t.Error("zero capacity cache should not store anything")
	}
}
