package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTTLManager_Expiry(t *testing.T) {
	evicted := atomic.Int32{}
	mgr := NewTTLManager(func(key string) {
		evicted.Add(1)
	}, 20*time.Millisecond)
	defer mgr.Stop()

	mgr.Set("short", 30*time.Millisecond)
	mgr.Set("long", 10*time.Second)

	time.Sleep(100 * time.Millisecond)

	if evicted.Load() != 1 {
		t.Fatalf("expected 1 eviction, got %d", evicted.Load())
	}
}

func TestTTLManager_Remove(t *testing.T) {
	evicted := atomic.Int32{}
	mgr := NewTTLManager(func(key string) {
		evicted.Add(1)
	}, 20*time.Millisecond)
	defer mgr.Stop()

	mgr.Set("key", 30*time.Millisecond)
	mgr.Remove("key") // cancel before expiry

	time.Sleep(100 * time.Millisecond)

	if evicted.Load() != 0 {
		t.Fatalf("expected 0 evictions after Remove, got %d", evicted.Load())
	}
}

func TestTTLManager_Update(t *testing.T) {
	evicted := atomic.Int32{}
	mgr := NewTTLManager(func(key string) {
		evicted.Add(1)
	}, 20*time.Millisecond)
	defer mgr.Stop()

	// Set short TTL then extend it
	mgr.Set("key", 30*time.Millisecond)
	mgr.Set("key", 10*time.Second) // extend

	time.Sleep(100 * time.Millisecond)

	// Should NOT have been evicted — TTL was extended
	if evicted.Load() != 0 {
		t.Fatalf("key should not have been evicted after TTL extension")
	}
}

func TestTTLManager_MultipleKeys(t *testing.T) {
	var evictedKeys []string
	var mu sync.Mutex

	mgr := NewTTLManager(func(key string) {
		mu.Lock()
		evictedKeys = append(evictedKeys, key)
		mu.Unlock()
	}, 10*time.Millisecond)
	defer mgr.Stop()

	for i := 0; i < 10; i++ {
		mgr.Set(fmt.Sprintf("key-%d", i), 30*time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	count := len(evictedKeys)
	mu.Unlock()

	if count != 10 {
		t.Fatalf("expected 10 evictions, got %d", count)
	}
}
