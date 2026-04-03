package cache

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Policy defines the eviction strategy.
type Policy string

const (
	PolicyLRU Policy = "lru"
	PolicyLFU Policy = "lfu"
)

// Engine is the unified cache interface combining eviction + TTL management.
type Engine struct {
	policy     Policy
	lru        *LRU
	lfu        *LFU
	ttl        *TTLManager
	totalOps   atomic.Int64
	totalBytes atomic.Int64
}

// NewEngine creates a cache engine with the given policy and capacity.
func NewEngine(policy Policy, capacity int, cleanupInterval time.Duration) (*Engine, error) {
	e := &Engine{policy: policy}

	switch policy {
	case PolicyLRU:
		e.lru = NewLRU(capacity)
	case PolicyLFU:
		e.lfu = NewLFU(capacity)
	default:
		return nil, fmt.Errorf("unknown eviction policy: %s", policy)
	}

	e.ttl = NewTTLManager(func(key string) {
		e.deleteInternal(key)
	}, cleanupInterval)

	return e, nil
}

// Get retrieves a value by key.
func (e *Engine) Get(key string) ([]byte, bool) {
	e.totalOps.Add(1)
	switch e.policy {
	case PolicyLRU:
		return e.lru.Get(key)
	case PolicyLFU:
		return e.lfu.Get(key)
	}
	return nil, false
}

// Put stores a key-value pair with optional TTL.
func (e *Engine) Put(key string, value []byte, ttl time.Duration) {
	e.totalOps.Add(1)
	e.totalBytes.Add(int64(len(value)))

	switch e.policy {
	case PolicyLRU:
		e.lru.Put(key, value, ttl)
	case PolicyLFU:
		e.lfu.Put(key, value, ttl)
	}

	if ttl > 0 {
		e.ttl.Set(key, ttl)
	}
}

// Delete removes a key explicitly.
func (e *Engine) Delete(key string) bool {
	e.ttl.Remove(key)
	return e.deleteInternal(key)
}

// Flush clears all keys.
func (e *Engine) Flush() {
	switch e.policy {
	case PolicyLRU:
		e.lru.Flush()
	}
}

// Stats returns engine statistics.
func (e *Engine) Stats() map[string]any {
	stats := map[string]any{
		"policy":     string(e.policy),
		"total_ops":  e.totalOps.Load(),
		"total_bytes": e.totalBytes.Load(),
	}

	switch e.policy {
	case PolicyLRU:
		hits, misses, evictions := e.lru.Stats()
		stats["hits"] = hits
		stats["misses"] = misses
		stats["evictions"] = evictions
		stats["hit_rate"] = e.lru.HitRate()
		stats["size"] = e.lru.Len()
	case PolicyLFU:
		hits, misses, evictions := e.lfu.Stats()
		stats["hits"] = hits
		stats["misses"] = misses
		stats["evictions"] = evictions
	}

	return stats
}

// Close stops background goroutines.
func (e *Engine) Close() {
	e.ttl.Stop()
}

func (e *Engine) deleteInternal(key string) bool {
	switch e.policy {
	case PolicyLRU:
		return e.lru.Delete(key)
	case PolicyLFU:
		return e.lfu.Delete(key)
	}
	return false
}
