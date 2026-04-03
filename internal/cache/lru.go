package cache

import (
	"container/list"
	"sync"
	"time"
)

// entry holds the key-value pair stored in the linked list.
type entry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

// LRU is a thread-safe, fixed-capacity Least Recently Used cache.
// Get and Put are both O(1) using a hashmap + doubly-linked list.
type LRU struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
	hits     int64
	misses   int64
	evictions int64
}

// NewLRU creates an LRU cache with the given capacity.
func NewLRU(capacity int) *LRU {
	return &LRU{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

// Get retrieves a value. Returns (value, true) if found and not expired.
func (c *LRU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}

	e := elem.Value.(*entry)

	// Check TTL expiry
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		c.removeElement(elem)
		c.misses++
		return nil, false
	}

	// Move to front — most recently used
	c.order.MoveToFront(elem)
	c.hits++
	return e.value, true
}

// Put inserts or updates a key. ttl=0 means no expiry.
func (c *LRU) Put(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	// Update existing
	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		e := elem.Value.(*entry)
		e.value = value
		e.expiresAt = expiresAt
		return
	}

	// Evict LRU if at capacity
	if c.order.Len() >= c.capacity {
		c.evictOldest()
	}

	e := &entry{key: key, value: value, expiresAt: expiresAt}
	elem := c.order.PushFront(e)
	c.items[key] = elem
}

// Delete removes a key from the cache.
func (c *LRU) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return false
	}
	c.removeElement(elem)
	return true
}

// Len returns the current number of items.
func (c *LRU) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order.Len()
}

// Stats returns cache hit/miss/eviction counters.
func (c *LRU) Stats() (hits, misses, evictions int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses, c.evictions
}

// HitRate returns the cache hit ratio as a float64.
func (c *LRU) HitRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := c.hits + c.misses
	if total == 0 {
		return 0
	}
	return float64(c.hits) / float64(total)
}

// Flush removes all items from the cache.
func (c *LRU) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element, c.capacity)
	c.order.Init()
}

// Keys returns all non-expired keys (for debugging/admin).
func (c *LRU) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	keys := make([]string, 0, c.order.Len())
	for elem := c.order.Front(); elem != nil; elem = elem.Next() {
		e := elem.Value.(*entry)
		if e.expiresAt.IsZero() || now.Before(e.expiresAt) {
			keys = append(keys, e.key)
		}
	}
	return keys
}

func (c *LRU) evictOldest() {
	back := c.order.Back()
	if back != nil {
		c.removeElement(back)
		c.evictions++
	}
}

func (c *LRU) removeElement(elem *list.Element) {
	c.order.Remove(elem)
	e := elem.Value.(*entry)
	delete(c.items, e.key)
}
