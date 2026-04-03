package cache

import (
	"sync"
	"time"
)

// lfuEntry stores value + frequency metadata.
type lfuEntry struct {
	key       string
	value     []byte
	freq      int
	expiresAt time.Time
}

// freqBucket is a set of keys with the same access frequency.
type freqBucket struct {
	keys map[string]struct{}
}

// LFU is a thread-safe Least Frequently Used cache with O(1) operations.
// Uses the min-frequency trick: track the current minimum frequency and
// a map from frequency → set of keys at that frequency.
type LFU struct {
	mu       sync.Mutex
	capacity int
	minFreq  int
	items    map[string]*lfuEntry
	freqs    map[int]*freqBucket
	hits     int64
	misses   int64
	evictions int64
}

// NewLFU creates an LFU cache with the given capacity.
func NewLFU(capacity int) *LFU {
	return &LFU{
		capacity: capacity,
		items:    make(map[string]*lfuEntry),
		freqs:    make(map[int]*freqBucket),
	}
}

// Get retrieves a value and increments its access frequency.
func (c *LFU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}

	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		c.removeEntry(e)
		c.misses++
		return nil, false
	}

	c.incrementFreq(e)
	c.hits++
	return e.value, true
}

// Put inserts or updates a key-value pair.
func (c *LFU) Put(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capacity <= 0 {
		return
	}

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	if e, ok := c.items[key]; ok {
		e.value = value
		e.expiresAt = expiresAt
		c.incrementFreq(e)
		return
	}

	if len(c.items) >= c.capacity {
		c.evictLFU()
	}

	e := &lfuEntry{key: key, value: value, freq: 1, expiresAt: expiresAt}
	c.items[key] = e
	c.addToFreqBucket(1, key)
	c.minFreq = 1
}

// Delete removes a key.
func (c *LFU) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return false
	}
	c.removeEntry(e)
	return true
}

// Stats returns hit/miss/eviction counters.
func (c *LFU) Stats() (hits, misses, evictions int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, c.evictions
}

func (c *LFU) incrementFreq(e *lfuEntry) {
	oldFreq := e.freq
	c.removeFromFreqBucket(oldFreq, e.key)
	e.freq++
	c.addToFreqBucket(e.freq, e.key)
	if c.minFreq == oldFreq && len(c.freqs[oldFreq].keys) == 0 {
		c.minFreq++
	}
}

func (c *LFU) evictLFU() {
	bucket := c.freqs[c.minFreq]
	if bucket == nil || len(bucket.keys) == 0 {
		return
	}
	// Pick any key from the min-freq bucket
	var victim string
	for k := range bucket.keys {
		victim = k
		break
	}
	if e, ok := c.items[victim]; ok {
		c.removeEntry(e)
		c.evictions++
	}
}

func (c *LFU) removeEntry(e *lfuEntry) {
	c.removeFromFreqBucket(e.freq, e.key)
	delete(c.items, e.key)
}

func (c *LFU) addToFreqBucket(freq int, key string) {
	if c.freqs[freq] == nil {
		c.freqs[freq] = &freqBucket{keys: make(map[string]struct{})}
	}
	c.freqs[freq].keys[key] = struct{}{}
}

func (c *LFU) removeFromFreqBucket(freq int, key string) {
	if bucket, ok := c.freqs[freq]; ok {
		delete(bucket.keys, key)
		if len(bucket.keys) == 0 {
			delete(c.freqs, freq)
		}
	}
}
