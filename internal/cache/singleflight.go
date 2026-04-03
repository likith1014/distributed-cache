package cache

import (
	"sync"
	"time"
)

// call represents an in-flight or completed fetch for a single key.
type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
	ts  time.Time // when this call completed
}

// SingleFlight suppresses duplicate function calls for the same key.
//
// The thundering herd problem: if 1000 goroutines simultaneously
// miss the same cache key and all try to fetch from the backing store,
// the store gets hammered. SingleFlight ensures only ONE fetch happens
// — the other 999 wait and share the result.
//
// This is identical to golang.org/x/sync/singleflight but without
// the external dependency, keeping the binary lean.
type SingleFlight struct {
	mu      sync.Mutex
	inflight map[string]*call
	metrics  SingleFlightMetrics
}

// SingleFlightMetrics tracks deduplication effectiveness.
type SingleFlightMetrics struct {
	mu            sync.Mutex
	TotalCalls    int64
	DedupedCalls  int64
	ActiveFlights int64
}

func (m *SingleFlightMetrics) record(deduped bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalCalls++
	if deduped {
		m.DedupedCalls++
	}
}

// DedupeRatio returns the fraction of calls that were deduped.
func (m *SingleFlightMetrics) DedupeRatio() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.TotalCalls == 0 {
		return 0
	}
	return float64(m.DedupedCalls) / float64(m.TotalCalls)
}

// NewSingleFlight creates a SingleFlight coordinator.
func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		inflight: make(map[string]*call),
	}
}

// Do executes fn for key, or waits for an in-flight call to complete.
// Returns (value, shared, error) where shared=true means this call
// was deduplicated — it waited for another goroutine's result.
func (sf *SingleFlight) Do(key string, fn func() ([]byte, error)) ([]byte, bool, error) {
	sf.mu.Lock()

	if c, ok := sf.inflight[key]; ok {
		// Already in flight — join the waitgroup and share result
		sf.mu.Unlock()
		sf.metrics.record(true)
		c.wg.Wait()
		return c.val, true, c.err
	}

	// First caller — we do the work
	c := &call{}
	c.wg.Add(1)
	sf.inflight[key] = c
	sf.metrics.ActiveFlights++
	sf.mu.Unlock()

	sf.metrics.record(false)
	c.val, c.err = fn()
	c.ts = time.Now()
	c.wg.Done()

	sf.mu.Lock()
	delete(sf.inflight, key)
	sf.metrics.ActiveFlights--
	sf.mu.Unlock()

	return c.val, false, c.err
}

// ActiveCount returns the number of in-flight fetches.
func (sf *SingleFlight) ActiveCount() int {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return len(sf.inflight)
}

// Metrics returns deduplication statistics.
func (sf *SingleFlight) Metrics() SingleFlightMetrics {
	sf.metrics.mu.Lock()
	defer sf.metrics.mu.Unlock()
	return sf.metrics
}

// ────────────────────────────────────────────────────────────────
// CacheWithSingleFlight wraps Engine + SingleFlight to handle
// cache-aside pattern with thundering herd protection.
// ────────────────────────────────────────────────────────────────

// Loader is a function that fetches a value from the backing store
// when a cache miss occurs.
type Loader func(key string) ([]byte, error)

// CacheWithFlight combines cache lookup with single-flight dedup.
type CacheWithFlight struct {
	engine *Engine
	sf     *SingleFlight
	loader Loader
}

// NewCacheWithFlight creates a cache-aside layer with thundering herd protection.
func NewCacheWithFlight(engine *Engine, loader Loader) *CacheWithFlight {
	return &CacheWithFlight{
		engine: engine,
		sf:     NewSingleFlight(),
		loader: loader,
	}
}

// Get retrieves a value, loading from backing store on miss.
// Concurrent misses for the same key are collapsed into one load.
func (c *CacheWithFlight) Get(key string, ttl time.Duration) ([]byte, error) {
	// Fast path — cache hit, no locking needed
	if val, ok := c.engine.Get(key); ok {
		return val, nil
	}

	// Cache miss — use single-flight to prevent thundering herd
	val, shared, err := c.sf.Do(key, func() ([]byte, error) {
		// Double-check cache inside the flight to handle the window
		// between cache miss and acquiring the flight lock
		if val, ok := c.engine.Get(key); ok {
			return val, nil
		}
		// Fetch from backing store
		v, err := c.loader(key)
		if err != nil {
			return nil, err
		}
		// Populate cache for future requests
		c.engine.Put(key, v, ttl)
		return v, nil
	})

	_ = shared // true if this goroutine piggybacked on another's fetch
	return val, err
}

// FlightMetrics returns single-flight deduplication statistics.
func (c *CacheWithFlight) FlightMetrics() SingleFlightMetrics {
	return c.sf.Metrics()
}

// ActiveFlights returns how many keys are currently being fetched.
func (c *CacheWithFlight) ActiveFlights() int {
	return c.sf.ActiveCount()
}
