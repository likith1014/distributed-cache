package cache

import (
	"container/heap"
	"sync"
	"time"
)

// ttlItem is an item in the TTL min-heap.
type ttlItem struct {
	key       string
	expiresAt time.Time
	index     int
}

// ttlHeap implements heap.Interface for TTL-ordered items.
type ttlHeap []*ttlItem

func (h ttlHeap) Len() int            { return len(h) }
func (h ttlHeap) Less(i, j int) bool  { return h[i].expiresAt.Before(h[j].expiresAt) }
func (h ttlHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *ttlHeap) Push(x any) {
	n := len(*h)
	item := x.(*ttlItem)
	item.index = n
	*h = append(*h, item)
}
func (h *ttlHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// TTLManager manages key expiration using a background goroutine and min-heap.
// When a key expires, it calls the provided evict function.
type TTLManager struct {
	mu      sync.Mutex
	h       ttlHeap
	items   map[string]*ttlItem
	evict   func(key string)
	stopCh  chan struct{}
	ticker  *time.Ticker
}

// NewTTLManager creates a TTL manager that calls evict(key) on expiry.
// cleanupInterval controls how often the background sweep runs.
func NewTTLManager(evict func(key string), cleanupInterval time.Duration) *TTLManager {
	m := &TTLManager{
		h:      make(ttlHeap, 0, 256),
		items:  make(map[string]*ttlItem),
		evict:  evict,
		stopCh: make(chan struct{}),
		ticker: time.NewTicker(cleanupInterval),
	}
	heap.Init(&m.h)
	go m.runCleanup()
	return m
}

// Set registers (or updates) a TTL for a key.
func (m *TTLManager) Set(key string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	expiresAt := time.Now().Add(ttl)

	if existing, ok := m.items[key]; ok {
		existing.expiresAt = expiresAt
		heap.Fix(&m.h, existing.index)
		return
	}

	item := &ttlItem{key: key, expiresAt: expiresAt}
	heap.Push(&m.h, item)
	m.items[key] = item
}

// Remove cancels the TTL for a key (e.g. after explicit deletion).
func (m *TTLManager) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if item, ok := m.items[key]; ok {
		heap.Remove(&m.h, item.index)
		delete(m.items, key)
	}
}

// Stop halts the background cleanup goroutine.
func (m *TTLManager) Stop() {
	m.ticker.Stop()
	close(m.stopCh)
}

// ExpiredCount returns how many keys are currently past their TTL.
func (m *TTLManager) ExpiredCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	count := 0
	for _, item := range m.items {
		if now.After(item.expiresAt) {
			count++
		}
	}
	return count
}

func (m *TTLManager) runCleanup() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.sweep()
		}
	}
}

func (m *TTLManager) sweep() {
	now := time.Now()
	var expired []string

	m.mu.Lock()
	for m.h.Len() > 0 && now.After(m.h[0].expiresAt) {
		item := heap.Pop(&m.h).(*ttlItem)
		delete(m.items, item.key)
		expired = append(expired, item.key)
	}
	m.mu.Unlock()

	// Call evict outside the lock to avoid deadlock with cache mutex
	for _, key := range expired {
		m.evict(key)
	}
}
