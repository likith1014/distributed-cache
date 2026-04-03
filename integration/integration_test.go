package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/likith1014/distributed-cache/internal/cache"
	"github.com/likith1014/distributed-cache/internal/cluster"
	"github.com/likith1014/distributed-cache/internal/storage"
	"os"
)

// TestIntegration_CacheWithReplication tests the full stack:
// cache engine + replication manager + consistent hash routing.
func TestIntegration_CacheWithReplication(t *testing.T) {
	// Build a 3-node simulated cluster in memory
	nodes := make([]*cache.Engine, 3)
	replicas := make([]*cluster.MockReplicaClient, 3)
	mgrs := make([]*cluster.ReplicationManager, 3)
	ring := cluster.NewRing(150)

	for i := 0; i < 3; i++ {
		engine, err := cache.NewEngine(cache.PolicyLRU, 10_000, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()
		nodes[i] = engine

		node := cluster.Node{ID: fmt.Sprintf("node-%d", i), Address: "127.0.0.1", Port: 7070 + i}
		ring.AddNode(node)
		replicas[i] = cluster.NewMockReplicaClient(node.ID)
	}

	// Wire replication: each node replicates to the other two
	for i := 0; i < 3; i++ {
		mgrs[i] = cluster.NewReplicationManager(ring, zap.NewNop())
		for j := 0; j < 3; j++ {
			if j != i {
				mgrs[i].AddReplica(replicas[j])
			}
		}
	}
	defer func() {
		for _, m := range mgrs {
			m.Stop()
		}
	}()

	// Write 1000 keys to node-0 with replication
	t.Run("write_and_replicate", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key-%d", i)
			val := []byte(fmt.Sprintf("value-%d", i))
			nodes[0].Put(key, val, 10*time.Second)
			mgrs[0].Replicate(key, val, 10*time.Second)
		}

		// Allow async replication to complete
		time.Sleep(200 * time.Millisecond)

		// Verify replicas received all ops
		for i := 1; i < 3; i++ {
			ops := replicas[i].Received()
			if len(ops) != 1000 {
				t.Errorf("replica %d: expected 1000 ops, got %d", i, len(ops))
			}
		}
	})

	// Verify consistent hash ring routes the same key to the same node
	t.Run("consistent_routing", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("routing-key-%d", i)
			n1, _ := ring.GetNode(key)
			n2, _ := ring.GetNode(key)
			if n1.ID != n2.ID {
				t.Errorf("key %s routed to different nodes: %s vs %s", key, n1.ID, n2.ID)
			}
		}
	})
}

// TestIntegration_CacheWithWAL tests crash recovery via WAL replay.
func TestIntegration_CacheWithWAL(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write data and record in WAL
	wal, err := storage.NewWAL(dir, 64)
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := cache.NewEngine(cache.PolicyLRU, 10_000, time.Second)

	writes := []struct{ key, val string }{
		{"user:1", `{"name":"Alice"}`},
		{"user:2", `{"name":"Bob"}`},
		{"product:1", `{"price":99}`},
	}

	for _, w := range writes {
		engine.Put(w.key, []byte(w.val), 0)
		wal.Write(storage.WALEntry{
			Op:    storage.OpPut,
			Key:   w.key,
			Value: []byte(w.val),
		})
	}

	// Delete one key
	engine.Delete("user:2")
	wal.Write(storage.WALEntry{Op: storage.OpDelete, Key: "user:2"})
	wal.Close()
	engine.Close()

	// Phase 2: Simulate restart — replay WAL into fresh engine
	engine2, _ := cache.NewEngine(cache.PolicyLRU, 10_000, time.Second)
	defer engine2.Close()

	wal2, _ := storage.NewWAL(dir, 64)
	defer wal2.Close()

	if err := wal2.Replay(func(e storage.WALEntry) error {
		switch e.Op {
		case storage.OpPut:
			engine2.Put(e.Key, e.Value, 0)
		case storage.OpDelete:
			engine2.Delete(e.Key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Verify state after replay
	if val, ok := engine2.Get("user:1"); !ok || string(val) != `{"name":"Alice"}` {
		t.Errorf("user:1 not recovered correctly: found=%v val=%s", ok, val)
	}
	if _, ok := engine2.Get("user:2"); ok {
		t.Error("user:2 should have been deleted")
	}
	if val, ok := engine2.Get("product:1"); !ok || string(val) != `{"price":99}` {
		t.Errorf("product:1 not recovered correctly: found=%v val=%s", ok, val)
	}
}

// TestIntegration_ThunderingHerd verifies single-flight under concurrent load.
func TestIntegration_ThunderingHerd(t *testing.T) {
	engine, _ := cache.NewEngine(cache.PolicyLRU, 10_000, time.Second)
	defer engine.Close()

	loaderCalls := atomic.Int64{}
	loader := func(key string) ([]byte, error) {
		loaderCalls.Add(1)
		time.Sleep(50 * time.Millisecond) // simulate slow DB query
		return []byte("expensive-result"), nil
	}

	cacheWithFlight := cache.NewCacheWithFlight(engine, loader)

	// 500 goroutines all miss the same key simultaneously
	const goroutines = 500
	var wg sync.WaitGroup
	results := make([][]byte, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			val, err := cacheWithFlight.Get("hot-key", 10*time.Second)
			if err != nil {
				t.Errorf("goroutine %d: error: %v", idx, err)
				return
			}
			results[idx] = val
		}(i)
	}
	wg.Wait()

	// Exactly 1 loader call despite 500 concurrent misses
	if loaderCalls.Load() != 1 {
		t.Fatalf("thundering herd not prevented: %d loader calls (expected 1)", loaderCalls.Load())
	}

	// All goroutines got the correct result
	for i, r := range results {
		if string(r) != "expensive-result" {
			t.Errorf("goroutine %d: unexpected result: %s", i, r)
		}
	}

	metrics := cacheWithFlight.FlightMetrics()
	t.Logf("Single-flight: %d total, %d deduped (%.1f%% dedup rate)",
		metrics.TotalCalls, metrics.DedupedCalls,
		float64(metrics.DedupedCalls)/float64(metrics.TotalCalls)*100,
	)
}

// TestIntegration_LRUEvictionUnderLoad verifies eviction under sustained write pressure.
func TestIntegration_LRUEvictionUnderLoad(t *testing.T) {
	capacity := 1000
	engine, _ := cache.NewEngine(cache.PolicyLRU, capacity, time.Second)
	defer engine.Close()

	// Write 3x capacity — only the most recent ~1000 should survive
	total := capacity * 3
	for i := 0; i < total; i++ {
		engine.Put(fmt.Sprintf("key-%d", i), []byte("v"), 0)
	}

	stats := engine.Stats()
	size := stats["size"].(int)
	if size > capacity {
		t.Errorf("cache exceeded capacity: size=%d capacity=%d", size, capacity)
	}

	// Recent keys should still be there
	for i := total - 100; i < total; i++ {
		if _, ok := engine.Get(fmt.Sprintf("key-%d", i)); !ok {
			t.Errorf("recent key key-%d not found", i)
		}
	}
}
