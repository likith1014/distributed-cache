package bench

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/likith1014/distributed-cache/internal/cache"
	"github.com/likith1014/distributed-cache/internal/cluster"
)

// BenchmarkLRUGet measures single-threaded LRU read throughput.
func BenchmarkLRUGet(b *testing.B) {
	c := cache.NewLRU(1_000_000)
	for i := 0; i < 10_000; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte("value"), 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get(fmt.Sprintf("key-%d", rand.Intn(10_000)))
		}
	})
}

// BenchmarkLRUPut measures LRU write throughput.
func BenchmarkLRUPut(b *testing.B) {
	c := cache.NewLRU(1_000_000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Put(fmt.Sprintf("key-%d", i), []byte("value-data-here"), 0)
			i++
		}
	})
}

// BenchmarkLFUGet measures LFU read throughput.
func BenchmarkLFUGet(b *testing.B) {
	c := cache.NewLFU(1_000_000)
	for i := 0; i < 10_000; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte("value"), 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get(fmt.Sprintf("key-%d", rand.Intn(10_000)))
		}
	})
}

// BenchmarkConsistentHash measures ring lookup throughput.
func BenchmarkConsistentHash(b *testing.B) {
	ring := cluster.NewRing(150)
	for i := 0; i < 10; i++ {
		ring.AddNode(cluster.Node{
			ID:      fmt.Sprintf("node-%d", i),
			Address: "127.0.0.1",
			Port:    7070 + i,
		})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ring.GetNode(fmt.Sprintf("key-%d", rand.Int63()))
		}
	})
}

// ThroughputTest runs a sustained throughput test targeting 1M ops/sec.
// Not a Go benchmark — call this as a standalone load test.
func ThroughputTest(duration time.Duration, concurrency int, capacity int) ThroughputResult {
	engine, _ := cache.NewEngine(cache.PolicyLRU, capacity, time.Second)
	defer engine.Close()

	// Pre-populate 10% of capacity
	for i := 0; i < capacity/10; i++ {
		engine.Put(fmt.Sprintf("key-%d", i), []byte("initial-value"), 0)
	}

	var (
		ops    atomic.Int64
		errors atomic.Int64
		start  = time.Now()
		stopCh = make(chan struct{})
	)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}

				key := fmt.Sprintf("key-%d", rand.Intn(capacity))

				// 80% reads, 20% writes (typical cache workload)
				if rand.Intn(100) < 80 {
					engine.Get(key)
				} else {
					engine.Put(key, []byte("benchmark-value"), 10*time.Second)
				}
				ops.Add(1)
			}
		}(i)
	}

	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	elapsed := time.Since(start)
	totalOps := ops.Load()

	return ThroughputResult{
		Duration:    elapsed,
		TotalOps:    totalOps,
		OpsPerSec:   float64(totalOps) / elapsed.Seconds(),
		Errors:      errors.Load(),
		Concurrency: concurrency,
	}
}

// ThroughputResult holds the outcome of a throughput test.
type ThroughputResult struct {
	Duration    time.Duration
	TotalOps    int64
	OpsPerSec   float64
	Errors      int64
	Concurrency int
}

func (r ThroughputResult) String() string {
	return fmt.Sprintf(
		"ops/sec: %.0f | total: %d | duration: %s | concurrency: %d | errors: %d",
		r.OpsPerSec, r.TotalOps, r.Duration.Round(time.Millisecond), r.Concurrency, r.Errors,
	)
}
