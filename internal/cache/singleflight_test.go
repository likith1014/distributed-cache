package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleFlight_Basic(t *testing.T) {
	sf := NewSingleFlight()
	calls := atomic.Int32{}

	fn := func() ([]byte, error) {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return []byte("result"), nil
	}

	val, shared, err := sf.Do("key", fn)
	if err != nil {
		t.Fatal(err)
	}
	if shared {
		t.Error("first call should not be shared")
	}
	if string(val) != "result" {
		t.Fatalf("expected 'result', got '%s'", val)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}
}

func TestSingleFlight_Deduplication(t *testing.T) {
	sf := NewSingleFlight()
	calls := atomic.Int32{}

	fn := func() ([]byte, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond) // slow fetch
		return []byte("shared-result"), nil
	}

	const goroutines = 100
	results := make([][]byte, goroutines)
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			val, _, err := sf.Do("same-key", fn)
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", idx, err)
				return
			}
			results[idx] = val
		}(i)
	}

	wg.Wait()

	// Only ONE call should have been made to the fetch function
	if calls.Load() != 1 {
		t.Fatalf("expected 1 actual call, got %d (thundering herd not prevented)", calls.Load())
	}

	// All goroutines should have received the same result
	for i, r := range results {
		if string(r) != "shared-result" {
			t.Errorf("goroutine %d got unexpected result: %s", i, r)
		}
	}

	metrics := sf.Metrics()
	t.Logf("Total calls: %d, Deduped: %d, Ratio: %.1f%%",
		metrics.TotalCalls, metrics.DedupedCalls,
		float64(metrics.DedupedCalls)/float64(metrics.TotalCalls)*100)
}

func TestSingleFlight_DifferentKeys(t *testing.T) {
	sf := NewSingleFlight()
	calls := atomic.Int32{}

	fn := func() ([]byte, error) {
		calls.Add(1)
		return []byte("val"), nil
	}

	// Different keys should each trigger their own call
	for i := 0; i < 5; i++ {
		sf.Do(fmt.Sprintf("key-%d", i), fn)
	}

	if calls.Load() != 5 {
		t.Fatalf("expected 5 calls for 5 different keys, got %d", calls.Load())
	}
}

func TestSingleFlight_ErrorPropagation(t *testing.T) {
	sf := NewSingleFlight()
	expectedErr := fmt.Errorf("backing store unreachable")

	fn := func() ([]byte, error) {
		time.Sleep(10 * time.Millisecond)
		return nil, expectedErr
	}

	const goroutines = 10
	var wg sync.WaitGroup
	errors := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _, errors[idx] = sf.Do("error-key", fn)
		}(i)
	}
	wg.Wait()

	// All goroutines should receive the error
	for i, err := range errors {
		if err == nil {
			t.Errorf("goroutine %d: expected error, got nil", i)
		}
	}
}

func TestCacheWithFlight_LoadOnMiss(t *testing.T) {
	engine, _ := NewEngine(PolicyLRU, 1000, time.Second)
	defer engine.Close()

	loaderCalls := atomic.Int32{}
	loader := func(key string) ([]byte, error) {
		loaderCalls.Add(1)
		return []byte("from-store-" + key), nil
	}

	cache := NewCacheWithFlight(engine, loader)

	// First call — cache miss, loader called
	val, err := cache.Get("mykey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "from-store-mykey" {
		t.Fatalf("unexpected value: %s", val)
	}
	if loaderCalls.Load() != 1 {
		t.Fatalf("expected 1 loader call, got %d", loaderCalls.Load())
	}

	// Second call — cache hit, loader NOT called
	val, err = cache.Get("mykey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "from-store-mykey" {
		t.Fatalf("unexpected value on cache hit: %s", val)
	}
	if loaderCalls.Load() != 1 {
		t.Fatalf("expected still 1 loader call, got %d", loaderCalls.Load())
	}
}
