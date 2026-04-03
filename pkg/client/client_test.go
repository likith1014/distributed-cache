package client

import (
	"context"
	"testing"
	"time"
)

func TestClient_New(t *testing.T) {
	c, err := New(&Options{
		Nodes: []string{"localhost:7070", "localhost:7071", "localhost:7072"},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	if c.ring.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes in ring, got %d", c.ring.NodeCount())
	}
}

func TestClient_EmptyNodes(t *testing.T) {
	_, err := New(&Options{Nodes: []string{}})
	if err == nil {
		t.Fatal("expected error with no nodes")
	}
}

func TestClient_GetMany_KeyGrouping(t *testing.T) {
	c, _ := New(&Options{
		Nodes: []string{"localhost:7070", "localhost:7071", "localhost:7072"},
	})
	defer c.Close()

	// GetMany should not error even with many keys
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26))
	}

	results, missing, err := c.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany error: %v", err)
	}
	// All keys miss since nodes aren't real
	_ = results
	_ = missing
}

func TestClient_Stats(t *testing.T) {
	c, _ := New(&Options{Nodes: []string{"localhost:7070"}})
	defer c.Close()

	stats := c.Stats()
	if stats["total_gets"] != 0 {
		t.Error("expected 0 gets initially")
	}
}

func TestClient_DefaultOptions(t *testing.T) {
	opts := (&Options{}).withDefaults()
	if opts.RequestTimeout == 0 {
		t.Error("expected non-zero request timeout")
	}
	if opts.MaxRetries == 0 {
		t.Error("expected non-zero max retries")
	}
	if opts.VirtualNodes == 0 {
		t.Error("expected non-zero virtual nodes")
	}
}
