package cluster

import (
	"fmt"
	"testing"
)

func TestRing_AddAndGet(t *testing.T) {
	ring := NewRing(150)

	nodeA := Node{ID: "node-a", Address: "10.0.0.1", Port: 7070}
	nodeB := Node{ID: "node-b", Address: "10.0.0.2", Port: 7070}
	nodeC := Node{ID: "node-c", Address: "10.0.0.3", Port: 7070}

	ring.AddNode(nodeA)
	ring.AddNode(nodeB)
	ring.AddNode(nodeC)

	if ring.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got %d", ring.NodeCount())
	}

	// Every key should map to a node
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, ok := ring.GetNode(key)
		if !ok {
			t.Fatalf("key %s returned no node", key)
		}
		if node.ID == "" {
			t.Fatalf("key %s returned empty node ID", key)
		}
	}
}

func TestRing_Deterministic(t *testing.T) {
	ring := NewRing(150)
	ring.AddNode(Node{ID: "n1", Address: "10.0.0.1", Port: 7070})
	ring.AddNode(Node{ID: "n2", Address: "10.0.0.2", Port: 7070})
	ring.AddNode(Node{ID: "n3", Address: "10.0.0.3", Port: 7070})

	// Same key must always map to same node
	key := "consistent-key"
	first, _ := ring.GetNode(key)
	for i := 0; i < 100; i++ {
		node, _ := ring.GetNode(key)
		if node.ID != first.ID {
			t.Fatalf("non-deterministic: got %s then %s", first.ID, node.ID)
		}
	}
}

func TestRing_MinimalDisruption(t *testing.T) {
	ring := NewRing(150)
	nodes := []Node{
		{ID: "n1", Address: "10.0.0.1", Port: 7070},
		{ID: "n2", Address: "10.0.0.2", Port: 7070},
		{ID: "n3", Address: "10.0.0.3", Port: 7070},
	}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	// Record key assignments before adding node
	keys := make([]string, 1000)
	before := make(map[string]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		node, _ := ring.GetNode(keys[i])
		before[keys[i]] = node.ID
	}

	// Add a 4th node
	ring.AddNode(Node{ID: "n4", Address: "10.0.0.4", Port: 7070})

	// Count how many keys moved
	moved := 0
	for _, key := range keys {
		node, _ := ring.GetNode(key)
		if node.ID != before[key] {
			moved++
		}
	}

	// With 4 nodes, expect ~25% of keys to move (1/4)
	// Allow 10% tolerance
	ratio := float64(moved) / float64(len(keys))
	if ratio > 0.35 {
		t.Errorf("too many keys moved on node add: %.1f%% (expected ~25%%)", ratio*100)
	}
	t.Logf("Key migration on node add: %.1f%% (%d/%d keys)", ratio*100, moved, len(keys))
}

func TestRing_RemoveNode(t *testing.T) {
	ring := NewRing(150)
	ring.AddNode(Node{ID: "n1", Address: "10.0.0.1", Port: 7070})
	ring.AddNode(Node{ID: "n2", Address: "10.0.0.2", Port: 7070})
	ring.AddNode(Node{ID: "n3", Address: "10.0.0.3", Port: 7070})

	ring.RemoveNode("n2")

	if ring.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes after removal, got %d", ring.NodeCount())
	}

	// No key should route to removed node
	for i := 0; i < 1000; i++ {
		node, ok := ring.GetNode(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("expected node for key")
		}
		if node.ID == "n2" {
			t.Fatalf("key routed to removed node n2")
		}
	}
}

func TestRing_GetNodes_Replication(t *testing.T) {
	ring := NewRing(150)
	for i := 1; i <= 5; i++ {
		ring.AddNode(Node{ID: fmt.Sprintf("n%d", i), Address: "10.0.0.1", Port: 7070 + i})
	}

	// Request 3 replicas for a key
	nodes := ring.GetNodes("replicated-key", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// All must be distinct physical nodes
	seen := map[string]bool{}
	for _, n := range nodes {
		if seen[n.ID] {
			t.Fatalf("duplicate node in replica set: %s", n.ID)
		}
		seen[n.ID] = true
	}
}

func TestRing_EmptyRing(t *testing.T) {
	ring := NewRing(150)
	_, ok := ring.GetNode("any-key")
	if ok {
		t.Fatal("expected empty ring to return no node")
	}
}

func TestRing_Distribution(t *testing.T) {
	ring := NewRing(150)
	nodeIDs := []string{"n1", "n2", "n3", "n4"}
	for _, id := range nodeIDs {
		ring.AddNode(Node{ID: id, Address: "10.0.0.1", Port: 7070})
	}

	counts := make(map[string]int)
	total := 10_000
	for i := 0; i < total; i++ {
		node, _ := ring.GetNode(fmt.Sprintf("key-%d", i))
		counts[node.ID]++
	}

	// Each node should handle roughly 25% of keys
	// Allow 10% variance
	for _, id := range nodeIDs {
		ratio := float64(counts[id]) / float64(total)
		if ratio < 0.15 || ratio > 0.35 {
			t.Errorf("node %s has uneven load: %.1f%% (expected ~25%%)", id, ratio*100)
		}
		t.Logf("Node %s: %d keys (%.1f%%)", id, counts[id], ratio*100)
	}
}
