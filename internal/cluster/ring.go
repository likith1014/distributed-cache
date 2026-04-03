package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const defaultVirtualNodes = 150

// Node represents a physical cache server in the cluster.
type Node struct {
	ID      string
	Address string
	Port    int
}

func (n Node) String() string {
	return fmt.Sprintf("%s(%s:%d)", n.ID, n.Address, n.Port)
}

// Ring is a consistent hash ring with virtual nodes.
// Adding/removing a node only remaps ~1/N of all keys.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	ring         map[uint32]Node  // hash → node
	sorted       []uint32         // sorted ring positions
	nodes        map[string]Node  // nodeID → node
}

// NewRing creates a new consistent hash ring.
func NewRing(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = defaultVirtualNodes
	}
	return &Ring{
		virtualNodes: virtualNodes,
		ring:         make(map[uint32]Node),
		nodes:        make(map[string]Node),
	}
}

// AddNode adds a node and its virtual nodes to the ring.
func (r *Ring) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes[node.ID] = node

	for i := 0; i < r.virtualNodes; i++ {
		key := fmt.Sprintf("%s#vn%d", node.ID, i)
		hash := r.hash(key)
		r.ring[hash] = node
		r.sorted = append(r.sorted, hash)
	}

	sort.Slice(r.sorted, func(i, j int) bool {
		return r.sorted[i] < r.sorted[j]
	})
}

// RemoveNode removes a node and its virtual nodes from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.nodes, nodeID)

	// Rebuild sorted ring without this node's virtual nodes
	newSorted := make([]uint32, 0, len(r.sorted))
	for i := 0; i < r.virtualNodes; i++ {
		key := fmt.Sprintf("%s#vn%d", nodeID, i)
		hash := r.hash(key)
		delete(r.ring, hash)
	}
	for _, h := range r.sorted {
		if _, exists := r.ring[h]; exists {
			newSorted = append(newSorted, h)
		}
	}
	r.sorted = newSorted
}

// GetNode returns the node responsible for the given key.
// Uses clockwise lookup on the ring — O(log N) binary search.
func (r *Ring) GetNode(key string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sorted) == 0 {
		return Node{}, false
	}

	hash := r.hash(key)
	idx := r.search(hash)
	return r.ring[r.sorted[idx]], true
}

// GetNodes returns up to `count` distinct nodes for a key (for replication).
func (r *Ring) GetNodes(key string, count int) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sorted) == 0 || count <= 0 {
		return nil
	}

	hash := r.hash(key)
	idx := r.search(hash)

	seen := make(map[string]bool)
	result := make([]Node, 0, count)

	for i := 0; i < len(r.sorted) && len(result) < count; i++ {
		pos := (idx + i) % len(r.sorted)
		node := r.ring[r.sorted[pos]]
		if !seen[node.ID] {
			seen[node.ID] = true
			result = append(result, node)
		}
	}

	return result
}

// Nodes returns all physical nodes in the cluster.
func (r *Ring) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// NodeCount returns the number of physical nodes.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// KeysToMigrate returns the fraction of keys that would move
// when adding a new node — useful for rebalancing estimation.
func (r *Ring) KeysToMigrate() float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.nodes) == 0 {
		return 0
	}
	return 1.0 / float64(len(r.nodes))
}

// search finds the first ring position >= hash using binary search.
func (r *Ring) search(hash uint32) int {
	idx := sort.Search(len(r.sorted), func(i int) bool {
		return r.sorted[i] >= hash
	})
	if idx == len(r.sorted) {
		idx = 0 // wrap around
	}
	return idx
}

// hash computes a 32-bit hash for a string key using SHA-256.
func (r *Ring) hash(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}
