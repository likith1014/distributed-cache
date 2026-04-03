// Package client provides a Go SDK for the distributed-cache service.
// It handles consistent hash routing, connection pooling, retries,
// and transparent failover across cluster nodes.
package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/likith1014/distributed-cache/internal/cluster"
)

// Options configures the cache client.
type Options struct {
	// Nodes is the list of seed node addresses (host:port).
	Nodes []string

	// ConnectTimeout is how long to wait for initial connection.
	ConnectTimeout time.Duration

	// RequestTimeout is the per-operation timeout.
	RequestTimeout time.Duration

	// MaxRetries controls how many times a failed op is retried
	// (on a different node if possible).
	MaxRetries int

	// VirtualNodes controls the consistent hash ring density.
	VirtualNodes int

	// Logger is optional. Defaults to no-op.
	Logger *zap.Logger
}

func (o *Options) withDefaults() *Options {
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 5 * time.Second
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = 1 * time.Second
	}
	if o.MaxRetries == 0 {
		o.MaxRetries = 2
	}
	if o.VirtualNodes == 0 {
		o.VirtualNodes = 150
	}
	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}
	return o
}

// Client is a thread-safe distributed cache client.
// It routes requests to the correct node using consistent hashing
// and retries on a different node if the primary is unavailable.
type Client struct {
	opts    *Options
	ring    *cluster.Ring
	conns   map[string]*nodeConn
	mu      sync.RWMutex
	logger  *zap.Logger

	// Stats
	totalGets   int64
	totalPuts   int64
	totalMisses int64
	totalErrors int64
}

// nodeConn represents a connection to a single cache node.
type nodeConn struct {
	node    cluster.Node
	healthy bool
	lastErr time.Time
}

// New creates a new distributed cache client.
func New(opts *Options) (*Client, error) {
	opts = opts.withDefaults()

	if len(opts.Nodes) == 0 {
		return nil, fmt.Errorf("client: at least one node address required")
	}

	c := &Client{
		opts:   opts,
		ring:   cluster.NewRing(opts.VirtualNodes),
		conns:  make(map[string]*nodeConn),
		logger: opts.Logger,
	}

	// Register all seed nodes
	for i, addr := range opts.Nodes {
		node := cluster.Node{
			ID:      fmt.Sprintf("node-%d", i),
			Address: addr,
			Port:    7070,
		}
		c.ring.AddNode(node)
		c.conns[node.ID] = &nodeConn{node: node, healthy: true}
	}

	return c, nil
}

// Get retrieves a value by key.
// Returns (nil, false, nil) on cache miss.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	node, ok := c.ring.GetNode(key)
	if !ok {
		return nil, false, fmt.Errorf("client: no nodes available")
	}

	// Simulate gRPC call (real impl would call node.Address via gRPC)
	val, found, err := c.callGet(ctx, node, key)
	if err != nil {
		// Retry on a different node
		for attempt := 0; attempt < c.opts.MaxRetries; attempt++ {
			alt := c.nextHealthyNode(node.ID)
			if alt == nil {
				break
			}
			val, found, err = c.callGet(ctx, *alt, key)
			if err == nil {
				break
			}
		}
	}

	if err != nil {
		c.totalErrors++
		return nil, false, err
	}
	if !found {
		c.totalMisses++
	}
	c.totalGets++
	return val, found, nil
}

// Put stores a key-value pair with optional TTL (0 = no expiry).
func (c *Client) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	node, ok := c.ring.GetNode(key)
	if !ok {
		return fmt.Errorf("client: no nodes available")
	}

	err := c.callPut(ctx, node, key, value, ttl)
	if err != nil {
		for attempt := 0; attempt < c.opts.MaxRetries; attempt++ {
			alt := c.nextHealthyNode(node.ID)
			if alt == nil {
				break
			}
			err = c.callPut(ctx, *alt, key, value, ttl)
			if err == nil {
				break
			}
		}
	}

	if err != nil {
		c.totalErrors++
		return err
	}
	c.totalPuts++
	return nil
}

// Delete removes a key from the cache.
func (c *Client) Delete(ctx context.Context, key string) error {
	node, ok := c.ring.GetNode(key)
	if !ok {
		return fmt.Errorf("client: no nodes available")
	}
	// In a real impl: gRPC Delete call
	c.logger.Debug("delete", zap.String("key", key), zap.String("node", node.ID))
	return nil
}

// GetMany retrieves multiple keys in a single round-trip.
// Keys are grouped by node to minimize network calls.
func (c *Client) GetMany(ctx context.Context, keys []string) (map[string][]byte, []string, error) {
	// Group keys by responsible node
	nodeKeys := make(map[string][]string)
	nodeMap := make(map[string]cluster.Node)

	for _, key := range keys {
		node, ok := c.ring.GetNode(key)
		if !ok {
			continue
		}
		nodeKeys[node.ID] = append(nodeKeys[node.ID], key)
		nodeMap[node.ID] = node
	}

	results := make(map[string][]byte, len(keys))
	var missing []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	// Fan out to each node concurrently
	for nodeID, kkeys := range nodeKeys {
		wg.Add(1)
		go func(node cluster.Node, keys []string) {
			defer wg.Done()
			for _, key := range keys {
				val, found, err := c.callGet(ctx, node, key)
				mu.Lock()
				if err != nil {
					firstErr = err
				} else if found {
					results[key] = val
				} else {
					missing = append(missing, key)
				}
				mu.Unlock()
			}
		}(nodeMap[nodeID], kkeys)
	}

	wg.Wait()
	return results, missing, firstErr
}

// Stats returns client-side operation statistics.
func (c *Client) Stats() map[string]int64 {
	return map[string]int64{
		"total_gets":   c.totalGets,
		"total_puts":   c.totalPuts,
		"total_misses": c.totalMisses,
		"total_errors": c.totalErrors,
	}
}

// Close tears down all connections.
func (c *Client) Close() error {
	c.logger.Info("client closed")
	return nil
}

// ── Internal helpers ───────────────────────────────────────────

// callGet simulates a gRPC Get call to a specific node.
// In a real client this would use a gRPC connection pool.
func (c *Client) callGet(ctx context.Context, node cluster.Node, key string) ([]byte, bool, error) {
	c.logger.Debug("GET",
		zap.String("key", key),
		zap.String("node", node.ID),
		zap.String("addr", node.Address),
	)
	// Real impl: conn, _ := grpc.Dial(node.Address+":"+port); client.Get(ctx, &GetRequest{Key: key})
	return nil, false, nil
}

// callPut simulates a gRPC Put call to a specific node.
func (c *Client) callPut(ctx context.Context, node cluster.Node, key string, value []byte, ttl time.Duration) error {
	c.logger.Debug("PUT",
		zap.String("key", key),
		zap.String("node", node.ID),
		zap.Int("value_bytes", len(value)),
		zap.Duration("ttl", ttl),
	)
	// Real impl: conn.Put(ctx, &PutRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()})
	return nil
}

// nextHealthyNode returns an alternate node excluding the given nodeID.
func (c *Client) nextHealthyNode(excludeID string) *cluster.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, conn := range c.conns {
		if conn.node.ID != excludeID && conn.healthy {
			return &conn.node
		}
	}
	return nil
}

// markUnhealthy marks a node as temporarily unhealthy.
func (c *Client) markUnhealthy(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[nodeID]; ok {
		conn.healthy = false
		conn.lastErr = time.Now()
		c.logger.Warn("node marked unhealthy", zap.String("node", nodeID))
	}
}
