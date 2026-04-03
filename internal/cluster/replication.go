package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ReplicaOp is a single replication operation queued for a follower.
type ReplicaOp struct {
	Key       string
	Value     []byte
	TTL       time.Duration
	Op        string // "put" or "delete"
	Timestamp time.Time
	Seq       uint64
}

// ReplicaClient is the interface for sending ops to a remote node.
// In production this would be a gRPC client; here it's an interface
// so it can be mocked in tests.
type ReplicaClient interface {
	Replicate(ctx context.Context, op ReplicaOp) error
	Ping(ctx context.Context) error
	NodeID() string
}

// ReplicationManager manages async replication from primary to replicas.
// Each replica gets its own in-memory queue and goroutine.
// Failed ops are retried with exponential backoff.
type ReplicationManager struct {
	mu       sync.RWMutex
	replicas map[string]*replicaWorker
	ring     *Ring
	logger   *zap.Logger
	seq      atomic.Uint64

	// Metrics
	TotalSent   atomic.Int64
	TotalFailed atomic.Int64
	QueueDepth  atomic.Int64
}

// replicaWorker handles replication to a single replica node.
type replicaWorker struct {
	client  ReplicaClient
	queue   chan ReplicaOp
	stopCh  chan struct{}
	logger  *zap.Logger
	backoff time.Duration
	sent    atomic.Int64
	failed  atomic.Int64
}

// NewReplicationManager creates a replication manager.
func NewReplicationManager(ring *Ring, logger *zap.Logger) *ReplicationManager {
	return &ReplicationManager{
		replicas: make(map[string]*replicaWorker),
		ring:     ring,
		logger:   logger,
	}
}

// AddReplica registers a replica node and starts its replication worker.
func (m *ReplicationManager) AddReplica(client ReplicaClient) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w := &replicaWorker{
		client:  client,
		queue:   make(chan ReplicaOp, 10_000),
		stopCh:  make(chan struct{}),
		logger:  m.logger,
		backoff: 50 * time.Millisecond,
	}

	m.replicas[client.NodeID()] = w
	go w.run()

	m.logger.Info("replica worker started", zap.String("node", client.NodeID()))
}

// RemoveReplica stops and removes a replica worker.
func (m *ReplicationManager) RemoveReplica(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if w, ok := m.replicas[nodeID]; ok {
		close(w.stopCh)
		delete(m.replicas, nodeID)
		m.logger.Info("replica worker stopped", zap.String("node", nodeID))
	}
}

// Replicate fans out a write operation to all replica workers.
// Non-blocking: each op is queued; the worker sends asynchronously.
func (m *ReplicationManager) Replicate(key string, value []byte, ttl time.Duration) {
	op := ReplicaOp{
		Key:       key,
		Value:     value,
		TTL:       ttl,
		Op:        "put",
		Timestamp: time.Now(),
		Seq:       m.seq.Add(1),
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	for nodeID, w := range m.replicas {
		select {
		case w.queue <- op:
			m.QueueDepth.Add(1)
		default:
			// Queue full — log and drop (replica is falling behind)
			m.logger.Warn("replica queue full, dropping op",
				zap.String("node", nodeID),
				zap.String("key", key),
			)
			m.TotalFailed.Add(1)
		}
	}
}

// ReplicateDelete fans out a delete operation to all replicas.
func (m *ReplicationManager) ReplicateDelete(key string) {
	op := ReplicaOp{
		Key:       key,
		Op:        "delete",
		Timestamp: time.Now(),
		Seq:       m.seq.Add(1),
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, w := range m.replicas {
		select {
		case w.queue <- op:
		default:
		}
	}
}

// Stats returns per-replica replication statistics.
func (m *ReplicationManager) Stats() map[string]map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]map[string]int64, len(m.replicas))
	for nodeID, w := range m.replicas {
		result[nodeID] = map[string]int64{
			"sent":        w.sent.Load(),
			"failed":      w.failed.Load(),
			"queue_depth": int64(len(w.queue)),
		}
	}
	return result
}

// Stop halts all replica workers.
func (m *ReplicationManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.replicas {
		close(w.stopCh)
	}
}

// ── replicaWorker ──────────────────────────────────────────────

func (w *replicaWorker) run() {
	for {
		select {
		case <-w.stopCh:
			return
		case op := <-w.queue:
			w.sendWithRetry(op)
		}
	}
}

func (w *replicaWorker) sendWithRetry(op ReplicaOp) {
	maxRetries := 5
	backoff := w.backoff

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := w.client.Replicate(ctx, op)
		cancel()

		if err == nil {
			w.sent.Add(1)
			return
		}

		w.logger.Warn("replication failed, retrying",
			zap.String("node", w.client.NodeID()),
			zap.String("key", op.Key),
			zap.Int("attempt", attempt+1),
			zap.Error(err),
		)

		// Exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms
		select {
		case <-w.stopCh:
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > 800*time.Millisecond {
				backoff = 800 * time.Millisecond
			}
		}
	}

	w.failed.Add(1)
	w.logger.Error("replication permanently failed after retries",
		zap.String("node", w.client.NodeID()),
		zap.String("key", op.Key),
		zap.Uint64("seq", op.Seq),
	)
}

// ── Mock ReplicaClient for testing ────────────────────────────

// MockReplicaClient is an in-memory replica for unit tests.
type MockReplicaClient struct {
	nodeID   string
	mu       sync.Mutex
	received []ReplicaOp
	failNext bool
	latency  time.Duration
}

func NewMockReplicaClient(nodeID string) *MockReplicaClient {
	return &MockReplicaClient{nodeID: nodeID}
}

func (m *MockReplicaClient) Replicate(ctx context.Context, op ReplicaOp) error {
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return fmt.Errorf("simulated failure")
	}
	m.received = append(m.received, op)
	return nil
}

func (m *MockReplicaClient) Ping(ctx context.Context) error { return nil }
func (m *MockReplicaClient) NodeID() string                 { return m.nodeID }

func (m *MockReplicaClient) Received() []ReplicaOp {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ReplicaOp, len(m.received))
	copy(out, m.received)
	return out
}

func (m *MockReplicaClient) SetFailNext() {
	m.mu.Lock()
	m.failNext = true
	m.mu.Unlock()
}
