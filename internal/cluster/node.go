package cluster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// NodeStatus represents the health state of a node.
type NodeStatus string

const (
	StatusAlive    NodeStatus = "alive"
	StatusSuspect  NodeStatus = "suspect"
	StatusDead     NodeStatus = "dead"
)

// NodeInfo tracks runtime health of a cluster member.
type NodeInfo struct {
	Node
	Status      NodeStatus
	LastSeen    time.Time
	FailedPings int
}

// HealthMonitor tracks cluster node liveness using periodic heartbeats.
// A node is marked suspect after missedHeartbeats failures,
// and dead after deadThreshold failures.
type HealthMonitor struct {
	mu               sync.RWMutex
	nodes            map[string]*NodeInfo
	ring             *Ring
	logger           *zap.Logger
	heartbeatInterval time.Duration
	missedHeartbeats int
	deadThreshold    int
	stopCh           chan struct{}
	OnNodeDead       func(node Node)
	OnNodeRevived    func(node Node)
}

// NewHealthMonitor creates a health monitor for the cluster ring.
func NewHealthMonitor(ring *Ring, logger *zap.Logger) *HealthMonitor {
	return &HealthMonitor{
		nodes:            make(map[string]*NodeInfo),
		ring:             ring,
		logger:           logger,
		heartbeatInterval: 2 * time.Second,
		missedHeartbeats: 3,
		deadThreshold:    6,
		stopCh:           make(chan struct{}),
	}
}

// RegisterNode adds a node to health monitoring.
func (m *HealthMonitor) RegisterNode(node Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = &NodeInfo{
		Node:     node,
		Status:   StatusAlive,
		LastSeen: time.Now(),
	}
}

// RecordHeartbeat marks a node as alive.
func (m *HealthMonitor) RecordHeartbeat(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info, ok := m.nodes[nodeID]; ok {
		wasDeadOrSuspect := info.Status != StatusAlive
		info.LastSeen = time.Now()
		info.FailedPings = 0
		info.Status = StatusAlive
		if wasDeadOrSuspect && m.OnNodeRevived != nil {
			go m.OnNodeRevived(info.Node)
		}
	}
}

// Start begins the background health check loop.
func (m *HealthMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkAll()
		}
	}
}

// Stop halts health monitoring.
func (m *HealthMonitor) Stop() {
	close(m.stopCh)
}

// AliveNodes returns all nodes currently marked alive.
func (m *HealthMonitor) AliveNodes() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Node, 0, len(m.nodes))
	for _, info := range m.nodes {
		if info.Status == StatusAlive {
			result = append(result, info.Node)
		}
	}
	return result
}

// ClusterStatus returns a summary of node health.
func (m *HealthMonitor) ClusterStatus() map[string]NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	status := make(map[string]NodeStatus, len(m.nodes))
	for id, info := range m.nodes {
		status[id] = info.Status
	}
	return status
}

func (m *HealthMonitor) checkAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, info := range m.nodes {
		elapsed := now.Sub(info.LastSeen)
		missed := int(elapsed / m.heartbeatInterval)

		if missed >= m.deadThreshold && info.Status != StatusDead {
			info.Status = StatusDead
			m.logger.Warn("node declared dead",
				zap.String("node", info.ID),
				zap.Duration("silent_for", elapsed),
			)
			// Remove from ring and notify
			m.ring.RemoveNode(info.ID)
			if m.OnNodeDead != nil {
				go m.OnNodeDead(info.Node)
			}
		} else if missed >= m.missedHeartbeats && info.Status == StatusAlive {
			info.Status = StatusSuspect
			info.FailedPings = missed
			m.logger.Warn("node suspected",
				zap.String("node", info.ID),
				zap.Int("missed_heartbeats", missed),
			)
		}
	}
}
