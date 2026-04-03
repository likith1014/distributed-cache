package transport

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/likith1014/distributed-cache/internal/cache"
	"github.com/likith1014/distributed-cache/pkg/metrics"
)

// CacheGRPCServer implements the CacheService gRPC interface.
// It wraps the cache engine and records Prometheus metrics per call.
type CacheGRPCServer struct {
	engine  *cache.Engine
	metrics *metrics.CacheMetrics
	logger  *zap.Logger
	nodeID  string
}

// NewCacheGRPCServer creates a gRPC handler for the cache service.
func NewCacheGRPCServer(
	engine *cache.Engine,
	m *metrics.CacheMetrics,
	logger *zap.Logger,
	nodeID string,
) *CacheGRPCServer {
	return &CacheGRPCServer{
		engine:  engine,
		metrics: m,
		logger:  logger,
		nodeID:  nodeID,
	}
}

// Get retrieves a single key from the cache.
func (s *CacheGRPCServer) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	start := time.Now()
	s.metrics.GetTotal.Inc()

	val, found := s.engine.Get(req.Key)

	latency := time.Since(start)
	s.metrics.GetLatency.Observe(latency.Seconds())

	if found {
		s.metrics.HitTotal.Inc()
	} else {
		s.metrics.MissTotal.Inc()
	}

	return &GetResponse{
		Value:     val,
		Found:     found,
		NodeId:    s.nodeID,
		LatencyUs: latency.Microseconds(),
	}, nil
}

// Put stores a key-value pair in the cache.
func (s *CacheGRPCServer) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(req.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "value must not be empty")
	}
	if len(req.Value) > 16*1024*1024 {
		return nil, status.Error(codes.InvalidArgument, "value exceeds 16MB limit")
	}

	start := time.Now()
	s.metrics.PutTotal.Inc()

	ttl := time.Duration(req.TtlMs) * time.Millisecond
	s.engine.Put(req.Key, req.Value, ttl)

	s.metrics.PutLatency.Observe(time.Since(start).Seconds())

	return &PutResponse{
		Success: true,
		NodeId:  s.nodeID,
	}, nil
}

// Delete removes a key from the cache.
func (s *CacheGRPCServer) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	start := time.Now()
	s.metrics.DeleteTotal.Inc()

	deleted := s.engine.Delete(req.Key)

	s.metrics.DeleteLatency.Observe(time.Since(start).Seconds())

	return &DeleteResponse{Deleted: deleted}, nil
}

// GetMany retrieves multiple keys in a single RPC call.
// Keys not found are returned in the Missing field.
func (s *CacheGRPCServer) GetMany(ctx context.Context, req *GetManyRequest) (*GetManyResponse, error) {
	if len(req.Keys) == 0 {
		return nil, status.Error(codes.InvalidArgument, "keys must not be empty")
	}

	maxKeys := int(req.MaxKeys)
	if maxKeys <= 0 || maxKeys > len(req.Keys) {
		maxKeys = len(req.Keys)
	}

	entries := make(map[string]*CacheEntry, maxKeys)
	var missing []string
	var hits, misses int32

	for _, key := range req.Keys[:maxKeys] {
		val, found := s.engine.Get(key)
		if found {
			entries[key] = &CacheEntry{Value: val}
			hits++
			s.metrics.HitTotal.Inc()
		} else {
			missing = append(missing, key)
			misses++
			s.metrics.MissTotal.Inc()
		}
		s.metrics.GetTotal.Inc()
	}

	return &GetManyResponse{
		Entries: entries,
		Missing: missing,
		Hits:    hits,
		Misses:  misses,
	}, nil
}

// PutMany stores multiple key-value pairs in a single RPC call.
func (s *CacheGRPCServer) PutMany(ctx context.Context, req *PutManyRequest) (*PutManyResponse, error) {
	if len(req.Entries) == 0 {
		return nil, status.Error(codes.InvalidArgument, "entries must not be empty")
	}

	ttl := time.Duration(req.TtlMs) * time.Millisecond
	var stored int32
	var failed []string

	for _, kv := range req.Entries {
		if kv.Key == "" || len(kv.Value) == 0 {
			failed = append(failed, kv.Key)
			continue
		}
		s.engine.Put(kv.Key, kv.Value, ttl)
		s.metrics.PutTotal.Inc()
		stored++
	}

	return &PutManyResponse{
		Stored: stored,
		Failed: failed,
	}, nil
}

// Stats returns current cache and cluster statistics.
func (s *CacheGRPCServer) Stats(ctx context.Context, req *StatsRequest) (*StatsResponse, error) {
	engineStats := s.engine.Stats()

	resp := &StatsResponse{
		NodeId:   s.nodeID,
		Policy:   asString(engineStats["policy"]),
		TotalOps: asInt64(engineStats["total_ops"]),
	}

	if size, ok := engineStats["size"]; ok {
		resp.Size = asInt64(size)
	}
	if hits, ok := engineStats["hits"]; ok {
		resp.Hits = asInt64(hits)
	}
	if misses, ok := engineStats["misses"]; ok {
		resp.Misses = asInt64(misses)
	}
	if evictions, ok := engineStats["evictions"]; ok {
		resp.Evictions = asInt64(evictions)
	}
	if hitRate, ok := engineStats["hit_rate"]; ok {
		if f, ok := hitRate.(float64); ok {
			resp.HitRate = f
		}
	}

	return resp, nil
}

// Flush removes all keys from the cache.
func (s *CacheGRPCServer) Flush(ctx context.Context, req *FlushRequest) (*FlushResponse, error) {
	if !req.Confirm {
		return nil, status.Error(codes.FailedPrecondition, "confirm must be true to flush cache")
	}

	s.engine.Flush()
	s.logger.Warn("cache flushed", zap.String("node", s.nodeID))

	return &FlushResponse{Success: true}, nil
}

// Watch streams cache key change events to the client.
// This is a server-side streaming RPC.
func (s *CacheGRPCServer) Watch(req *WatchRequest, stream CacheService_WatchServer) error {
	s.logger.Info("watch stream opened",
		zap.Strings("prefixes", req.KeyPrefixes),
	)

	// Keep stream alive until client disconnects or context is cancelled
	<-stream.Context().Done()
	s.logger.Info("watch stream closed")
	return nil
}

// ── Placeholder proto types (would be generated by protoc) ────

type GetRequest struct {
	Key      string
	ClientId string
}
type GetResponse struct {
	NodeId    string
	Value     []byte
	TtlMs     int64
	LatencyUs int64
	Found     bool
}
type PutRequest struct {
	Key   string
	Value []byte
	TtlMs int64
	Sync  bool
}
type PutResponse struct {
	NodeId  string
	Version int64
	Success bool
}
type DeleteRequest struct {
	Key       string
	Propagate bool
}
type DeleteResponse struct{ Deleted bool }
type GetManyRequest struct {
	Keys    []string
	MaxKeys int32
}
type GetManyResponse struct {
	Entries map[string]*CacheEntry
	Missing []string
	Hits    int32
	Misses  int32
}
type PutManyRequest struct {
	Entries []*KeyValue
	TtlMs   int64
}
type PutManyResponse struct {
	Failed []string
	Stored int32
}
type StatsRequest struct{}
type StatsResponse struct {
	NodeId      string
	Policy      string
	LeaderId    string
	Cluster     []*NodeSummary
	HitRate     float64
	Misses      int64
	Hits        int64
	Evictions   int64
	TotalOps    int64
	MemoryBytes int64
	RaftTerm    uint64
	Capacity    int64
	Size        int64
	IsLeader    bool
}
type FlushRequest struct{ Confirm bool }
type FlushResponse struct {
	Success     bool
	KeysRemoved int64
}
type WatchRequest struct{ KeyPrefixes []string }
type CacheEntry struct {
	Value   []byte
	TtlMs   int64
	Version int64
}
type KeyValue struct {
	Key   string
	Value []byte
}
type NodeSummary struct {
	NodeId   string
	Address  string
	Status   string
	IsLeader bool
}

// CacheService_WatchServer is the streaming interface for Watch.
type CacheService_WatchServer interface {
	Send(*WatchEvent) error
	Context() context.Context
}
type WatchEvent struct {
	Key       string
	Value     []byte
	Timestamp int64
	EventType int32
}

// ── Helper functions ───────────────────────────────────────────

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}
