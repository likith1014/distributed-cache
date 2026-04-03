package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CacheMetrics holds all Prometheus metrics for the cache server.
type CacheMetrics struct {
	// Operation counters
	GetTotal    prometheus.Counter
	PutTotal    prometheus.Counter
	DeleteTotal prometheus.Counter
	HitTotal    prometheus.Counter
	MissTotal   prometheus.Counter

	// Latency histograms
	GetLatency    prometheus.Histogram
	PutLatency    prometheus.Histogram
	DeleteLatency prometheus.Histogram

	// Cache state gauges
	CacheSize     prometheus.Gauge
	EvictionTotal prometheus.Counter
	TTLExpired    prometheus.Counter

	// Cluster metrics
	NodeCount      prometheus.Gauge
	ReplicationLag prometheus.Histogram
	RaftTerm       prometheus.Gauge
	IsLeader       prometheus.Gauge

	// System metrics
	MemoryBytes    prometheus.Gauge
	GoroutineCount prometheus.Gauge
}

// NewCacheMetrics registers all metrics with Prometheus.
func NewCacheMetrics(nodeID string) *CacheMetrics {
	labels := prometheus.Labels{"node": nodeID}

	return &CacheMetrics{
		GetTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "cache_get_total",
			Help:        "Total number of GET operations",
			ConstLabels: labels,
		}),
		PutTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "cache_put_total",
			Help:        "Total number of PUT operations",
			ConstLabels: labels,
		}),
		DeleteTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "cache_delete_total",
			Help:        "Total number of DELETE operations",
			ConstLabels: labels,
		}),
		HitTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "cache_hit_total",
			Help:        "Total number of cache hits",
			ConstLabels: labels,
		}),
		MissTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "cache_miss_total",
			Help:        "Total number of cache misses",
			ConstLabels: labels,
		}),
		GetLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "cache_get_duration_seconds",
			Help:        "GET operation latency distribution",
			ConstLabels: labels,
			Buckets:     prometheus.DefBuckets,
		}),
		PutLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "cache_put_duration_seconds",
			Help:        "PUT operation latency distribution",
			ConstLabels: labels,
			Buckets:     prometheus.DefBuckets,
		}),
		DeleteLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "cache_delete_duration_seconds",
			Help:        "DELETE operation latency distribution",
			ConstLabels: labels,
			Buckets:     prometheus.DefBuckets,
		}),
		CacheSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "cache_size_keys",
			Help:        "Current number of keys in cache",
			ConstLabels: labels,
		}),
		EvictionTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "cache_eviction_total",
			Help:        "Total number of evictions",
			ConstLabels: labels,
		}),
		TTLExpired: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "cache_ttl_expired_total",
			Help:        "Total number of TTL expirations",
			ConstLabels: labels,
		}),
		NodeCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "cluster_node_count",
			Help:        "Number of alive nodes in cluster",
			ConstLabels: labels,
		}),
		ReplicationLag: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "cluster_replication_lag_seconds",
			Help:        "Replication lag from leader to followers",
			ConstLabels: labels,
			Buckets:     []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		}),
		RaftTerm: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_term",
			Help:        "Current Raft term",
			ConstLabels: labels,
		}),
		IsLeader: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_is_leader",
			Help:        "1 if this node is the Raft leader",
			ConstLabels: labels,
		}),
		MemoryBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "cache_memory_bytes",
			Help:        "Estimated memory used by cache data",
			ConstLabels: labels,
		}),
		GoroutineCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "go_goroutine_count",
			Help:        "Number of active goroutines",
			ConstLabels: labels,
		}),
	}
}
