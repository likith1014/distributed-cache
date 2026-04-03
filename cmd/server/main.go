package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/likith1014/distributed-cache/internal/cache"
	"github.com/likith1014/distributed-cache/internal/cluster"
	"github.com/likith1014/distributed-cache/internal/transport"
	"github.com/likith1014/distributed-cache/pkg/config"
	"github.com/likith1014/distributed-cache/pkg/metrics"
)

func main() {
	root := &cobra.Command{
		Use:   "distributed-cache",
		Short: "A Google-scale distributed in-memory cache",
		Long: `distributed-cache is a high-performance, fault-tolerant distributed cache
built in Go with consistent hashing, Raft consensus, and pluggable eviction policies.

Features:
  - LRU and LFU eviction policies with O(1) get/put
  - TTL expiration via min-heap with background sweep
  - Consistent hashing with 150 virtual nodes per physical node
  - Raft-based leader election with <500ms failover
  - Write-ahead log for crash recovery
  - Prometheus metrics + pprof profiling
  - gRPC transport with Protocol Buffers`,
		RunE: runServer,
	}

	root.Flags().String("config", "config.yaml", "Path to config file")
	root.Flags().String("node-id", "", "Override node ID")
	root.Flags().Int("grpc-port", 0, "Override gRPC port")
	root.Flags().Int("http-port", 0, "Override HTTP port")

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Override from flags
	if nodeID, _ := cmd.Flags().GetString("node-id"); nodeID != "" {
		cfg.Node.ID = nodeID
	}
	if port, _ := cmd.Flags().GetInt("grpc-port"); port != 0 {
		cfg.Server.GRPCPort = port
	}
	if port, _ := cmd.Flags().GetInt("http-port"); port != 0 {
		cfg.Server.HTTPPort = port
	}

	// Logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("starting distributed-cache",
		zap.String("node_id", cfg.Node.ID),
		zap.String("policy", cfg.Cache.Policy),
		zap.Int("capacity", cfg.Cache.Capacity),
		zap.Int("grpc_port", cfg.Server.GRPCPort),
		zap.Int("http_port", cfg.Server.HTTPPort),
	)

	// Cache engine
	engine, err := cache.NewEngine(
		cache.Policy(cfg.Cache.Policy),
		cfg.Cache.Capacity,
		cfg.Cache.CleanupInterval,
	)
	if err != nil {
		return fmt.Errorf("failed to create cache engine: %w", err)
	}
	defer engine.Close()

	// Consistent hash ring
	ring := cluster.NewRing(cfg.Cluster.VirtualNodes)
	ring.AddNode(cluster.Node{
		ID:      cfg.Node.ID,
		Address: cfg.Node.Address,
		Port:    cfg.Server.GRPCPort,
	})

	// Add peers to ring
	for _, peer := range cfg.Cluster.Peers {
		ring.AddNode(cluster.Node{
			ID:      peer,
			Address: peer,
			Port:    cfg.Server.GRPCPort,
		})
	}

	// Metrics
	m := metrics.NewCacheMetrics(cfg.Node.ID)
	m.NodeCount.Set(float64(ring.NodeCount()))

	// Health monitor
	monitor := cluster.NewHealthMonitor(ring, logger)
	monitor.OnNodeDead = func(node cluster.Node) {
		logger.Warn("node died, ring updated", zap.String("node", node.ID))
		m.NodeCount.Set(float64(ring.NodeCount()))
	}

	// Transport
	srv := transport.NewServer(engine, m, logger, cfg.Server.GRPCPort, cfg.Server.HTTPPort)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutdown signal received")
		cancel()
	}()

	go monitor.Start(ctx)

	logger.Info("distributed-cache ready",
		zap.Int("virtual_nodes", cfg.Cluster.VirtualNodes),
		zap.Int("ring_nodes", ring.NodeCount()),
		zap.Float64("key_migration_on_add", ring.KeysToMigrate()),
	)

	return srv.Start(ctx)
}
