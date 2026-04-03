package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level server configuration.
type Config struct {
	Storage StorageConfig `yaml:"storage"`
	Node    NodeConfig    `yaml:"node"`
	Cache   CacheConfig   `yaml:"cache"`
	Cluster ClusterConfig `yaml:"cluster"`
	Server  ServerConfig  `yaml:"server"`
}

type NodeConfig struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

type CacheConfig struct {
	Policy          string        `yaml:"policy"`           // lru or lfu
	Capacity        int           `yaml:"capacity"`         // max keys
	CleanupInterval time.Duration `yaml:"cleanup_interval"` // TTL sweep interval
}

type ClusterConfig struct {
	Peers        []string `yaml:"peers"`         // peer addresses
	VirtualNodes int      `yaml:"virtual_nodes"` // per physical node
	Replicas     int      `yaml:"replicas"`      // replication factor
}

type StorageConfig struct {
	WALDir      string `yaml:"wal_dir"`
	SnapshotDir string `yaml:"snapshot_dir"`
	WALMaxMB    int    `yaml:"wal_max_mb"`
}

type ServerConfig struct {
	GRPCPort int `yaml:"grpc_port"`
	HTTPPort int `yaml:"http_port"`
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:      "node-1",
			Address: "localhost",
		},
		Cache: CacheConfig{
			Policy:          "lru",
			Capacity:        1_000_000,
			CleanupInterval: 1 * time.Second,
		},
		Cluster: ClusterConfig{
			VirtualNodes: 150,
			Replicas:     3,
		},
		Storage: StorageConfig{
			WALDir:      "./data/wal",
			WALMaxMB:    512,
			SnapshotDir: "./data/snapshots",
		},
		Server: ServerConfig{
			GRPCPort: 7070,
			HTTPPort: 8080,
		},
	}
}

// Load reads a YAML config file and merges with defaults.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // use defaults
		}
		return nil, fmt.Errorf("config: read file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	return cfg, nil
}
