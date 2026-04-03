package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Snapshot is a point-in-time capture of all cache key-value pairs.
type Snapshot struct {
	CreatedAt time.Time            `json:"created_at"`
	Entries   map[string]SnapEntry `json:"entries"`
	NodeID    string               `json:"node_id"`
	Version   uint64               `json:"version"`
}

// SnapEntry holds value and optional TTL deadline.
type SnapEntry struct {
	ExpiresAt time.Time `json:"exp,omitempty"`
	Value     []byte    `json:"v"`
}

// SnapshotManager handles periodic snapshot creation and WAL compaction.
// After a snapshot is taken, the WAL is rotated — log replay only needs
// to start from the snapshot rather than the beginning of time.
type SnapshotManager struct {
	wal     *WAL
	dir     string
	nodeID  string
	version uint64
	mu      sync.Mutex
}

// NewSnapshotManager creates a snapshot manager backed by the given directory.
func NewSnapshotManager(dir, nodeID string, wal *WAL) (*SnapshotManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("snapshot: create dir: %w", err)
	}
	return &SnapshotManager{
		dir:    dir,
		nodeID: nodeID,
		wal:    wal,
	}, nil
}

// Take creates a new snapshot from the current cache state.
// The caller provides a function that returns all live key-value pairs.
func (m *SnapshotManager) Take(dump func() map[string]SnapEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.version++
	snap := Snapshot{
		Version:   m.version,
		CreatedAt: time.Now(),
		NodeID:    m.nodeID,
		Entries:   dump(),
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("snapshot: marshal: %w", err)
	}

	// Write atomically: write to .tmp then rename
	path := m.snapPath(m.version)
	tmp := path + ".tmp"

	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("snapshot: write: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("snapshot: rename: %w", err)
	}

	// Rotate WAL so recovery starts from this snapshot
	if m.wal != nil {
		if err := m.wal.Rotate(); err != nil {
			return fmt.Errorf("snapshot: rotate wal: %w", err)
		}
	}

	// Clean up old snapshots (keep last 3)
	m.pruneOld(3)

	return nil
}

// Latest loads the most recent snapshot, if any.
// Returns (nil, nil) if no snapshot exists yet.
func (m *SnapshotManager) Latest() (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	files, err := m.listSnapshots()
	if err != nil || len(files) == 0 {
		return nil, err
	}

	// Most recent is last (sorted ascending by version)
	path := files[len(files)-1]
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read: %w", err)
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("snapshot: unmarshal: %w", err)
	}

	return &snap, nil
}

// Count returns the number of snapshots on disk.
func (m *SnapshotManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	files, _ := m.listSnapshots()
	return len(files)
}

// Version returns the current snapshot version counter.
func (m *SnapshotManager) Version() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version
}

func (m *SnapshotManager) snapPath(version uint64) string {
	return filepath.Join(m.dir, fmt.Sprintf("snap-%020d.json", version))
}

func (m *SnapshotManager) listSnapshots() ([]string, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			files = append(files, filepath.Join(m.dir, e.Name()))
		}
	}
	sort.Strings(files) // ascending by version number in filename
	return files, nil
}

func (m *SnapshotManager) pruneOld(keep int) {
	files, err := m.listSnapshots()
	if err != nil || len(files) <= keep {
		return
	}
	toDelete := files[:len(files)-keep]
	for _, f := range toDelete {
		os.Remove(f)
	}
}
