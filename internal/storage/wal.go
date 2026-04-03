package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OpType identifies the WAL operation kind.
type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
	OpFlush  OpType = 3
)

// WALEntry is a single operation record in the write-ahead log.
type WALEntry struct {
	Op        OpType    `json:"op"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
	TTL       int64     `json:"ttl,omitempty"` // nanoseconds
	Timestamp time.Time `json:"ts"`
	Checksum  uint32    `json:"checksum"`
}

// WAL is a simple append-only write-ahead log for crash recovery.
// Every Put/Delete is written to disk before being applied to memory.
// On restart, the WAL is replayed to restore state.
type WAL struct {
	mu       sync.Mutex
	dir      string
	file     *os.File
	writer   *bufio.Writer
	sequence uint64
	size     int64
	maxSize  int64 // rotate when exceeded
}

// NewWAL opens (or creates) a WAL in the given directory.
func NewWAL(dir string, maxSizeMB int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open file: %w", err)
	}

	info, _ := f.Stat()
	return &WAL{
		dir:     dir,
		file:    f,
		writer:  bufio.NewWriterSize(f, 64*1024),
		size:    info.Size(),
		maxSize: int64(maxSizeMB) * 1024 * 1024,
	}, nil
}

// Write appends a WAL entry and syncs to disk.
func (w *WAL) Write(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry.Timestamp = time.Now()
	entry.Checksum = checksum(entry.Key, entry.Value)

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("wal: marshal: %w", err)
	}

	// Write length-prefixed record
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	if _, err := w.writer.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.writer.Write(data); err != nil {
		return err
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	w.size += int64(4 + len(data))
	w.sequence++
	return nil
}

// Replay reads all WAL entries and calls fn for each one.
// Used on startup to restore cache state.
func (w *WAL) Replay(fn func(WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := filepath.Join(w.dir, "wal.log")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		length := binary.BigEndian.Uint32(lenBuf[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			break
		}

		var entry WALEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue // skip corrupt entries
		}

		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}

// Rotate creates a new WAL file (called after a snapshot is taken).
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	// Archive old WAL
	old := filepath.Join(w.dir, "wal.log")
	archive := filepath.Join(w.dir, fmt.Sprintf("wal-%d.log.bak", time.Now().Unix()))
	_ = os.Rename(old, archive)

	// Open new WAL
	f, err := os.OpenFile(old, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.writer = bufio.NewWriterSize(f, 64*1024)
	w.size = 0
	return nil
}

// ShouldRotate returns true when the WAL exceeds maxSize.
func (w *WAL) ShouldRotate() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxSize > 0 && w.size >= w.maxSize
}

// Sequence returns the current write sequence number.
func (w *WAL) Sequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// checksum computes a simple FNV-32 checksum for data integrity.
func checksum(key string, value []byte) uint32 {
	const prime = 16777619
	hash := uint32(2166136261)
	for _, b := range []byte(key) {
		hash ^= uint32(b)
		hash *= prime
	}
	for _, b := range value {
		hash ^= uint32(b)
		hash *= prime
	}
	return hash
}
