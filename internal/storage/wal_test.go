package storage

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestWAL_WriteAndReplay(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	entries := []WALEntry{
		{Op: OpPut, Key: "k1", Value: []byte("v1")},
		{Op: OpPut, Key: "k2", Value: []byte("v2"), TTL: int64(10 * time.Second)},
		{Op: OpDelete, Key: "k1"},
	}

	for _, e := range entries {
		if err := wal.Write(e); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	wal.Close()

	// Replay from disk
	wal2, _ := NewWAL(dir, 64)
	defer wal2.Close()

	var replayed []WALEntry
	if err := wal2.Replay(func(e WALEntry) error {
		replayed = append(replayed, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(replayed))
	}
	if replayed[0].Key != "k1" || string(replayed[0].Value) != "v1" {
		t.Errorf("entry 0 mismatch: %+v", replayed[0])
	}
	if replayed[2].Op != OpDelete || replayed[2].Key != "k1" {
		t.Errorf("entry 2 mismatch: %+v", replayed[2])
	}
}

func TestWAL_Rotate(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	for i := 0; i < 10; i++ {
		wal.Write(WALEntry{Op: OpPut, Key: fmt.Sprintf("k%d", i), Value: []byte("v")})
	}

	if err := wal.Rotate(); err != nil {
		t.Fatalf("rotate failed: %v", err)
	}

	// After rotate, replay should return 0 entries (new empty WAL)
	var count int
	wal.Replay(func(e WALEntry) error {
		count++
		return nil
	})
	if count != 0 {
		t.Fatalf("expected 0 entries after rotate, got %d", count)
	}

	// Old WAL should be archived
	entries, _ := os.ReadDir(dir)
	hasBackup := false
	for _, e := range entries {
		if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".bak" {
			hasBackup = true
		}
	}
	if !hasBackup {
		t.Error("expected .bak archive file after rotate")
	}
}

func TestWAL_Checksum(t *testing.T) {
	c1 := checksum("key", []byte("value"))
	c2 := checksum("key", []byte("value"))
	c3 := checksum("key", []byte("different"))

	if c1 != c2 {
		t.Error("checksum should be deterministic")
	}
	if c1 == c3 {
		t.Error("different values should have different checksums")
	}
}

func TestWAL_Sequence(t *testing.T) {
	dir := t.TempDir()
	wal, _ := NewWAL(dir, 64)
	defer wal.Close()

	for i := 0; i < 5; i++ {
		wal.Write(WALEntry{Op: OpPut, Key: fmt.Sprintf("k%d", i), Value: []byte("v")})
	}

	if wal.Sequence() != 5 {
		t.Fatalf("expected sequence=5, got %d", wal.Sequence())
	}
}
