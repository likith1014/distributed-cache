package cluster

import (
	"testing"
	"time"
)

func TestReplicationManager_BasicReplication(t *testing.T) {
	ring := NewRing(150)
	mgr := NewReplicationManager(ring, zapNop())

	replica1 := NewMockReplicaClient("replica-1")
	replica2 := NewMockReplicaClient("replica-2")

	mgr.AddReplica(replica1)
	mgr.AddReplica(replica2)

	mgr.Replicate("user:123", []byte(`{"name":"Likith"}`), 10*time.Second)
	mgr.Replicate("product:456", []byte(`{"price":99}`), 0)

	// Give async workers time to process
	time.Sleep(100 * time.Millisecond)

	for _, replica := range []*MockReplicaClient{replica1, replica2} {
		ops := replica.Received()
		if len(ops) != 2 {
			t.Fatalf("replica %s: expected 2 ops, got %d", replica.NodeID(), len(ops))
		}
		if ops[0].Key != "user:123" {
			t.Errorf("replica %s: expected key 'user:123', got '%s'", replica.NodeID(), ops[0].Key)
		}
		if string(ops[0].Value) != `{"name":"Likith"}` {
			t.Errorf("replica %s: unexpected value: %s", replica.NodeID(), ops[0].Value)
		}
	}
}

func TestReplicationManager_RetryOnFailure(t *testing.T) {
	ring := NewRing(150)
	mgr := NewReplicationManager(ring, zapNop())

	replica := NewMockReplicaClient("replica-1")
	replica.SetFailNext() // first attempt will fail

	mgr.AddReplica(replica)
	mgr.Replicate("key", []byte("value"), 0)

	// Allow retries to complete
	time.Sleep(300 * time.Millisecond)

	ops := replica.Received()
	if len(ops) != 1 {
		t.Fatalf("expected 1 successful op after retry, got %d", len(ops))
	}
}

func TestReplicationManager_DeleteReplication(t *testing.T) {
	ring := NewRing(150)
	mgr := NewReplicationManager(ring, zapNop())

	replica := NewMockReplicaClient("replica-1")
	mgr.AddReplica(replica)

	mgr.ReplicateDelete("stale-key")
	time.Sleep(100 * time.Millisecond)

	ops := replica.Received()
	if len(ops) != 1 {
		t.Fatalf("expected 1 delete op, got %d", len(ops))
	}
	if ops[0].Op != "delete" {
		t.Errorf("expected op='delete', got '%s'", ops[0].Op)
	}
	if ops[0].Key != "stale-key" {
		t.Errorf("expected key='stale-key', got '%s'", ops[0].Key)
	}
}

func TestReplicationManager_Stats(t *testing.T) {
	ring := NewRing(150)
	mgr := NewReplicationManager(ring, zapNop())

	r1 := NewMockReplicaClient("r1")
	r2 := NewMockReplicaClient("r2")
	mgr.AddReplica(r1)
	mgr.AddReplica(r2)

	for i := 0; i < 5; i++ {
		mgr.Replicate("k", []byte("v"), 0)
	}
	time.Sleep(150 * time.Millisecond)

	stats := mgr.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected stats for 2 replicas, got %d", len(stats))
	}
	for nodeID, s := range stats {
		if s["sent"] != 5 {
			t.Errorf("node %s: expected 5 sent, got %d", nodeID, s["sent"])
		}
	}
}

func TestReplicationManager_RemoveReplica(t *testing.T) {
	ring := NewRing(150)
	mgr := NewReplicationManager(ring, zapNop())

	replica := NewMockReplicaClient("replica-1")
	mgr.AddReplica(replica)
	mgr.RemoveReplica("replica-1")

	// After removal, replication should not reach the replica
	mgr.Replicate("key", []byte("value"), 0)
	time.Sleep(100 * time.Millisecond)

	if len(replica.Received()) != 0 {
		t.Error("expected no ops after replica removal")
	}
}

// zapNop returns a no-op zap logger for tests.
func zapNop() *zap.Logger {
	return zap.NewNop()
}
