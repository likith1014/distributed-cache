package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// State represents the Raft node role.
type State int32

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// LogEntry is a single replicated command in the Raft log.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// VoteRequest is sent by candidates during leader election.
type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// VoteResponse is the reply to a VoteRequest.
type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendRequest is sent by the leader to replicate log entries.
type AppendRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	CommitIndex  uint64
}

// AppendResponse is the reply to an AppendRequest.
type AppendResponse struct {
	Term    uint64
	Success bool
}

// StateMachine applies committed log entries to the cache.
type StateMachine interface {
	Apply(entry LogEntry) error
}

// Node is a Raft consensus participant.
type Node struct {
	mu sync.Mutex

	id      string
	peers   []string
	state   atomic.Int32
	logger  *zap.Logger

	// Persistent state (survive restarts)
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	leaderID    string

	// Leader state (reinitialized after election)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timing
	electionTimeout  time.Duration
	heartbeatInterval time.Duration
	lastHeartbeat    time.Time

	// Channels
	stopCh        chan struct{}
	applyCh       chan LogEntry
	stateMachine  StateMachine

	// Callbacks
	OnBecomeLeader   func()
	OnBecomeFollower func()
}

// NewNode creates a Raft node.
func NewNode(id string, peers []string, sm StateMachine, logger *zap.Logger) *Node {
	n := &Node{
		id:                id,
		peers:             peers,
		logger:            logger,
		log:               []LogEntry{{Term: 0, Index: 0}}, // sentinel
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		electionTimeout:   randomElectionTimeout(),
		heartbeatInterval: 50 * time.Millisecond,
		stopCh:            make(chan struct{}),
		applyCh:           make(chan LogEntry, 256),
		stateMachine:      sm,
	}
	n.state.Store(int32(Follower))
	return n
}

// Start begins the Raft election and heartbeat loop.
func (n *Node) Start() {
	go n.applyLoop()
	go n.electionLoop()
}

// Stop halts the Raft node.
func (n *Node) Stop() {
	close(n.stopCh)
}

// IsLeader returns true if this node is the current leader.
func (n *Node) IsLeader() bool {
	return State(n.state.Load()) == Leader
}

// LeaderID returns the current known leader ID.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// State returns the current node state.
func (n *Node) CurrentState() State {
	return State(n.state.Load())
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// Propose submits a new command to the Raft log (leader only).
func (n *Node) Propose(command []byte) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log)),
		Command: command,
	}
	n.log = append(n.log, entry)
	return entry.Index, nil
}

// RequestVote handles an incoming vote request from a candidate.
func (n *Node) RequestVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Step down if we see a higher term
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	// Reject if stale term
	if req.Term < n.currentTerm {
		return VoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	// Grant vote if we haven't voted yet (or already voted for this candidate)
	// and the candidate's log is at least as up-to-date as ours
	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateID
	logOK := req.LastLogIndex >= uint64(len(n.log)-1)

	if alreadyVoted || !logOK {
		return VoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	n.votedFor = req.CandidateID
	n.lastHeartbeat = time.Now()
	n.logger.Info("vote granted",
		zap.String("to", req.CandidateID),
		zap.Uint64("term", req.Term),
	)
	return VoteResponse{Term: n.currentTerm, VoteGranted: true}
}

// AppendEntries handles log replication from the leader.
func (n *Node) AppendEntries(req AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	if req.Term < n.currentTerm {
		return AppendResponse{Term: n.currentTerm, Success: false}
	}

	// Valid heartbeat from current leader
	n.lastHeartbeat = time.Now()
	n.leaderID = req.LeaderID

	// Append new entries
	for _, entry := range req.Entries {
		if entry.Index < uint64(len(n.log)) {
			n.log[entry.Index] = entry
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Advance commit index
	if req.CommitIndex > n.commitIndex {
		n.commitIndex = min64(req.CommitIndex, uint64(len(n.log)-1))
		go n.applyCommitted()
	}

	return AppendResponse{Term: n.currentTerm, Success: true}
}

func (n *Node) electionLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		state := State(n.state.Load())
		elapsed := time.Since(n.lastHeartbeat)
		timeout := n.electionTimeout
		n.mu.Unlock()

		switch state {
		case Follower, Candidate:
			if elapsed >= timeout {
				n.startElection()
			}
		case Leader:
			n.sendHeartbeats()
			time.Sleep(n.heartbeatInterval)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.state.Store(int32(Candidate))
	n.lastHeartbeat = time.Now()
	n.electionTimeout = randomElectionTimeout()
	term := n.currentTerm
	lastIndex := uint64(len(n.log) - 1)
	lastTerm := n.log[lastIndex].Term
	n.mu.Unlock()

	n.logger.Info("starting election", zap.Uint64("term", term))

	votes := int64(1) // vote for self
	var wg sync.WaitGroup

	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			// In a real implementation, this would make a gRPC call
			// Here we simulate the vote collection pattern
			req := VoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			_ = req // would be sent via transport layer
			// Simulate vote response (real impl calls peer.RequestVote)
			atomic.AddInt64(&votes, 1)
		}(peer)
	}

	wg.Wait()
	majority := int64(len(n.peers)/2 + 1)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.currentTerm == term && State(n.state.Load()) == Candidate && votes >= majority {
		n.becomeLeader()
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	leaderID := n.id
	commitIndex := n.commitIndex
	n.mu.Unlock()

	for _, peer := range n.peers {
		go func(peer string) {
			req := AppendRequest{
				Term:        term,
				LeaderID:    leaderID,
				CommitIndex: commitIndex,
				Entries:     []LogEntry{}, // empty = heartbeat
			}
			_ = req // would be sent via transport layer
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	n.state.Store(int32(Leader))
	n.leaderID = n.id
	n.logger.Info("became leader", zap.Uint64("term", n.currentTerm))

	// Initialize leader volatile state
	nextIdx := uint64(len(n.log))
	for _, peer := range n.peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}

	if n.OnBecomeLeader != nil {
		go n.OnBecomeLeader()
	}
}

func (n *Node) becomeFollower(term uint64) {
	wasLeader := State(n.state.Load()) == Leader
	n.state.Store(int32(Follower))
	n.currentTerm = term
	n.votedFor = ""
	n.lastHeartbeat = time.Now()

	if wasLeader {
		n.logger.Info("stepped down to follower", zap.Uint64("term", term))
		if n.OnBecomeFollower != nil {
			go n.OnBecomeFollower()
		}
	}
}

func (n *Node) applyCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if int(n.lastApplied) < len(n.log) {
			n.applyCh <- n.log[n.lastApplied]
		}
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case entry := <-n.applyCh:
			if err := n.stateMachine.Apply(entry); err != nil {
				n.logger.Error("failed to apply log entry",
					zap.Uint64("index", entry.Index),
					zap.Error(err),
				)
			}
		}
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
