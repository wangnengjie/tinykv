// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int
	// number of election timeout
	randomElectionTimeout int
	// random
	rand *rand.Rand

	readOnly *readOnly

	readStates []ReadState

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardstate, confstate, _ := c.Storage.InitialState()
	raftlog := newLog(c.Storage)
	raftlog.committed = hardstate.Commit
	peers := c.peers
	if len(confstate.Nodes) != 0 {
		peers = confstate.Nodes
	}
	r := &Raft{
		id:               c.ID,
		Term:             hardstate.Term,
		Vote:             hardstate.Vote,
		RaftLog:          raftlog,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
		readOnly:         newReadOnly(),
	}
	for _, id := range peers {
		r.Prs[id] = &Progress{}
	}
	r.becomeFollower(r.Term, None)
	return r
}

// SoftState return the soft state of raft
func (r *Raft) SoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

// HardState return the hard state of raft.
func (r *Raft) HardState() pb.HardState {
	// HardState or &HardState?
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// send handle send pb.Message
func (r *Raft) send(m pb.Message) {
	// Todo: handle term safety
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.id == to {
		return false
	}
	pr := r.Prs[to]
	prevLogIndex := pr.Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	ents := r.RaftLog.getEntries(pr.Next)
	entries := make([]*pb.Entry, 0, len(ents))
	for i := range ents {
		entries = append(entries, &ents[i])
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.id == to {
		return
	}
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeat, Term: r.Term, To: to, Context: r.readOnly.lastReadRequest()})
}

func (r *Raft) sendReadIndexResp(ris *readIndexState) {
	if ris.from == r.id || ris.from == None {
		r.readStates = append(r.readStates, ris.rs)
	} else {
		r.send(pb.Message{MsgType: pb.MessageType_MsgReadIndexResponse, Index: ris.rs.ReadIndex, To: ris.from, Context: ris.rs.ReadRequest})
	}
}

// sendRequestVote sends a requestvote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64, lastTerm uint64, lastIndex uint64) {
	if r.id == to {
		return
	}
	r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVote, Term: r.Term, To: to, LogTerm: lastTerm, Index: lastIndex})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// reset when switch state
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader

	lastIndex := r.RaftLog.LastIndex()
	r.eachPeer(func(_ uint64, pr *Progress) {
		pr.Next = lastIndex + 1
		pr.Match = 0
	})
	r.readOnly = newReadOnly()
	// propose a noop entry
	//r.appendEntries([]*pb.Entry{{Data: nil}})
	_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: nil}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term == 0 {
		// local message
		// MsgBeat || MsgHup || MsgPropose || MsgReadIndex
	} else if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	} else if m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, Term: r.Term, To: m.From, Reject: true})
		case pb.MessageType_MsgHeartbeat:
			r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, Term: r.Term, To: m.From, Context: m.Context})
		case pb.MessageType_MsgAppend:
			r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, Term: r.Term, To: m.From, Reject: true})
		}
		return nil
	}
	// now m.Term == r.Term as we return when m.Term < r.Term
	var err error = nil
	switch r.State {
	case StateFollower:
		err = r.stepFollower(m)
	case StateCandidate:
		err = r.stepCandidate(m)
	case StateLeader:
		err = r.stepLeader(m)
	}
	return err
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgReadIndex:
		if r.Lead == None {
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgReadIndexResponse:
		r.readStates = append(r.readStates, ReadState{
			ReadIndex:   m.Index,
			ReadRequest: m.Context,
		})
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		// candidate will drop the Propose msg
		return ErrProposalDropped
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		win, lose := r.checkElection()
		if win {
			r.becomeLeader()
		} else if lose {
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.appendEntries(m.Entries)
		r.eachPeer(func(id uint64, _ *Progress) {
			r.sendAppend(id)
		})
		r.RaftLog.updateCommit(r.Term, r.Prs)
	case pb.MessageType_MsgBeat:
		r.eachPeer(func(id uint64, _ *Progress) {
			r.sendHeartbeat(id)
		})
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgReadIndex:
		commitIndexTerm, err := r.RaftLog.Term(r.RaftLog.committed)
		if err != nil || commitIndexTerm != r.Term {
			return nil
		}
		r.readOnly.addRequest(r.RaftLog.committed, &m)
		if r.readOnly.recvAck(len(r.Prs), &m) {
			riss := r.readOnly.advance(&m)
			for _, ris := range riss {
				r.sendReadIndexResp(ris)
			}
		} else {
			r.eachPeer(func(id uint64, pr *Progress) {
				r.sendHeartbeat(id)
			})
		}
	}
	return nil
}

// campaign setup an election
func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	r.eachPeer(func(id uint64, pr *Progress) {
		r.sendRequestVote(id, lastTerm, lastIndex)
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From
	index, ok := r.RaftLog.appendEntries(m.LogTerm, m.Index, m.Commit, m.Entries)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		Term:    r.Term,
		Index:   index,
		Reject:  !ok,
	})
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if m.Reject {
		pr.Next = m.Index + 1
		r.sendAppend(m.From)
	} else {
		pr.Next = m.Index + 1
		pr.Match = m.Index
		// update commit
		if r.RaftLog.updateCommit(r.Term, r.Prs) {
			// broadcast commit update
			r.eachPeer(func(id uint64, pr *Progress) {
				r.sendAppend(id)
			})
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, Term: r.Term, To: m.From, Context: m.Context})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
	if m.Context == nil || len(m.Context) == 0 {
		return
	}
	if r.readOnly.recvAck(len(r.Prs), &m) {
		riss := r.readOnly.advance(&m)
		for _, ris := range riss {
			r.sendReadIndexResp(ris)
		}
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	reject := lastTerm > m.LogTerm || (lastTerm == m.LogTerm && lastIndex > m.Index)
	if (r.Vote != None && r.Vote != m.From) || reject {
		r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Term: r.Term, Reject: true})
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Term: r.Term, Reject: false})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// traverse each peer
func (r *Raft) eachPeer(fn func(id uint64, pr *Progress)) {
	for k, v := range r.Prs {
		fn(k, v)
	}
}

// checkElection check whether a candidate win or lose the election.
// return (win, lose)
func (r *Raft) checkElection() (bool, bool) {
	countVote := 0
	countReject := 0
	for _, b := range r.votes {
		if b {
			countVote++
		} else {
			countReject++
		}
	}
	threshold := len(r.Prs) / 2
	return countVote > threshold, countReject > threshold
}

// append entries to RaftLog
//
// only used by MsgPropose and become leader
func (r *Raft) appendEntries(ents []*pb.Entry) {
	l := r.RaftLog.LastIndex() + 1
	for i := range ents {
		ents[i].Term = r.Term
		ents[i].Index = l + uint64(i)
	}
	r.RaftLog.append(ents)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}
