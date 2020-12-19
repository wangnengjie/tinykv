package raft

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type ReadState struct {
	ReadIndex   uint64
	ReadRequest []byte
}

type readIndexState struct {
	from uint64
	rs   ReadState
	ack  map[uint64]struct{}
}

type readOnly struct {
	pendingReadIndex      map[string]*readIndexState
	pendingReadIndexQueue []*readIndexState
}

func newReadOnly() *readOnly {
	return &readOnly{
		pendingReadIndex:      make(map[string]*readIndexState),
		pendingReadIndexQueue: make([]*readIndexState, 0),
	}
}

func (ro *readOnly) addRequest(readIndex uint64, msg *pb.Message) {
	s := string(msg.Context)
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	ris := &readIndexState{
		from: msg.From,
		rs: ReadState{
			ReadIndex:   readIndex,
			ReadRequest: msg.Context,
		},
		ack: make(map[uint64]struct{}),
	}
	ro.pendingReadIndex[s] = ris
	ro.pendingReadIndexQueue = append(ro.pendingReadIndexQueue, ris)
}

func (ro *readOnly) recvAck(peersSize int, msg *pb.Message) bool {
	ris, ok := ro.pendingReadIndex[string(msg.Context)]
	if !ok {
		return false
	}
	ris.ack[msg.From] = struct{}{}
	return len(ris.ack) > peersSize/2
}

func (ro *readOnly) advance(msg *pb.Message) []*readIndexState {
	ris, ok := ro.pendingReadIndex[string(msg.Context)]
	if !ok {
		panic("ctx not in pending map")
	}
	idx := 0
	found := false
	for i, _ris := range ro.pendingReadIndexQueue {
		if _ris == ris {
			idx = i
			found = true
			break
		}
	}
	if found {
		riss := ro.pendingReadIndexQueue[:idx+1]
		ro.pendingReadIndexQueue = ro.pendingReadIndexQueue[idx+1:]
		for _, _ris := range riss {
			delete(ro.pendingReadIndex, string(_ris.rs.ReadRequest))
		}
		return riss
	}
	return nil
}

func (ro *readOnly) lastReadRequest() []byte {
	if len(ro.pendingReadIndexQueue) == 0 {
		return nil
	}
	return util.SafeCopy(ro.pendingReadIndexQueue[len(ro.pendingReadIndexQueue)-1].rs.ReadRequest)
}
