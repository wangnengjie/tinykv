package raftstore

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/raft"
)

type applier struct {
	shouldRemove  bool
	id            uint64
	term          uint64
	region        *metapb.Region
	proposals     []*Proposal
	readProposals []*ReadProposal
	readCmds      []raft.ReadState
}

func newApplier(peer *peer) *applier {
	return &applier{
		id:     peer.PeerId(),
		region: peer.Region(),
	}
}

// call update after apply a snapshot
func (a *applier) update(region *metapb.Region) {
	a.readCmds = nil
	a.setRegion(region)
	a.notifyStale()
}

func (a *applier) destroy() {
	regionId := a.region.GetId()
	for _, prop := range a.proposals {
		NotifyReqRegionRemoved(regionId, prop.cb)
	}
	for _, prop := range a.readProposals {
		NotifyReqRegionRemoved(regionId, prop.cb)
	}
	a.readCmds = nil
	a.region = nil
	a.proposals = nil
	a.readProposals = nil
}

func (a *applier) notifyStale() {
	for _, prop := range a.proposals {
		NotifyStaleReq(a.term, prop.cb)
	}
	for _, prop := range a.readProposals {
		NotifyStaleReq(a.term, prop.cb)
	}
	a.proposals = nil
	a.readProposals = nil
}

func (a *applier) setRegion(region *metapb.Region) {
	a.region = region
}

func (a *applier) setTerm(term uint64) {
	a.term = term
}

func (a *applier) appendProposals(props []*Proposal) {
	a.proposals = append(a.proposals, props...)
}

func (a *applier) appendReadProposals(props []*ReadProposal) {
	a.readProposals = append(a.readProposals, props...)
}

func (a *applier) appendReadCmds(readCmds []raft.ReadState) {
	a.readCmds = append(a.readCmds, readCmds...)
}

func (a *applier) findCallBack(index, term uint64) *message.Callback {
	for {
		if len(a.proposals) == 0 {
			return nil
		}
		prop := a.proposals[0]
		if term < prop.term {
			return nil
		}
		a.proposals = a.proposals[1:]
		if prop.term == term && prop.index == index {
			return prop.cb
		}
		NotifyStaleReq(prop.term, prop.cb)
	}
}

func (a *applier) findReadCallBack(ctx []byte) *message.Callback {
	for {
		if len(a.readProposals) == 0 {
			return nil
		}
		prop := a.readProposals[0]
		a.readProposals = a.readProposals[1:]
		if bytes.Equal(ctx, prop.readCmd) {
			return prop.cb
		}
		NotifyStaleReq(prop.term, prop.cb)
	}
}

func (a *applier) getReadCmd(appliedIndex uint64) []raft.ReadState {
	idx := len(a.readCmds)
	for i, cmd := range a.readCmds {
		if cmd.ReadIndex > appliedIndex {
			idx = i
			break
		}
	}
	cmds := a.readCmds[:idx]
	a.readCmds = a.readCmds[idx:]
	return cmds
}
