package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

// move MsgApply & MsgApplyRes here to avoid cycle import
type MsgApply struct {
	Update        bool
	Term          uint64
	Region        *metapb.Region
	Proposals     []*Proposal
	ReadProposals []*ReadProposal
	ReadCmds      []raft.ReadState
	CommitEntries []eraftpb.Entry
}

type MsgApplyRes struct {
	ApplyState   *rspb.RaftApplyState
	SizeDiffHint int64
	ProcessRes   []ProcessResult
}

type applyMsgHandler struct {
	applier *applier
	router  *router
	engine  *engine_util.Engines
	ctx     applyContext
}

type applyContext struct {
	res  MsgApplyRes
	kvWB engine_util.WriteBatch
	cbs  []*message.Callback
}

type ProcessResType int

const (
	ProcessTypeCompactRes ProcessResType = iota
	ProcessTypeConfChangeRes
	ProcessTypeSplitRes
)

type ProcessResult struct {
	Type   ProcessResType
	paylod interface{}
}

type ProcessCompactRes struct {
	firstIndex     uint64
	truncatedIndex uint64
}

type ProcessConfChangeRes struct {
	region     *metapb.Region
	confChange *eraftpb.ConfChange
	peer       *metapb.Peer
}

type ProcessSplitRegionRes struct {
	region    *metapb.Region
	newRegion *metapb.Region
}

func newApplyMsgHandler(applier *applier, router *router, engine *engine_util.Engines) *applyMsgHandler {
	state, _ := meta.GetApplyState(engine.Kv, applier.region.Id)
	return &applyMsgHandler{
		applier: applier,
		router:  router,
		engine:  engine,
		ctx: applyContext{
			res: MsgApplyRes{
				ApplyState: state,
			},
		},
	}
}

func (a *applyMsgHandler) HandleApplyMsg(msg *MsgApply) {
	// after apply a snapshot, Update will set to true.
	// We ensure that every penging apply task for that region
	// will be done before apply the snapshot.
	if msg.Update {
		a.applier.update(msg.Term)
	}
	a.applier.setRegion(msg.Region)
	a.applier.appendProposals(msg.Proposals)
	a.applier.appendReadProposals(msg.ReadProposals)
	a.applier.appendReadCmds(msg.ReadCmds)
	if a.applier.shouldRemove {
		a.applier.notifyStale(msg.Term)
		return
	}
	for _, entry := range msg.CommitEntries {
		if a.ctx.res.ApplyState.AppliedIndex+1 != entry.Index {
			panic(fmt.Sprintf("want index %d but get %d", a.ctx.res.ApplyState.AppliedIndex, entry.Index))
		}
		a.process(&entry)
		a.ctx.res.ApplyState.AppliedIndex = entry.Index
		if a.applier.shouldRemove {
			break
		}
	}
	// set RaftApplyState
	err := a.ctx.kvWB.SetMeta(meta.ApplyStateKey(a.applier.region.GetId()), a.ctx.res.ApplyState)
	if err != nil {
		panic(err)
	}
	state := rspb.PeerState_Normal
	if a.applier.shouldRemove {
		state = rspb.PeerState_Tombstone
	}
	// set RegionState
	meta.WriteRegionState(&a.ctx.kvWB, a.applier.region, state)
	// write to db
	err = a.ctx.kvWB.WriteToDB(a.engine.Kv)
	if err != nil {
		panic(err)
	}
	// read command should be processed after write to db
	a.processReadCmds(a.ctx.res.ApplyState.AppliedIndex)
	a.ctx.kvWB.Reset()
	a.ctx.doneCbs()
	err = a.router.send(a.applier.region.GetId(), message.Msg{
		Type: message.MsgTypeApplyRes,
		Data: &a.ctx.res,
	})
	if err != nil {
		panic(err)
	}
}

func (a *applyMsgHandler) process(entry *eraftpb.Entry) {
	if entry.Data == nil {
		return
	}
	req := &raft_cmdpb.RaftCmdRequest{}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		var confChangeEntry eraftpb.ConfChange
		if err := confChangeEntry.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
		if err := req.Unmarshal(confChangeEntry.Context); err != nil {
			panic(err)
		}
	} else {
		if err := req.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
	}
	// check region epoch here
	if err := util.CheckRegionEpoch(req, a.applier.region, true); err != nil {
		cb := a.applier.findCallBack(entry.Index, entry.Term)
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}
	if req.AdminRequest != nil {

	} else {
		a.processWriteCmd(entry, req)
	}
}

func (a *applyMsgHandler) processWriteCmd(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest) {
	cb := a.applier.findCallBack(entry.Index, entry.Term)
	resp := newCmdResp()
	// check key in region, since there might be region split request when processing cmd
	for _, r := range req.Requests {
		if key := util.GetKeyInRequest(r); key != nil {
			if err := util.CheckKeyInRegion(key, a.applier.region); err != nil {
				if cb != nil {
					BindRespError(resp, err)
					cb.Done(nil)
				}
				return
			}
		}
	}
	for _, cmd := range req.Requests {
		switch cmd.CmdType {
		case raft_cmdpb.CmdType_Put:
			a.ctx.res.SizeDiffHint += int64(len(cmd.Put.Key) + len(cmd.Put.Value) + len(cmd.Put.Cf))
			a.ctx.kvWB.SetCF(cmd.Put.Cf, cmd.Put.Key, cmd.Put.Value)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}})
		case raft_cmdpb.CmdType_Delete:
			a.ctx.res.SizeDiffHint -= int64(len(cmd.Delete.Cf) + len(cmd.Delete.Key))
			a.ctx.kvWB.DeleteCF(cmd.Delete.Cf, cmd.Delete.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}})
		}
	}
	if cb != nil {
		cb.Resp = resp
		a.ctx.appendCallBack(cb)
	}
}

func (a *applyMsgHandler) processReadCmds(appliedIndex uint64) {
	readCmds := a.applier.getReadCmd(appliedIndex)
LOOP:
	for i, _ := range readCmds {
		cb := a.applier.findReadCallBack(readCmds[i].ReadRequest)
		if cb == nil {
			continue
		}
		req := raft_cmdpb.RaftCmdRequest{}
		err := req.Unmarshal(readCmds[i].ReadRequest[8:]) // timestamp int64
		if err != nil {
			panic(err)
		}
		if err = util.CheckRegionEpoch(&req, a.applier.region, true); err != nil {
			cb.Done(ErrResp(err))
			continue
		}
		for _, r := range req.Requests {
			if key := util.GetKeyInRequest(r); key != nil {
				if err = util.CheckKeyInRegion(key, a.applier.region); err != nil {
					cb.Done(ErrResp(err))
					continue LOOP
				}
			}
		}
		resp := newCmdResp()
		for _, cmd := range req.Requests {
			switch cmd.CmdType {
			case raft_cmdpb.CmdType_Get:
				val, _ := engine_util.GetCF(a.engine.Kv, cmd.Get.Cf, cmd.Get.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}})
			case raft_cmdpb.CmdType_Snap:
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: a.applier.region}})
				cb.Txn = a.engine.Kv.NewTransaction(false)
			}
		}
		cb.Done(resp)
	}
}

func (a *applyMsgHandler) processAdminRequest(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest) {
	cb := a.applier.findCallBack(entry.Index, entry.Term)
	resp := newCmdResp()
	switch req.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// actually CompactLog do not have a callback, so resp is useless
		a.processCompactLog(req, resp)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		a.processChangePeer(entry, req, resp)
	case raft_cmdpb.AdminCmdType_Split:

	}
	if cb != nil {
		cb.Resp = resp
		a.ctx.appendCallBack(cb)
	}
}

func (a *applyMsgHandler) processCompactLog(req *raft_cmdpb.RaftCmdRequest, resp *raft_cmdpb.RaftCmdResponse) {
	compactIndex, compactTerm := req.AdminRequest.CompactLog.CompactIndex, req.AdminRequest.CompactLog.CompactTerm
	firstIndex := a.ctx.res.ApplyState.TruncatedState.Index + 1
	if compactIndex > firstIndex {
		a.ctx.res.ApplyState.TruncatedState.Index = compactIndex
		a.ctx.res.ApplyState.TruncatedState.Term = compactTerm
		a.ctx.appendResult(ProcessTypeCompactRes, &ProcessCompactRes{firstIndex: firstIndex, truncatedIndex: compactIndex})
	}
}

func (a *applyMsgHandler) processChangePeer(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, resp *raft_cmdpb.RaftCmdResponse) {
	changePeerCmd := req.AdminRequest.ChangePeer
	region := &metapb.Region{}
	err := util.CloneMsg(a.applier.region, region)
	if err != nil {
		panic(err)
	}
	idx := len(region.Peers)
	for i, p := range region.Peers {
		if p.Id == changePeerCmd.Peer.Id && p.StoreId == changePeerCmd.Peer.StoreId {
			idx = i
			break
		}
	}
	switch changePeerCmd.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if idx == len(region.Peers) { // if not found in region peers
			region.Peers = append(region.Peers, changePeerCmd.Peer)
			region.RegionEpoch.ConfVer++
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if idx != len(region.Peers) { // if found in region peers
			region.Peers = append(region.Peers[:idx], region.Peers[idx+1:]...)
			region.RegionEpoch.ConfVer++
			if a.applier.id == changePeerCmd.Peer.Id {
				a.applier.shouldRemove = true
			}
		}
	}
	resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region}}
	cc := &eraftpb.ConfChange{}
	_ = cc.Unmarshal(entry.Data)
	a.ctx.appendResult(ProcessTypeConfChangeRes, &ProcessConfChangeRes{
		region:     region,
		confChange: cc,
		peer:       changePeerCmd.Peer,
	})
	a.applier.setRegion(region)
}

func (a *applyMsgHandler) processSplit(req *raft_cmdpb.RaftCmdRequest, resp *raft_cmdpb.RaftCmdResponse) {
	region := &metapb.Region{}
	err := util.CloneMsg(a.applier.region, region)
	if err != nil {
		panic(err)
	}
	split := req.AdminRequest.Split
	if err := util.CheckKeyInRegion(split.SplitKey, region); err != nil {
		BindRespError(resp, err)
		return
	}
	region.RegionEpoch.Version++
	newPeers := make([]*metapb.Peer, 0, len(split.NewPeerIds))
	// peers in newRegion has same store id to current peers
	for i, p := range region.Peers {
		newPeers = append(newPeers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: p.StoreId})
	}
	newRegion := &metapb.Region{
		Id:          split.NewRegionId,
		StartKey:    util.SafeCopy(split.SplitKey),
		EndKey:      util.SafeCopy(region.EndKey),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: region.RegionEpoch.ConfVer, Version: region.RegionEpoch.Version},
		Peers:       newPeers,
	}
	region.EndKey = util.SafeCopy(split.SplitKey)
	meta.WriteRegionState(&a.ctx.kvWB, newRegion, rspb.PeerState_Normal)
	// origin region will set int HandleApplyMsg func
	// meta.WriteRegionState(&a.ctx.kvWB, region, rspb.PeerState_Normal)
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{region, newRegion},
		},
	}
	a.ctx.appendResult(ProcessTypeSplitRes, &ProcessSplitRegionRes{region: region, newRegion: newRegion})
	a.applier.setRegion(region)
	// SizeDiffHint should be reset since split
	a.ctx.res.SizeDiffHint = 0
}

func (ac *applyContext) appendResult(Type ProcessResType, payload interface{}) {
	ac.res.ProcessRes = append(ac.res.ProcessRes, ProcessResult{
		Type:   Type,
		paylod: payload,
	})
}

func (ac *applyContext) appendCallBack(cb *message.Callback) {
	if cb != nil {
		ac.cbs = append(ac.cbs, cb)
	}
}

func (ac *applyContext) doneCbs() {
	for _, cb := range ac.cbs {
		cb.Done(nil)
	}
}
