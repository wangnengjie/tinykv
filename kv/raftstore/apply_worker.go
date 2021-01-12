package raftstore

import (
	"fmt"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type applyWorker struct {
	router *router
	engine *engine_util.Engines
	// applyCh is create by router
	applyCh chan *message.Msg
}

func newApplyWorker(ctx *GlobalContext) *applyWorker {
	return &applyWorker{
		router:  ctx.router,
		engine:  ctx.engine,
		applyCh: ctx.router.applySender,
	}
}

func (aw *applyWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msg *message.Msg
	for {
		select {
		case <-closeCh:
			return
		case msg = <-aw.applyCh:
			if msg.Type != message.MsgTypeApply {
				panic(fmt.Sprintf("msg type should be %d but get %d", message.MsgTypeApply, msg.Type))
			}
			ps := aw.router.get(msg.RegionID)
			newApplyMsgHandler(ps.applier, aw.router, aw.engine).HandleApplyMsg(msg.Data.(*message.MsgApply))
		}
	}
}
