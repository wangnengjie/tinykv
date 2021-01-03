package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// check whether error is raft_storage.RegionError
func regionError(err error, rgError **errorpb.Error, errstr *string) {
	if e, ok := err.(*raft_storage.RegionError); ok {
		*rgError = e.RequestErr
	} else if errstr != nil {
		*errstr = err.Error()
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		regionError(err, &resp.RegionError, &resp.Error)
		return resp, nil
	}
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		regionError(err, &resp.RegionError, &resp.Error)
		return resp, nil
	}
	resp.NotFound = val == nil
	resp.Value = val
	return resp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{
		{Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		}},
	})
	if err != nil {
		regionError(err, &resp.RegionError, &resp.Error)
		return resp, nil
	}
	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{
		{Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		}},
	})
	if err != nil {
		regionError(err, &resp.RegionError, &resp.Error)
		return resp, nil
	}
	return resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawScanResponse{}
	if req.Limit == 0 {
		return resp, nil
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		regionError(err, &resp.RegionError, &resp.Error)
		return resp, nil
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	resp.Kvs = make([]*kvrpcpb.KvPair, 0, req.Limit)
	for i := uint32(0); i < req.Limit && iter.Valid(); i++ {
		key := iter.Item().KeyCopy(nil)
		// what's the error?
		val, err := iter.Item().ValueCopy(nil)
		if err != nil {
			regionError(err, &resp.RegionError, &resp.Error)
			break
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		iter.Next()
	}
	return resp, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	errHandler := func(err error) (*kvrpcpb.GetResponse, error) {
		regionError(err, &resp.RegionError, nil)
		if resp.RegionError != nil {
			return resp, nil
		}
		return nil, err
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return errHandler(err)
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return errHandler(err)
	}
	if lock != nil && lock.IsLockedFor(req.Key, txn.StartTS, resp) {
		return resp, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return errHandler(err)
	}
	resp.Value = val
	resp.NotFound = val == nil
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	errHandler := func(err error) (*kvrpcpb.PrewriteResponse, error) {
		regionError(err, &resp.RegionError, nil)
		if resp.RegionError != nil {
			return resp, nil
		}
		return nil, err
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return errHandler(err)
	}
	defer reader.Close()

	var keys [][]byte
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mutation := range req.Mutations {
		w, cts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return errHandler(err)
		}
		if w != nil && txn.StartTS <= cts {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    txn.StartTS,
				ConflictTs: cts,
				Key:        mutation.Key,
				Primary:    req.PrimaryLock,
			}})
			continue
		}
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return errHandler(err)
		}
		if lock != nil && (lock.Ts != txn.StartTS || !bytes.Equal(lock.Primary, req.PrimaryLock)) {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)})
			continue
		}
		kind := mvcc.WriteKindFromProto(mutation.Op)
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
		switch kind {
		case mvcc.WriteKindPut:
			txn.PutValue(mutation.Key, mutation.Value)
		case mvcc.WriteKindDelete:
			txn.DeleteValue(mutation.Key)
		}
	}
	server.Latches.Validate(txn, keys)
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return errHandler(err)
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	errHandler := func(err error) (*kvrpcpb.CommitResponse, error) {
		regionError(err, &resp.RegionError, nil)
		if resp.RegionError != nil {
			return resp, nil
		}
		return nil, err
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return errHandler(err)
	}
	defer reader.Close()

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return errHandler(err)
		}
		if lock == nil {
			return resp, nil
		}
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{Retryable: fmt.Sprintf("key:%+v locked by other transaction", key)}
			return resp, nil
		}
		write := &mvcc.Write{Kind: lock.Kind, StartTS: txn.StartTS}
		txn.PutWrite(key, req.CommitVersion, write)
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return errHandler(err)
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	errHandler := func(err error) (*kvrpcpb.ScanResponse, error) {
		regionError(err, &resp.RegionError, nil)
		if resp.RegionError != nil {
			return resp, nil
		}
		return nil, err
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return errHandler(err)
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()
		if err != nil {
			return errHandler(err)
		}
		if key == nil && value == nil {
			break
		}
		// keyError does not have a Error() func, so check lock outside scanner
		lock, err := txn.GetLock(key)
		if err != nil {
			return errHandler(err)
		}
		if lock != nil && lock.Ts <= txn.StartTS {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{Locked: lock.Info(key)},
				Key:   key,
				Value: value,
			})
			continue
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	errHandler := func(err error) (*kvrpcpb.CheckTxnStatusResponse, error) {
		regionError(err, &resp.RegionError, nil)
		if resp.RegionError != nil {
			return resp, nil
		}
		return nil, err
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return errHandler(err)
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return errHandler(err)
	}
	if lock != nil && lock.Ts == txn.StartTS {
		// there is a lock on key
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(req.PrimaryKey)
			}
			txn.DeleteLock(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			resp.Action = kvrpcpb.Action_NoAction
			resp.LockTtl = lock.Ttl
		}
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return errHandler(err)
		}
		return resp, nil
	}
	write, commitTS, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return errHandler(err)
	}
	if write == nil {
		txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{Kind: mvcc.WriteKindRollback, StartTS: txn.StartTS})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return errHandler(err)
		}
		return resp, nil
	}
	resp.Action = kvrpcpb.Action_NoAction
	if write.Kind == mvcc.WriteKindRollback {
		return resp, nil
	}
	resp.CommitVersion = commitTS
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	errHandler := func(err error) (*kvrpcpb.BatchRollbackResponse, error) {
		regionError(err, &resp.RegionError, nil)
		if resp.RegionError != nil {
			return resp, nil
		}
		return nil, err
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return errHandler(err)
	}
	defer reader.Close()

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		toWrite := &mvcc.Write{Kind: mvcc.WriteKindRollback, StartTS: txn.StartTS}
		lock, err := txn.GetLock(key)
		if err != nil {
			return errHandler(err)
		}
		if lock == nil || lock.Ts != txn.StartTS {
			// no lock for key, maybe rollback or commit
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return errHandler(err)
			}
			if write == nil {
				txn.PutWrite(key, txn.StartTS, toWrite)
				continue
			}
			if write.Kind == mvcc.WriteKindRollback {
				continue
			}
			// key has commit, return error
			resp.Error = &kvrpcpb.KeyError{Abort: fmt.Sprintf("key:%+v has been committed", key)}
			return resp, nil
		}
		if lock.Kind == mvcc.WriteKindPut {
			txn.DeleteValue(key)
		}
		txn.DeleteLock(key)
		txn.PutWrite(key, txn.StartTS, toWrite)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return errHandler(err)
	}
	return resp, nil
}

func (server *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	errHandler := func(err error) (*kvrpcpb.ResolveLockResponse, error) {
		regionError(err, &resp.RegionError, nil)
		if resp.RegionError != nil {
			return resp, nil
		}
		return nil, err
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return errHandler(err)
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	klpairs, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return errHandler(err)
	}
	var keys [][]byte
	for _, klp := range klpairs {
		keys = append(keys, klp.Key)
	}
	if req.CommitVersion == 0 {
		_resp, err := server.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{Context: req.Context, StartVersion: req.StartVersion, Keys: keys})
		if err != nil {
			return nil, err
		}
		resp.Error = _resp.Error
		resp.RegionError = _resp.RegionError
	} else {
		_resp, err := server.KvCommit(ctx, &kvrpcpb.CommitRequest{Context: req.Context, StartVersion: req.StartVersion, Keys: keys, CommitVersion: req.CommitVersion})
		if err != nil {
			return nil, err
		}
		resp.Error = _resp.Error
		resp.RegionError = _resp.RegionError
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
