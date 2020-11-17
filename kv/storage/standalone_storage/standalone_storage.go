package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	config *config.Config
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// maybe not need raft? so NewEngine is not suitable
	return &StandAloneStorage{db: nil, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	storePath := filepath.Join(s.config.DBPath, "StandAlone")
	s.db = engine_util.CreateDB(storePath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, errors.New("should start the StandAloneStorage first")
	}
	// read only txn
	return &StandAloneStorageReader{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.db == nil {
		return errors.New("should start the StandAloneStorage first")
	}
	// engine_util.PutCF & engine_util.DeleteCF will create new transaction everytime
	// so maybe use one txn can optimize performance
	// db.Update provide txn and do commit & discard for us
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		for _, modify := range batch {
			switch modify.Data.(type) {
			case storage.Put:
				err = txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
			case storage.Delete:
				err = txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			default:
				err = errors.New("unknown modify type")
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	// iter should be close by the caller
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	// Todo: close all iterators before discarding
	// reader should not be close before iters are closed
	// Discard() will call panic if some iters are not closed
	// maybe store the iter? Or it should be guaranteed by caller
	r.txn.Discard()
}
