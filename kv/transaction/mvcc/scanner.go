package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
	scanner.iter.Seek(EncodeKey(startKey, txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item := scan.iter.Item()
	commitTs, userKey := decodeKey(item.Key())
	if commitTs >= scan.txn.StartTS {
		scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
		return scan.Next()
	}
	val, err := item.Value()
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(val)
	if err != nil {
		return nil, nil, err
	}
	// this key has been deleted
	if write.Kind != WriteKindPut {
		scan.iter.Seek(EncodeKey(userKey, 0)) // go to next key
		return scan.Next()
	}
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if err != nil {
		return nil, nil, err
	}
	scan.iter.Seek(EncodeKey(userKey, 0)) // go to next key
	return userKey, value, nil
}
