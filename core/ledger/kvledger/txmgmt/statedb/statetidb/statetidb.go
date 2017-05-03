/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statetidb

import (
	"bytes"
	"errors"

	"fmt"
	"sync"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	logging "github.com/op/go-logging"
	TiDBKV "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/terror"
)

var logger = logging.MustGetLogger("statetidb")

var compositeKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)
var savePointKey = []byte{0x00}

type TiDB struct {
	Driver    *tikv.Driver
	Store     TiDBKV.Storage
}

// VersionedDB implements VersionedDB interface
type versionedDB struct {
	dbName string
	db     *TiDB
}

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
	db           *TiDB
	versionedDBs map[string]*versionedDB
	mux          sync.Mutex
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider() (*VersionedDBProvider, error) {
	tiDBDef := ledgerconfig.GetTiDBDefinition()
	driver := &tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s", tiDBDef.PDAddress))
	if err != nil {
		return nil, err
	}

	return &VersionedDBProvider{
		db:           &TiDB{driver, store},
		versionedDBs: make(map[string]*versionedDB),
		mux:          sync.Mutex{},
	}, nil
}

// GetDBHandle gets the handle to a named database
func (p *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	db := ledgerconfig.GetLedgerOwner() + dbName
	versionDB := p.versionedDBs[db]
	if versionDB == nil {
		versionDB = &versionedDB{db, p.db}
		p.versionedDBs[db] = versionDB
	}
	return versionDB, nil
}

// Close closes the underlying db
func (p *VersionedDBProvider) Close() {
	// do nothing
}

// Open implements method in VersionedDB interface
func (vdb *versionedDB) Open() error {
	// do nothing because shared db is used
	return nil
}

// Close implements method in VersionedDB interface
func (vdb *versionedDB) Close() {
	// do nothing because shared db is used
}

func (vdb *versionedDB) GetSavepoint() []byte {
	return constructCompositeKey(vdb.dbName, "", string(savePointKey))
}

// GetState implements method in VersionedDB interface
func (vdb *versionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	logger.Debugf("GetState(). ns=%s, key=%s", namespace, key)
	compositeKey := constructCompositeKey(vdb.dbName, namespace, key)

	txn, err := vdb.db.Store.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Commit()
	dbVal, err := txn.Get(compositeKey)
	if err != nil {
		if terror.ErrorEqual(err, TiDBKV.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if dbVal == nil {
		return nil, nil
	}
	val, ver := statedb.DecodeValue(dbVal)
	return &statedb.VersionedValue{Value: val, Version: ver}, nil
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *versionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	snapVer, err := vdb.db.Store.CurrentVersion()
	if err != nil {
		return nil, err
	}
	snap, err := vdb.db.Store.GetSnapshot(snapVer)
	if err != nil {
		return nil, err
	}
	tiKeys := make([]TiDBKV.Key, 0)
	for _, key := range keys {
		tiKeys = append(tiKeys, constructCompositeKey(vdb.dbName, namespace, key))
	}
	tiVals, err := snap.BatchGet(tiKeys)
	if err != nil {
		return nil, err
	}
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, k := range tiKeys {
		val, ver := statedb.DecodeValue(tiVals[string(k)])
		vals[i] = &statedb.VersionedValue{Value: val, Version: ver}
	}
	return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (vdb *versionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	compositeStartKey := constructCompositeKey(vdb.dbName, namespace, startKey)
	compositeEndKey := constructCompositeKey(vdb.dbName, namespace, endKey)
	if endKey == "" {
		compositeEndKey[len(compositeEndKey)-1] = lastKeyIndicator
	}
	snapVer, err := vdb.db.Store.CurrentVersion()
	if err != nil {
		return nil, err
	}
	snap, err := vdb.db.Store.GetSnapshot(snapVer)
	if err != nil {
		return nil, err
	}
	dbItr, err := snap.Seek(compositeStartKey)
	if err != nil {
		return nil, err
	}
	return newKVScanner(vdb.dbName, namespace, compositeEndKey, snap, dbItr), nil
}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *versionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return nil, errors.New("ExecuteQuery not supported for tiKV")
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *versionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	namespaces := batch.GetUpdatedNamespaces()
	txn, err := vdb.db.Store.Begin()
	if err != nil {
		return err
	}

	for _, ns := range namespaces {
		updates := batch.GetUpdates(ns)
		for k, vv := range updates {
			compositeKey := constructCompositeKey(vdb.dbName, ns, k)

			if vv.Value == nil {
				txn.Delete(compositeKey)
			} else {
				txn.Set(compositeKey, statedb.EncodeValue(vv.Value, vv.Version))
			}
		}
	}
	txn.Set(vdb.GetSavepoint(), height.ToBytes())
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *versionedDB) GetLatestSavePoint() (*version.Height, error) {
	txn, err := vdb.db.Store.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Commit()
	versionBytes, err := txn.Get(vdb.GetSavepoint())
	if err != nil {
		if terror.ErrorEqual(err, TiDBKV.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if versionBytes == nil {
		return nil, nil
	}
	version, _ := version.NewHeightFromBytes(versionBytes)
	return version, nil
}

func constructCompositeKey(db string, ns string, key string) []byte {
	bytes := append(append([]byte(db), compositeKeySep...), []byte(ns)...)
	return append(append(bytes, compositeKeySep...), []byte(key)...)
}

func splitCompositeKey(compositeKey []byte) (string, string, string) {
	split := bytes.SplitN(compositeKey, compositeKeySep, 3)
	return string(split[0]), string(split[1]), string(split[2])
}

type tiKVScanner struct {
	dbName    string
	namespace string
	endKey    []byte
	snap      TiDBKV.Snapshot
	dbItr     TiDBKV.Iterator
	lastErr   error
}

func newKVScanner(dbName string, namespace string, endKey []byte, snap TiDBKV.Snapshot, dbItr TiDBKV.Iterator) *tiKVScanner {
	return &tiKVScanner{
		dbName:    dbName,
		namespace: namespace,
		endKey:    endKey,
		snap:      snap,
		dbItr:     dbItr,
		lastErr:   nil,
	}
}

func (scanner *tiKVScanner) Next() (statedb.QueryResult, error) {
	if !scanner.dbItr.Valid() {
		return nil, scanner.lastErr 	// EOF
	}
	dbKey := scanner.dbItr.Key()
	db, ns, key := splitCompositeKey(dbKey)
	if ns != scanner.namespace || db != scanner.dbName {
		return nil, nil			// EOF
	}
	if bytes.Compare(dbKey, scanner.endKey) >= 0 {
		return nil, nil			// EOF
	}

	dbVal := scanner.dbItr.Value()
	dbValCopy := make([]byte, len(dbVal))
	copy(dbValCopy, dbVal)
	value, ver := statedb.DecodeValue(dbValCopy)

	scanner.lastErr = scanner.dbItr.Next()

	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
		VersionedValue: statedb.VersionedValue{Value: value, Version: ver}}, nil
}

func (scanner *tiKVScanner) Close() {
	scanner.dbItr.Close()
}
