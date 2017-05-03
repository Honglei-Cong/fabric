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
	"testing"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	TiDBKV "github.com/pingcap/tidb/kv"
)

// TestVDBEnv provides a level db backed versioned db for testing
type TestVDBEnv struct {
	t          testing.TB
	DBProvider statedb.VersionedDBProvider
}

// NewTestVDBEnv instantiates and new level db backed TestVDB
func NewTestVDBEnv(t testing.TB) *TestVDBEnv {
	t.Logf("Creating new TestVDBEnv")
	dbProvider, err := NewVersionedDBProvider()
	if err != nil {
		t.Fatal(err)
	}
	return &TestVDBEnv{t, dbProvider}
}

// Cleanup closes the db and removes the db folder
func (env *TestVDBEnv) Cleanup() {
	env.t.Log("Cleaningup TestVDBEnv")
	switch p := env.DBProvider.(type) {
	case *VersionedDBProvider:
		for dbName := range p.versionedDBs {
			err := p.cleanupTestDB(dbName)
			if err != nil {
				env.t.Fatal(err)
			}
		}
	}
	env.DBProvider.Close()
}

func (p *VersionedDBProvider) cleanupTestDB(dbName string) error {
	db := p.versionedDBs[dbName]
	if db == nil {
		return nil
	}

	txn, err := db.db.Store.Begin()
	if err != nil {
		return err
	}
	defer txn.Commit()

	keys := make([]TiDBKV.Key, 0)
	ite, err := txn.Seek([]byte(dbName))
	if ite != nil && !ite.Valid() {
		return nil		// EOF
	}
	for {
		err = ite.Next()
		if err != nil {
			return err
		}
		if !ite.Valid() {
			break
		}
		keys = append(keys, ite.Key())
	}

	for _, k := range keys {
		err := txn.Delete(k)
		if err != nil {
			return err
		}
	}

	return txn.Delete(constructCompositeKey(dbName, "", string(savePointKey)))
}
