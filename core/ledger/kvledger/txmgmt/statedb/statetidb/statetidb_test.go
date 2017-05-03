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
	"os"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/commontests"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/spf13/viper"
)

var TiPDUrl = "pd1:2379"

func TestMain(m *testing.M) {
	viper.Set("ledger.state.tiDBConfig.pdAddress", TiPDUrl)
	os.Exit(m.Run())
}

func TestBasicRW(t *testing.T) {
	env := NewTestVDBEnv(t)
	defer env.Cleanup()
	commontests.TestBasicRW(t, env.DBProvider)
}

func TestMultiDBBasicRW(t *testing.T) {
	env := NewTestVDBEnv(t)
	defer env.Cleanup()
	commontests.TestMultiDBBasicRW(t, env.DBProvider)
}

func TestDeletes(t *testing.T) {
	env := NewTestVDBEnv(t)
	defer env.Cleanup()
	commontests.TestDeletes(t, env.DBProvider)
}

func TestIterator(t *testing.T) {
	env := NewTestVDBEnv(t)
	defer env.Cleanup()
	commontests.TestIterator(t, env.DBProvider)
}

func TestEncodeDecodeValueAndVersion(t *testing.T) {
	testValueAndVersionEncodeing(t, []byte("value1"), version.NewHeight(1, 2))
	testValueAndVersionEncodeing(t, []byte{}, version.NewHeight(50, 50))
}

func testValueAndVersionEncodeing(t *testing.T, value []byte, version *version.Height) {
	encodedValue := statedb.EncodeValue(value, version)
	val, ver := statedb.DecodeValue(encodedValue)
	testutil.AssertEquals(t, val, value)
	testutil.AssertEquals(t, ver, version)
}

func TestCompositeKey(t *testing.T) {
	testCompositeKey(t, "ledger1", "ns", "key")
	testCompositeKey(t, "ledger2", "ns", "")
}

func testCompositeKey(t *testing.T, dbName string, ns string, key string) {
	compositeKey := constructCompositeKey(dbName, ns, key)
	t.Logf("compositeKey=%#v", compositeKey)
	db1, ns1, key1 := splitCompositeKey(compositeKey)
	testutil.AssertEquals(t, db1, dbName)
	testutil.AssertEquals(t, ns1, ns)
	testutil.AssertEquals(t, key1, key)
}
