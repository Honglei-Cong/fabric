package rbft

import (
	"fmt"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/opt"
	"sync"
)

const (
	closed dbState = iota
	opened
)

type dbState int32

type DB struct {
	path    string
	db      *leveldb.DB
	dbState dbState
	mutex   sync.Mutex

	readOpts        *opt.ReadOptions
	writeOptsNosync *opt.WriteOptions
	writeOptsSync   *opt.WriteOptions
}

func createDB(path string) *DB {
	readOpts := &opt.ReadOptions{}
	writeOptsNoSync := &opt.WriteOptions{}
	writeOptsSync := &opt.WriteOptions{}
	writeOptsSync.Sync = true

	return &DB{
		path:            path,
		dbState:         closed,
		readOpts:        readOpts,
		writeOptsNosync: writeOptsNoSync,
		writeOptsSync:   writeOptsSync,
	}
}

func (db *DB) Open() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.dbState == opened {
		return nil
	}

	dirEmpty, err := util.CreateDirIfMissing(db.path)
	if err != nil {
		return fmt.Errorf("failed to create db dir: %s", err)
	}
	dbOpts := &opt.Options{
		ErrorIfMissing: !dirEmpty,
	}
	db.db, err = leveldb.OpenFile(db.path, dbOpts)
	if err != nil {
		return fmt.Errorf("Failed to open db: %s", err)
	}

	db.dbState = opened
	return nil
}

func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.dbState == closed {
		return nil
	}

	if err := db.db.Close(); err != nil {
		return fmt.Errorf("Error when closing db: %s", err)
	}

	db.dbState = closed
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	value, err := db.db.Get(key, db.readOpts)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	return value, err
}

func (db *DB) Put(key []byte, value []byte, sync bool) (err error) {
	if sync {
		err = db.db.Put(key, value, db.writeOptsSync)
	} else {
		err = db.db.Put(key, value, db.writeOptsNosync)
	}
	return err
}

func (db *DB) Delelte(key []byte, sync bool) (err error) {
	if sync {
		err = db.db.Delete(key, db.writeOptsSync)
	} else {
		err = db.db.Delete(key, db.writeOptsNosync)
	}
	return err
}
