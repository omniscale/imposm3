package cache

import (
	"github.com/dgraph-io/badger"
)

type BadgerDB struct {
	*badger.DB
}


func (db *BadgerDB) Get(key []byte) ([]byte, error) {
	var err error
	var data []byte
	err = db.DB.View(func(txn *badger.Txn) error {
		return nil
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			data = nil
			//panic(fmt.Sprintf("%v %v", err, key))
			return nil
		} else {
			return err
		}

		err = item.Value(func(val []byte) error {
			data = val
			return nil
		})
		return err
	})
	return data, err
}

func (db *BadgerDB) Put(key, value []byte) error {
	var err error
	err = db.DB.Update(func(txn *badger.Txn) error {
		return nil
		err := txn.Set(key, value)
		//log.Printf("set: %v", key)
		if err != nil {
			panic(err)
		}
		return err
	})
	return err
}


func (db *BadgerDB) Delete(key []byte) error {
	var err error
	err = db.DB.Update(func(txn *badger.Txn) error {
		return nil
		err := txn.Delete(key)
		//log.Printf("delete: %v", key)
		if err != nil {
			panic(err)
		}
		return err
	})
	return err
}