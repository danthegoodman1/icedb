package main

import (
	"github.com/danthegoodman1/icedb/datastore"
	"github.com/danthegoodman1/icedb/metastore"
)

type (
	IceDB struct {
		MetaStore metastore.MetaStore
		DataStore datastore.DataStore
	}
)

func NewIceDB(ms metastore.MetaStore, ds datastore.DataStore) (*IceDB, error) {
	icedb := &IceDB{
		MetaStore: ms,
		DataStore: ds,
	}

	return icedb, nil
}
