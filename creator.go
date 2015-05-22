package simplebolt

import (
	"github.com/xyproto/pinterface"
)

// For implementing pinterface.ICreator

type BoltCreator struct {
	db *Database
}

func NewCreator(db *Database) *BoltCreator {
	return &BoltCreator{db}
}

func (b *BoltCreator) NewList(id string) pinterface.IList {
	return NewList(b.db, id)
}

func (b *BoltCreator) NewSet(id string) pinterface.ISet {
	return NewSet(b.db, id)
}

func (b *BoltCreator) NewHashMap(id string) pinterface.IHashMap {
	return NewHashMap(b.db, id)
}

func (b *BoltCreator) NewKeyValue(id string) pinterface.IKeyValue {
	return NewKeyValue(b.db, id)
}
