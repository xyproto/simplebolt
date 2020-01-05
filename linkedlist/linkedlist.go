// Package linkedlist provides a simple way to use the Bolt database
// and store data in a doubly linked list-like data structure manner,
// but keeping bolt's binary tree as its underlying data structure.
package linkedlist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	"github.com/etcd-io/bbolt"
	"github.com/golang/protobuf/proto"
	"github.com/xyproto/simplebolt"
	"github.com/xyproto/simplebolt/data"
	pb "github.com/xyproto/simplebolt/nodes_pb"
)

const (
	// Version number. Stable API within major version numbers.
	Version = 3.4
)

type (
	// Used for each of the datatypes
	boltBucket struct {
		db   *simplebolt.Database // the Bolt database
		name []byte               // the bucket name
	}

	// LinkedList is a doubly linked list. It is persisted using etcd-io/bbolt's b+tree
	// as its underlying data structure but with a simply linked list-like behaviour
	LinkedList boltBucket

	// storedData used its fields key, value and internal_ll to perform operations that
	// modify the corresponding values in Bolt. It implements data.StoredData and should
	// be returned from LinkedList.Front() and LinkedList.Back()
	storedData struct {
		// Key of the current item
		key []byte
		// Value of the current item
		value []byte
		// Underlying linked list at which to perform modifications (update and delete) given
		// a key and a value. It is initialised from linked list Front() and Back() methods.
		internal_ll *LinkedList
	}

	// Item is the element of the linked list returned by Front(), Back(), Next() and Prev().
	// It enables access to the underlying data in bbolt for getting an updating it.
	//
	// It can be used to traverse the linked list across every node of the data structure,
	// by calling Prev() and Next(). To retrieve, change or delete the underlying data,
	// the Data field has the corresponding methods.
	Item struct {
		Data data.StoredData
	}
)

var (
	// ErrBucketNotFound may be returned if a no Bolt bucket was found
	ErrBucketNotFound = errors.New("Bucket not found")

	// ErrKeyNotFound will be returned if the key was not found in a HashMap or KeyValue struct
	ErrKeyNotFound = errors.New("Key not found")

	// ErrDoesNotExist will be returned if an element was not found. Used in List, Set, HashMap and KeyValue.
	ErrDoesNotExist = errors.New("Does not exist")

	// ErrExistsInSet is only returned if an element is added to a Set, but it already exists
	ErrExistsInSet = errors.New("Element already exists in set")

	// ErrInvalidID is only returned if adding an element to a HashMap that contains a colon (:)
	ErrInvalidID = errors.New("Element ID can not contain \":\"")

	// ErrFoundIt is only used internally, for breaking out of Bolt DB style for-loops
	ErrFoundIt = errors.New("Found it")

	ErrReachedEnd = errors.New("Reached end of data structure")
)

// New returns a new doubly linkedlist with the given id as its identifier
func New(db *simplebolt.Database, id string) (*LinkedList, error) {
	name := []byte(id)
	if err := (*bbolt.DB)(db).Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return errors.New("Could not create bucket: " + err.Error())
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// Success
	return &LinkedList{db, name}, nil
}

// PushBack inserts data at the end of the doubly linked list.
// Returns an "Empty data" error if data is nil. It also may fail if either
// bbolt operations or protocol buffer serialization/deserialization fail
func (ll *LinkedList) PushBack(data []byte) error {
	// Checks whether there is new data.
	// Nothing gets pushed if data is nil and returns an Empty data error.
	if data == nil {
		// No data to push
		return fmt.Errorf("Empty data")
	}
	return (*bbolt.DB)(ll.db).Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ll.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		var (
			id        uint64
			err       error
			nodeBytes []byte
		)
		// Get the id of the new node
		id, _ = bucket.NextSequence()

		newNode := &pb.LinkedListNode{
			Data: data,
			Next: nil,
			Prev: nil,
		}

		// Get the last key/node bytes pair
		lastKey, nodeBytes := bucket.Cursor().Last()

		// Checks whether there are not other nodes in the list
		if lastKey == nil {
			// This is the first node, no need to link previous nodes to this one.
			// Serialize the first node
			if nodeBytes, err = proto.Marshal(newNode); err != nil {
				return fmt.Errorf("Could not marshal. %v", err)
			}
			// Save the first node
			return bucket.Put(byteID(id), nodeBytes)
		}
		// This is *not* the first node.
		// Update the last node to link to the ID of this new node
		// and this node to link to the ID of the last one.

		// De-serialize the last node to access the next link
		lastNode := &pb.LinkedListNode{}
		if err = proto.Unmarshal(nodeBytes, lastNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		// Set the next link of the last node to the ID of the new node
		lastNode.Next = byteID(id)
		// Serialize back the last node
		if nodeBytes, err = proto.Marshal(lastNode); err != nil {
			return fmt.Errorf("Could not marshal. %v", err)
		}
		// Save changes to the last node.
		if err = bucket.Put(lastKey, nodeBytes); err != nil {
			return fmt.Errorf("Could not save changes to the last node. %v", err)
		}
		// Link the new node to the last node
		newNode.Prev = lastKey
		// Serialize the new node
		if nodeBytes, err = proto.Marshal(newNode); err != nil {
			return fmt.Errorf("Could not marshal. %v", err)
		}
		// Save the new node and return the error
		return bucket.Put(byteID(id), nodeBytes)
	})
}

// PushFront inserts data at the beginning of the doubly linked list.
// Returns an "Empty data" error if data is nil. It also may fail if either
// bbolt operations or protocol buffer serialization/deserialization fail
func (ll *LinkedList) PushFront(data []byte) error {
	// Checks whether there is new data.
	// Nothing gets pushed if data is nil and returns an Empty data error.
	if data == nil {
		// No data to push
		return fmt.Errorf("Empty data")
	}
	return (*bbolt.DB)(ll.db).Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ll.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		var (
			id        uint64
			err       error
			nodeBytes []byte
		)
		// Get the id of the new node
		id, _ = bucket.NextSequence()

		newNode := &pb.LinkedListNode{
			Data: data,
			Next: nil,
			Prev: nil,
		}

		// Get the first key/node bytes pair
		firstKey, nodeBytes := bucket.Cursor().First()
		// Checks whether there are not other nodes in the list
		if firstKey == nil {
			// This is the first node, no need to link this node to other ones.
			// Serialize the first node
			if nodeBytes, err = proto.Marshal(newNode); err != nil {
				return fmt.Errorf("Could not marshal. %v", err)
			}
			// Save the first node
			return bucket.Put(byteID(id), nodeBytes)
		}
		// This is *not* the first node. Update this node to link to the ID
		// of the first node and the first node to link to the ID of this node.

		// De-serialize the first node to access the prev link
		firstNode := &pb.LinkedListNode{}
		if err = proto.Unmarshal(nodeBytes, firstNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		// Set the prev link of the first node to the ID of the new node
		firstNode.Prev = byteID(id)

		// Serialize back the first node
		if nodeBytes, err = proto.Marshal(firstNode); err != nil {
			return fmt.Errorf("Could not marshal. %v", err)
		}
		// Save the changes to the first node
		if err = bucket.Put(firstKey, nodeBytes); err != nil {
			return fmt.Errorf("Could not save changes to the first node. %v", err)
		}
		// Link the new node to the first node
		newNode.Next = firstKey
		// Serialize the new node
		if nodeBytes, err = proto.Marshal(newNode); err != nil {
			return fmt.Errorf("Could not marshal. %v", err)
		}
		// Save the new node
		return bucket.Put(byteID(id), nodeBytes)
	})
}

// Front returns the element at the front of the linked list.
// It may return an error in case of:
// * Empty linked list - a list with no elements
// * bbolt.View() error
// * proto.Unmarshal() error
func (ll *LinkedList) Front() (i *Item, err error) {
	return i, (*bbolt.DB)(ll.db).View(func(tx *bbolt.Tx) error {
		k, val, empty, err := ll.first()
		if err != nil {
			return err
		}
		if empty {
			return fmt.Errorf("Empty linked list")
		}
		llFirstNode := &pb.LinkedListNode{}
		if err := proto.Unmarshal(val, llFirstNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		i = &Item{
			Data: &storedData{
				key:         k,
				value:       llFirstNode.Data,
				internal_ll: ll,
			},
		}
		return nil
	})
}

// Back returns the element at the back of the linked list.
// It may return an error in case of:
// * Empty linked list - a list with no elements
// * bbolt.View() error
// * proto.Unmarshal() error
func (ll *LinkedList) Back() (i *Item, err error) {
	return i, (*bbolt.DB)(ll.db).View(func(tx *bbolt.Tx) error {
		k, val, empty, err := ll.last()
		if err != nil {
			return err
		}
		if empty {
			return fmt.Errorf("Empty linked list")
		}
		llLastNode := &pb.LinkedListNode{}
		if err := proto.Unmarshal(val, llLastNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		i = &Item{
			Data: &storedData{
				key:         k,
				value:       llLastNode.Data,
				internal_ll: ll,
			},
		}
		return nil
	})
}

// first checks whether the linked list has elements and returns the first key/value pair
func (ll *LinkedList) first() (key, val []byte, empty bool, err error) {
	err = (*bbolt.DB)(ll.db).View(func(tx *bbolt.Tx) error {
		var bucket *bbolt.Bucket
		if bucket = tx.Bucket(ll.name); bucket == nil {
			return ErrBucketNotFound
		}
		key, val = bucket.Cursor().First()
		if key == nil {
			empty = true
		} else {
			empty = false
		}
		return nil
	})
	return
}

// last checks whether the linked list has elements and returns the last key/value pair
func (ll *LinkedList) last() (key, val []byte, empty bool, err error) {
	err = (*bbolt.DB)(ll.db).View(func(tx *bbolt.Tx) error {
		var bucket *bbolt.Bucket
		if bucket = tx.Bucket(ll.name); bucket == nil {
			return ErrBucketNotFound
		}
		key, val = bucket.Cursor().Last()
		if key == nil {
			empty = true
		} else {
			empty = false
		}
		return nil
	})
	return
}

/* --- Utility functions --- */

// Create a byte slice from an uint64
func byteID(x uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, x)
	return b
}

// Next returns the next item pointed to by the current linked list item.
//
// It should be called after Front(). Otherwise always returns nil.
func (i *Item) Next() *Item {
	// Type assert the StoredData interface to a *storedData type
	currentKey := i.Data.(*storedData).key
	// Check whether the item refers to an actual item
	// Returns nil if not.
	if currentKey == nil {
		return nil
	}
	listName := i.Data.(*storedData).internal_ll.name
	db := (*bbolt.DB)(i.Data.(*storedData).internal_ll.db)
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(listName)
		if bucket == nil {
			return ErrBucketNotFound
		}
		// Retrieve this node to get prev node's link
		val := bucket.Get(currentKey)
		if val == nil {
			return ErrDoesNotExist
		}
		currentNode := &pb.LinkedListNode{}
		if err := proto.Unmarshal(val, currentNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		// Get next key
		nextKey := currentNode.GetNext()
		// Get next node
		val = bucket.Get(nextKey)
		if val == nil {
			return ErrReachedEnd
		}
		// Get next node
		nextNode := &pb.LinkedListNode{}
		if err := proto.Unmarshal(val, nextNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		// reset current item fields with next node's data
		i.Data.(*storedData).key = nextKey
		i.Data.(*storedData).value = nextNode.GetData()
		// keep the same internal_ll
		return nil
	})
	if err != nil {
		if err == ErrReachedEnd {
			return nil
		} else {
			log.Fatalf("Could not get next: %v\n", err)
			return nil
		}
	}
	return i
}

// Prev returns the previous item pointed to by the current linked list item.
//
// It should be called after Back(). Otherwise always returns nil.
func (i *Item) Prev() *Item {
	currentKey := i.Data.(*storedData).key
	// Check whether the LinkedListItem refers to an actual item.
	// Returns nil if not.
	if currentKey == nil {
		return nil
	}
	// Type assert the StoredData interface to a *storedData type
	listName := i.Data.(*storedData).internal_ll.name
	db := (*bbolt.DB)(i.Data.(*storedData).internal_ll.db)
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(listName)
		if bucket == nil {
			return ErrBucketNotFound
		}
		// Retrieve this node to get prev node's link
		val := bucket.Get(currentKey)
		if val == nil {
			return ErrDoesNotExist
		}
		currentNode := &pb.LinkedListNode{}
		if err := proto.Unmarshal(val, currentNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		// Get prev key
		prevKey := currentNode.GetPrev()
		// Get prev node
		val = bucket.Get(prevKey)
		if val == nil {
			return ErrReachedEnd
		}
		// Get prev node
		prevNode := &pb.LinkedListNode{}
		if err := proto.Unmarshal(val, prevNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		// reset current item fields with prev node's data
		i.Data.(*storedData).key = prevKey
		i.Data.(*storedData).value = prevNode.GetData()
		// keep the same internal_ll
		return nil
	})
	if err != nil {
		if err == ErrReachedEnd {
			return nil
		} else {
			log.Fatalf("Could not get next: %v\n", err)
			return nil
		}
	}
	return i
}

// Value returns the current value of the element at which the item refers to.
func (sd storedData) Value() []byte {
	return sd.value
}

// Update resets the value of the element at which the item refers to with the newData.
// Returns "Empty data" error if newData is nil
func (sd *storedData) Update(newData []byte) error {
	// Checks whether there is new data.
	// Nothing gets updated if newData is nil and returns Empty data.
	if newData == nil {
		return fmt.Errorf("Empty data")
	}

	listName := sd.internal_ll.name
	db := (*bbolt.DB)(sd.internal_ll.db)

	return db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(listName)
		if bucket == nil {
			return ErrBucketNotFound
		}
		// Get serialized current node
		currentNodeBytes := bucket.Get(sd.key)
		if currentNodeBytes == nil {
			return ErrDoesNotExist
		}
		var err error
		// De-serialize current node to access its data
		currentNode := &pb.LinkedListNode{}
		if err = proto.Unmarshal(currentNodeBytes, currentNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}
		// Reset data of current node
		currentNode.Data = newData
		// Serialize back the current node
		if currentNodeBytes, err = proto.Marshal(currentNode); err != nil {
			return fmt.Errorf("Could not marshal. %v", err)
		}
		// Save changes to current node
		if err = bucket.Put(sd.key, currentNodeBytes); err != nil {
			return fmt.Errorf("Could not update. %v", err)
		}
		return nil
	})
}

// Remove deletes from Bolt the element at which the item data refers to
func (sd *storedData) Remove() error {
	listName := sd.internal_ll.name
	db := (*bbolt.DB)(sd.internal_ll.db)

	return db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(listName)
		if bucket == nil {
			return ErrBucketNotFound
		}

		// Get serialized current node
		currentNodeBytes := bucket.Get(sd.key)
		if currentNodeBytes == nil {
			return ErrDoesNotExist
		}
		var err error
		// De-serialize the current node to access next/prev links
		currentNode := &pb.LinkedListNode{}
		if err = proto.Unmarshal(currentNodeBytes, currentNode); err != nil {
			return fmt.Errorf("Could not unmarshal. %v", err)
		}

		// Get link of prev/next nodes
		prevKey := currentNode.GetPrev()
		nextKey := currentNode.GetNext()

		// Checks whether the current node is linked to a previous node.
		if prevKey != nil {
			// Get serialized previous node
			prevNodeBytes := bucket.Get(prevKey)
			if prevNodeBytes == nil {
				return ErrDoesNotExist
			}
			// De-serialize the previous node to reset its next link
			prevNode := &pb.LinkedListNode{}
			err = proto.Unmarshal(prevNodeBytes, prevNode)
			if err != nil {
				return fmt.Errorf("Could not unmarshal. %v", err)
			}
			// Reset next link of previous node
			prevNode.Next = nextKey
			// Serialize back the next node
			prevNodeBytes, err = proto.Marshal(prevNode)
			if err != nil {
				return fmt.Errorf("Could not marshal. %v", err)
			}
			// Save changes to prev nodes
			err = bucket.Put(prevKey, prevNodeBytes)
			if err != nil {
				return fmt.Errorf("Could not update previous node's link. %v", err)
			}
		}

		// Checks whether the current node is linked to a next node.
		if nextKey != nil {
			// Get serialized next node
			nextNodeBytes := bucket.Get(nextKey)
			if nextNodeBytes == nil {
				return ErrDoesNotExist
			}
			// De-serialize the next node to reset its prev link
			nextNode := &pb.LinkedListNode{}
			err = proto.Unmarshal(nextNodeBytes, nextNode)
			if err != nil {
				return fmt.Errorf("Could not unmarshal. %v", err)
			}
			// Reset prev link of next node
			nextNode.Prev = prevKey
			// Serialize back the next node
			nextNodeBytes, err = proto.Marshal(nextNode)
			if err != nil {
				return fmt.Errorf("Could not marshal. %v", err)
			}
			// Save changes to next node
			err = bucket.Put(nextKey, nextNodeBytes)
			if err != nil {
				return fmt.Errorf("Could not update next node's link. %v", err)
			}
		}

		// Remove this node from Bolt
		if err = bucket.Delete(sd.key); err != nil {
			return fmt.Errorf("Could not delete key. %v", err)
		}

		// clear in-memory linked list stored data
		sd.key = nil
		sd.value = nil
		sd.internal_ll = nil
		sd = nil
		return nil
	})
}
