// linkedlist.go provides a simple way to use the Bolt database
// and store data in a doubly linked list-like data structure manner,
// but keeping bolt's binary tree as its underlying data structure.
package simplebolt

import (
	"errors"
	"bytes"
	"fmt"
	"log"

	"github.com/etcd-io/bbolt"
	"github.com/golang/protobuf/proto"
	pb "github.com/xyproto/simplebolt/ll_nodes_pb"
)

type (
	// LinkedList is a doubly linked list. It is persisted using etcd-io/bbolt's b+tree
	// as its underlying data structure but with a doubly linked list-like behaviour
	LinkedList boltBucket

	// storedData used its fields key, value and internal_ll to perform operations that
	// modify the corresponding values in Bolt. It implements StoredData, hence can be
	// used to build a variable of type Item wherever needed.
	storedData struct {
		// Key of the current item
		key []byte
		// Value of the current item
		value []byte
		// Underlying linked list at which to perform modifications (update and delete) given
		// a key and a value. It is initialised from linked list Front() and Back() methods.
		internal_ll *LinkedList
	}

	// Item is the element of the linked list returned by Front(), Back(), Next(), Prev(),
	// and all the Getters.
	//
	// It enables access to the underlying data in Bolt for getting an updating it.
	//
	// It can be used to traverse the linked list across every node of the data structure,
	// by calling Prev(), Next() and any of the Getter methods. To retrieve, change or
	// delete the underlying data, the Data field has the corresponding methods.
	Item struct {
		Data StoredData
	}
)

// New returns a new doubly linkedlist with the given id as its identifier
func NewLinkedList(db *Database, id string) (*LinkedList, error) {
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
// Returns a nil item if the list is empty.
//
// It may return an error in case of:
//
// bbolt.View() error
//
// proto.Unmarshal() error
func (ll *LinkedList) Front() (i *Item, err error) {
	return i, (*bbolt.DB)(ll.db).View(func(tx *bbolt.Tx) error {
		k, val, empty, err := ll.first()
		if err != nil {
			return err
		}
		if empty {
			i, err = nil, nil
			return err
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
// It returns a nil item if the list is empty.
//
// It may return an error in case of:
//
// bbolt.View() error
//
// proto.Unmarshal() error
func (ll *LinkedList) Back() (i *Item, err error) {
	return i, (*bbolt.DB)(ll.db).View(func(tx *bbolt.Tx) error {
		k, val, empty, err := ll.last()
		if err != nil {
			return err
		}
		if empty {
			i, err = nil, nil
			return err
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

// Get compares val with the value of every single node in the linked list,
// using bytes.Equal(). If it finds that v and the value of some node are
// equal, according to its criteria, then Get returns the item containing
// the value of the stored data.
//
// Note that you must provide a []byte with a value in exactly the same
// format as the stored data in the linked list. For a more flexible criteria on
// the equality of the given value and the value in the stored data, see GetFunc.
// 
// If Get can't find any matches, it returns an nil item and a nil error.
//
// It may return an error due to a failed call to ll.Front().
// It also returns either an "Empty list" error when called on a list with no elements,
// or an "Empty val" error when called with a nil []byte val. In all the cases, the
// returned item is nil.
//
// Note that both Get and GetFunc always return the first match, if any. If you inserted
// multiple copies of the same data into the same linked list and you want to retrieve
// them, you must call GetNext or GetNextFunc sucesively after a call either to Get,
// GetFunc, GetNext or GetNextFunc, passing in the item returned by one of these
// methods.
func(ll *LinkedList) Get(val []byte) (*Item, error) {
	// Check whether the list has no elements
	front, err := ll.Front()
	if err != nil {
		return nil, err
	}
	if front == nil {
		return nil, fmt.Errorf("Empty list")
	}
	// Check whether the user provided a value to get
	if val == nil {
		return nil, fmt.Errorf("Empty val")
	}
	var it *Item
	// Search from the front of the list until either
	// the end of the list or a match has been found.
	for k := front; k != nil; k = k.Next() {
		if bytes.Equal(val, k.Data.Value()) {
			// Found it!
			it = k
			break
		}
	}
	return it, nil
}

// GetFunc compares val with the value of every single node in the linked list,
// using the provided func to compare the given value and the value of the
// stored data. That way, you can define the criteria of equality between the two
// values that suits your data available at some point in time.
// 
// If GetFunc can't find any matches, it returns an nil item and a nil error.
//
// It may return an error due to a failed call to ll.Front().
// It also returns either an "Empty list" error when called on a list with no elements,
// an "Empty val" error when called with a nil interface{} val or an "Empty comparing
// function" when called with a nil function to compare. In all the cases, the returned
// item is nil.
//
// Note that both Get and GetFunc always return the first match, if any. If you inserted
// multiple copies of the same data into the same linked list and you want to retrieve
// them, you must call GetNext or GetNextFunc successively after a call either to Get,
// GetFunc, GetNext or GetNextFunc, passing in the item returned by one of these
// methods.
//
// For an example on the usage, see example/linkedlist/main.go
func(ll *LinkedList) GetFunc(val interface{}, equal func(a interface{}, b []byte) bool) (*Item, error) {
	// Check whether the list has no elements
	front, err := ll.Front()
	if err != nil {
		return nil, err
	}
	if front == nil {
		return nil, fmt.Errorf("Empty list")
	}
	// Check whether the user provided a value to get
	if val == nil {
		return nil, fmt.Errorf("Empty val")
	}
	// Check whether the user provided a function to compare for equality
	if equal == nil {
		return nil, fmt.Errorf("Empty comparing function")
	}
	var it *Item
	// Search from the front of the list until either
	// the end of the list or a match has been found.
	for k := front; k != nil; k = k.Next() {
		if equal(val, k.Data.Value()) {
			// Found it!
			it = k
			break
		}
	}
	return it, nil
}

// GetNext compares val with the value of every single node in the linked list,
// starting from the next item of the element pointed to by mark, using
// bytes.Equal(). If it finds that v and the value of some node are equal,
// according to its criteria, then Get returns the item containing the value of
// the stored data.
// 
// If GetNext can't find any match, it returns an nil item and a nil error.
//
// It may return an error due to a failed call to bbolt.View.
// It returns either an "Empty list" error when called on a list with no elements, 
// an "Empty val" error when called with a nil val to get, or an "Empty mark" error
// when called with a nil mark to begin from. In all the cases the item returned
// is nil.
//
// Note that you must pass in a []byte with a value in exactly the same
// format as the stored data in the linked list. For a more flexible criteria on
// the equality of the given value and the value in the stored data, see GetFunc and 
// GetNextFunc.
func(ll *LinkedList) GetNext(val []byte, mark *Item) (*Item, error) {
	// Check whether the linked list has no elements
	_, _, empty, err := ll.first()
	if err != nil {
		return nil, err
	}
	if empty {
		return nil, fmt.Errorf("Empty list")
	}
	// Check whether the user provided a value to get
	if val == nil {
		return nil, fmt.Errorf("Empty val")
	}
	// Check whether the user provided a mark to begin from
	if mark == nil {
		return nil, fmt.Errorf("Empty mark")
	}
	var it *Item
	// Search from the mark either until the end of the list or a match has been found.
	for k := mark; k != nil; k = k.Next() {
		if bytes.Equal(val, k.Data.Value()) {
			// Found it!
			it = k
			break
		}
	}
	return it, nil
}

// GetNextFunc compares val with the value of every single node in the linked list,
// starting from the next item of the element pointed to by mark, using the provided
// function. If it finds that v and the value of some node are equal, according to
// its criteria, then GetNextFunc returns the item containing the value of the
// stored data.
// 
// If GetNextFunc can't find any matches, it returns an nil item and a nil error.
//
// It returns either an "Empty val" error when called with a nil []byte val, an
// "Empty mark" error when called with a nil beggining mark, or an "Empty comparing 
// function" when called with a nil function to compare.
//
// For an example on the usage, see example/linkedlist/main.go
func(ll *LinkedList) GetNextFunc(val interface{}, mark *Item, equal func(a interface{}, b []byte) bool) (*Item, error) {
	// Check whether the linked list has no elements
	_, _, empty, err := ll.first()
	if err != nil {
		return nil, err
	}
	if empty {
		return nil, fmt.Errorf("Empty list")
	}
	// Check whether the user provided a value to get
	if val == nil {
		return nil, fmt.Errorf("Empty val")
	}
	// Check whether the user provided a mark to begin from
	if mark == nil {
		return nil, fmt.Errorf("Empty mark")
	}
	// Check ehwther the user provided a function to compare for equality
	if equal == nil {
		return nil, fmt.Errorf("Empty comparing function")
	}
	var it *Item
	// Search from the mark either until the end of the list or a match has been found.
	for k := mark; k != nil; k = k.Next() {
		if equal(val, k.Data.Value()) {
			// Found it!
			it = k
			break
		}
	}
	return it, nil
}

// Next returns the next item pointed to by the current linked list item.
//
// It should be called after Front() or any Getter method. Otherwise always returns nil.
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
			return errReachedEnd
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
		if err == errReachedEnd {
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
// It should be called after Back() or any Getter method. Otherwise always returns nil.
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
			return errReachedEnd
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
		if err == errReachedEnd {
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

// Update resets the value of the element at which the item refers
// to with the newData. Returns "Empty data" error if newData is nil.
//
// It may also return an error in case of bbolt Update or protocol buffer
// serialization/deserialization fail. In both cases, the data isn't updated.
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

// Remove deletes from Bolt the element at which the item data refers to.
//
// It may return an error in case of bbolt Update or protocol buffer
// serialization/deserialization fail. In both cases, the data isn't removed.
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

		// Let the Go Garbage Collector do its job.
		sd.key = nil
		sd.value = nil
		sd = nil
		return nil
	})
}
