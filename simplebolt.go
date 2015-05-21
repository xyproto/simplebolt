// Simplebolt provides a way to use Bolt that is similar to simpleredis
package simplebolt

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/boltdb/bolt"
)

// Common for each of the Bolt buckets used here
type boltBucket struct {
	db   *Database
	name []byte
}

type (
	Database bolt.DB

	List     boltBucket
	Set      boltBucket
	HashMap  boltBucket
	KeyValue boltBucket
)

const (
	// Version number. Stable API within major version numbers.
	Version = 1.0
)

var (
	ErrBucketNotFound = errors.New("Bucket not found!")
	ErrKeyNotFound    = errors.New("Key not found!")
	ErrDoesNotExist   = errors.New("Does not exist!")
)

/* --- Database functions --- */

// Create a new bolt database
func New(filename string) *Database {
	db, err := bolt.Open(filename, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	return (*Database)(db)
}

// Close the database
func (db *Database) Close() {
	(*bolt.DB)(db).Close()
}

// Split a string into two parts, given a delimiter.
// Returns the two parts and true if it works out.
func twoFields(s, delim string) (string, string, bool) {
	if strings.Count(s, delim) != 1 {
		return s, "", false
	}
	fields := strings.Split(s, delim)
	return fields[0], fields[1], true
}

/* --- List functions --- */

// Create a new list
func NewList(db *Database, id string) *List {
	name := []byte(id)
	(*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return fmt.Errorf("Could not create bucket: %s", err)
		}
		return nil
	})
	return &List{db, name}

}

// Add an element to the list
func (l *List) Add(value string) error {
	if l.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		n, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		key := strconv.Itoa(n)
		return bucket.Put([]byte(key), []byte(value))
	})
}

// Get all elements of a list
func (l *List) GetAll() (results []string, err error) {
	if l.name == nil {
		return nil, ErrDoesNotExist
	}
	return results, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(key, value []byte) error {
			results = append(results, string(value))
			return nil
		})
	})
}

// Get the last element of a list
func (l *List) GetLast() (result string, err error) {
	if l.name == nil {
		return "", ErrDoesNotExist
	}
	return result, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		cursor := bucket.Cursor()
		// Ignoring the key
		_, value := cursor.Last()
		result = string(value)
		return nil
	})
}

// Get the last N elements of a list
func (l *List) GetLastN(n int) (results []string, err error) {
	if l.name == nil {
		return nil, ErrDoesNotExist
	}
	return results, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		var size int64 = 0
		bucket.ForEach(func(key, value []byte) error {
			size++
			return nil
		})
		if size < int64(n) {
			return errors.New("Too few items in list")
		}
		// Ok, fetch the n last items. startPos is counting from 0.
		var startPos int64 = size - int64(n)
		var i int64 = 0
		bucket.ForEach(func(key, value []byte) error {
			if i >= startPos {
				results = append(results, string(value))
			}
			i++
			return nil
		})
		return nil
	})
}

// Remove this list
func (l *List) Remove() error {
	err := (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(l.name))
	})
	l.name = nil
	return err
}

/* --- Set functions --- */

// Create a new key/value if it does not already exist
func NewSet(db *Database, id string) *Set {
	name := []byte(id)
	(*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return fmt.Errorf("Could not create bucket: %s", err)
		}
		return nil
	})
	return &Set{db, name}
}

// Add an element to the set
func (s *Set) Add(value string) error {
	if s.name == nil {
		return ErrDoesNotExist
	}
	exists, err := s.Has(value)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("Element already exists in set")
	}
	return (*bolt.DB)(s.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		n, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		key := strconv.Itoa(n)
		return bucket.Put([]byte(key), []byte(value))
	})
}

// Check if a given value is in the set
func (s *Set) Has(value string) (exists bool, err error) {
	if s.name == nil {
		return false, ErrDoesNotExist
	}
	return exists, (*bolt.DB)(s.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		bucket.ForEach(func(byteKey, byteValue []byte) error {
			if value == string(byteValue) {
				exists = true
				return errors.New("Found value") // break
			}
			return nil
		})
		return nil
	})
}

// Get all elements of the set
func (s *Set) GetAll() (results []string, err error) {
	if s.name == nil {
		return nil, ErrDoesNotExist
	}
	return results, (*bolt.DB)(s.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(key, value []byte) error {
			results = append(results, string(value))
			return nil
		})
	})
}

// Remove an element from the set
func (s *Set) Del(value string) error {
	if s.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(s.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		var foundKey []byte
		return bucket.ForEach(func(byteKey, byteValue []byte) error {
			if value == string(byteValue) {
				foundKey = byteKey
				return errors.New("Found value") // break
			}
			return nil
		})
		return bucket.Delete([]byte(foundKey))
	})
}

// Remove this set
func (s *Set) Remove() error {
	err := (*bolt.DB)(s.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(s.name))
	})
	s.name = nil
	return err
}

///* --- HashMap functions --- */
//
//// Create a new hashmap
//func NewHashMap(db *Database, id string) *HashMap {
//	return &HashMap{db, id, 0}
//}
//
//// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
//func (rh *HashMap) Set(elementid, key, value string) error {
//	conn := rh.db.Get(rh.dbindex)
//	_, err := conn.Do("HSET", rh.id+":"+elementid, key, value)
//	return err
//}
//
//// Get a value from a hashmap given the element id (for instance a user id) and the key (for instance "password")
//func (rh *HashMap) Get(elementid, key string) (string, error) {
//	conn := rh.db.Get(rh.dbindex)
//	result, err := bolt.String(conn.Do("HGET", rh.id+":"+elementid, key))
//	if err != nil {
//		return "", err
//	}
//	return result, nil
//}
//
//// Check if a given elementid + key is in the hash map
//func (rh *HashMap) Has(elementid, key string) (bool, error) {
//	conn := rh.db.Get(rh.dbindex)
//	retval, err := conn.Do("HEXISTS", rh.id+":"+elementid, key)
//	if err != nil {
//		panic(err)
//	}
//	return bolt.Bool(retval, err)
//}
//
//// Check if a given elementid exists as a hash map at all
//func (rh *HashMap) Exists(elementid string) (bool, error) {
//	// TODO: key is not meant to be a wildcard, check for "*"
//	return hasKey(rh.db, rh.id+":"+elementid, rh.dbindex)
//}
//
//// Get all elementid's for all hash elements
//func (rh *HashMap) GetAll() ([]string, error) {
//	conn := rh.db.Get(rh.dbindex)
//	result, err := bolt.Values(conn.Do("KEYS", rh.id+":*"))
//	strs := make([]string, len(result))
//	idlen := len(rh.id)
//	for i := 0; i < len(result); i++ {
//		strs[i] = getString(result, i)[idlen+1:]
//	}
//	return strs, err
//}
//
//// Remove a key for an entry in a hashmap (for instance the email field for a user)
//func (rh *HashMap) DelKey(elementid, key string) error {
//	conn := rh.db.Get(rh.dbindex)
//	_, err := conn.Do("HDEL", rh.id+":"+elementid, key)
//	return err
//}
//
//// Remove an element (for instance a user)
//func (rh *HashMap) Del(elementid string) error {
//	conn := rh.db.Get(rh.dbindex)
//	_, err := conn.Do("DEL", rh.id+":"+elementid)
//	return err
//}
//
//// Remove this hashmap (all keys that starts with this hashmap id and a colon)
//func (rh *HashMap) Remove() error {
//	conn := rh.db.Get(rh.dbindex)
//	// Find all hashmap keys that starts with rh.id+":"
//	results, err := bolt.Values(conn.Do("KEYS", rh.id+":*"))
//	if err != nil {
//		return err
//	}
//	// For each key id
//	for i := 0; i < len(results); i++ {
//		// Delete this key
//		if _, err = conn.Do("DEL", getString(results, i)); err != nil {
//			return err
//		}
//	}
//	return nil
//}

/* --- KeyValue functions --- */

// Create a new key/value if it does not already exist
func NewKeyValue(db *Database, id string) *KeyValue {
	name := []byte(id)
	(*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return fmt.Errorf("Could not create bucket: %s", err)
		}
		return nil
	})
	return &KeyValue{db, name}
}

// Set a key and value
func (kv *KeyValue) Set(key, value string) error {
	if kv.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Put([]byte(key), []byte(value))
	})
}

// Get a value given a key
// Returns an error if the key was not found
func (kv *KeyValue) Get(key string) (val string, err error) {
	if kv.name == nil {
		return "", ErrDoesNotExist
	}
	err = (*bolt.DB)(kv.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		byteval := bucket.Get([]byte(key))
		if byteval == nil {
			return ErrKeyNotFound
		}
		val = string(byteval)
		return nil
	})
	return
}

// Remove a key
func (kv *KeyValue) Del(key string) error {
	if kv.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Delete([]byte(key))
	})
}

//// Increase the value of a key, returns the new value
//// Returns an empty string if there were errors,
//// or "0" if the key does not already exist.
func (kv *KeyValue) Inc(key string) (val string, err error) {
	if kv.name == nil {
		kv.name = []byte(key)
	}
	return val, (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		// The numeric value
		num := 0
		// Get the string value
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			// Create the bucket if it does not already exist
			bucket, err = tx.CreateBucketIfNotExists(kv.name)
			if err != nil {
				return fmt.Errorf("Could not create bucket: %s", err)
			}
		} else {
			val := string(bucket.Get([]byte(key)))
			if converted, err := strconv.Atoi(val); err == nil {
				// Conversion successful
				num = converted
			}
		}
		// Num is now either 0 or the previous numeric value
		num++
		// Convert the new value to a string and save it
		val = strconv.Itoa(num)
		err = bucket.Put([]byte(key), []byte(val))
		return err
	})
}

// Remove this key/value
func (kv *KeyValue) Remove() error {
	err := (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(kv.name))
	})
	kv.name = nil
	return err
}
