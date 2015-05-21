// Simplebolt provides a way to use Bolt that is similar to simpleredis
package simplebolt

import (
	//"strconv"
	"fmt"
	"log"
	"strings"

	"github.com/boltdb/bolt"
)

// Common for each of the Bolt buckets used here
type boltBucket struct {
	db *Database
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

///* --- List functions --- */
//
//// Create a new list
//func NewList(db *Database, id string) *List {
//	return &List{db, []byte(id)}
//}
//
//// Add an element to the list
//func (rl *List) Add(value string) error {
//	conn := rl.db.Get(rl.dbindex)
//	_, err := conn.Do("RPUSH", rl.id, value)
//	return err
//}
//
//// Get all elements of a list
//func (rl *List) GetAll() ([]string, error) {
//	conn := rl.db.Get(rl.dbindex)
//	result, err := bolt.Values(conn.Do("LRANGE", rl.id, "0", "-1"))
//	strs := make([]string, len(result))
//	for i := 0; i < len(result); i++ {
//		strs[i] = getString(result, i)
//	}
//	return strs, err
//}
//
//// Get the last element of a list
//func (rl *List) GetLast() (string, error) {
//	conn := rl.db.Get(rl.dbindex)
//	result, err := bolt.Values(conn.Do("LRANGE", rl.id, "-1", "-1"))
//	if len(result) == 1 {
//		return getString(result, 0), err
//	}
//	return "", err
//}
//
//// Get the last N elements of a list
//func (rl *List) GetLastN(n int) ([]string, error) {
//	conn := rl.db.Get(rl.dbindex)
//	result, err := bolt.Values(conn.Do("LRANGE", rl.id, "-"+strconv.Itoa(n), "-1"))
//	strs := make([]string, len(result))
//	for i := 0; i < len(result); i++ {
//		strs[i] = getString(result, i)
//	}
//	return strs, err
//}
//
//// Remove this list
//func (rl *List) Remove() error {
//	conn := rl.db.Get(rl.dbindex)
//	_, err := conn.Do("DEL", rl.id)
//	return err
//}
//
///* --- Set functions --- */
//
//// Create a new set
//func NewSet(db *Database, id string) *Set {
//	return &Set{db, id, 0}
//}
//
//// Add an element to the set
//func (rs *Set) Add(value string) error {
//	conn := rs.db.Get(rs.dbindex)
//	_, err := conn.Do("SADD", rs.id, value)
//	return err
//}
//
//// Check if a given value is in the set
//func (rs *Set) Has(value string) (bool, error) {
//	conn := rs.db.Get(rs.dbindex)
//	retval, err := conn.Do("SISMEMBER", rs.id, value)
//	if err != nil {
//		panic(err)
//	}
//	return bolt.Bool(retval, err)
//}
//
//// Get all elements of the set
//func (rs *Set) GetAll() ([]string, error) {
//	conn := rs.db.Get(rs.dbindex)
//	result, err := bolt.Values(conn.Do("SMEMBERS", rs.id))
//	strs := make([]string, len(result))
//	for i := 0; i < len(result); i++ {
//		strs[i] = getString(result, i)
//	}
//	return strs, err
//}
//
//// Remove an element from the set
//func (rs *Set) Del(value string) error {
//	conn := rs.db.Get(rs.dbindex)
//	_, err := conn.Do("SREM", rs.id, value)
//	return err
//}
//
//// Remove this set
//func (rs *Set) Remove() error {
//	conn := rs.db.Get(rs.dbindex)
//	_, err := conn.Do("DEL", rs.id)
//	return err
//}
//
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
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	return &KeyValue{db, name}
}

// Set a key and value
func (kv *KeyValue) Set(key, value string) error {
	return (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		return bucket.Put([]byte(key), []byte(value))
	})
}

// Get a value given a key
func (kv *KeyValue) Get(key string) (val string, err error) {
	err = (*bolt.DB)(kv.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", kv.name)
		}
		val = string(bucket.Get([]byte(key)))
		return nil
	})
	return
}

//// Remove a key
//func (rkv *KeyValue) Del(key string) error {
//	conn := rkv.db.Get(rkv.dbindex)
//	_, err := conn.Do("DEL", rkv.id+":"+key)
//	return err
//}
//
//// Increase the value of a key, returns the new value
//// Returns an empty string if there were errors,
//// or "0" if the key does not already exist.
//func (rkv *KeyValue) Inc(key string) (string, error) {
//	conn := rkv.db.Get(rkv.dbindex)
//	result, err := bolt.Int64(conn.Do("INCR", rkv.id+":"+key))
//	if err != nil {
//		return "0", err
//	}
//	return strconv.FormatInt(result, 10), nil
//}
//
//// Remove this key/value
//func (rkv *KeyValue) Remove() error {
//	conn := rkv.db.Get(rkv.dbindex)
//	// Find all keys that starts with rkv.id+":"
//	results, err := bolt.Values(conn.Do("KEYS", rkv.id+":*"))
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
//
//// --- Generic bolt functions ---
//
//// Check if a key exists. The key can be a wildcard (ie. "user*").
//func hasKey(db *Database, wildcard string, dbindex int) (bool, error) {
//	conn := db.Get(dbindex)
//	result, err := bolt.Values(conn.Do("KEYS", wildcard))
//	if err != nil {
//		return false, err
//	}
//	return len(result) > 0, nil
//}
