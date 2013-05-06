package simpleredis

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

// Common for each of the redis datastructures used here
type redisDatastructure struct {
	pool *ConnectionPool
	id   string
}

type (
	// A pool of readily available Redis connections
	ConnectionPool redis.Pool

	List     redisDatastructure
	Set      redisDatastructure
	HashMap  redisDatastructure
	KeyValue redisDatastructure
)

const (
	// Version number
	Version = 0.1
	// How many connections should stay ready for requests, at a maximum?
	maxIdleConnections = 3
	// The default [url]:port that Redis is running at
	defaultRedisServer = ":6379"
)

/* --- Helper functions --- */

// Connect to the local instance of Redis at port 6379
func newRedisConnection() (redis.Conn, error) {
	return redis.Dial("tcp", defaultRedisServer)
}

// Connect to host:port, host may be omitted, so ":6379" is valid
func newRedisConnectionTo(hostColonPort string) (redis.Conn, error) {
	return redis.Dial("tcp", hostColonPort)
}

// Get a string from a list of results at a given position
func getString(bi []interface{}, i int) string {
	return string(bi[i].([]uint8))
}

/* --- ConnectionPool functions --- */

// Create a new connection pool
func NewConnectionPool() *ConnectionPool {
	// The second argument is the maximum number of idle connections
	redisPool := redis.NewPool(newRedisConnection, maxIdleConnections)
	pool := ConnectionPool(*redisPool)
	return &pool
}

// Get one of the available connections from the connection pool
func (pool *ConnectionPool) Get() redis.Conn {
	redisPool := redis.Pool(*pool)
	return redisPool.Get()
}

// Close down the connection pool
func (pool *ConnectionPool) Close() {
	redisPool := redis.Pool(*pool)
	redisPool.Close()
}

/* --- List functions --- */

// Create a new list
func NewList(pool *ConnectionPool, id string) *List {
	return &List{pool, id}
}

// Add an element to the list
func (rl *List) Add(value string) error {
	conn := rl.pool.Get()
	_, err := conn.Do("RPUSH", rl.id, value)
	return err
}

// Get all elements of a list
func (rl *List) GetAll() ([]string, error) {
	conn := rl.pool.Get()
	result, err := redis.Values(conn.Do("LRANGE", rl.id, "0", "-1"))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Get the last element of a list
func (rl *List) GetLast() (string, error) {
	conn := rl.pool.Get()
	result, err := redis.Values(conn.Do("LRANGE", rl.id, "-1", "-1"))
	if len(result) == 1 {
		return getString(result, 0), err
	}
	return "", err
}

// Get the last N elements of a list
func (rl *List) GetLastN(n int) ([]string, error) {
	conn := rl.pool.Get()
	result, err := redis.Values(conn.Do("LRANGE", rl.id, "-"+strconv.Itoa(n), "-1"))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Remove this list
func (rl *List) Remove() error {
	conn := rl.pool.Get()
	_, err := conn.Do("DEL", rl.id)
	return err
}

/* --- Set functions --- */

// Create a new set
func NewSet(pool *ConnectionPool, id string) *Set {
	return &Set{pool, id}
}

// Add an element to the set
func (rs *Set) Add(value string) error {
	conn := rs.pool.Get()
	_, err := conn.Do("SADD", rs.id, value)
	return err
}

// Check if a given value is in the set
func (rs *Set) Has(value string) (bool, error) {
	conn := rs.pool.Get()
	retval, err := conn.Do("SISMEMBER", rs.id, value)
	if err != nil {
		panic(err)
	}
	return redis.Bool(retval, err)
}

// Get all elements of the set
func (rs *Set) GetAll() ([]string, error) {
	conn := rs.pool.Get()
	result, err := redis.Values(conn.Do("SMEMBERS", rs.id))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Remove an element from the set
func (rs *Set) Del(value string) error {
	conn := rs.pool.Get()
	_, err := conn.Do("SREM", rs.id, value)
	return err
}

// Remove this set
func (rs *Set) Remove() error {
	conn := rs.pool.Get()
	_, err := conn.Do("DEL", rs.id)
	return err
}

/* --- HashMap functions --- */

// Create a new hashmap
func NewHashMap(pool *ConnectionPool, id string) *HashMap {
	return &HashMap{pool, id}
}

// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (rh *HashMap) Set(elementid, key, value string) error {
	conn := rh.pool.Get()
	_, err := conn.Do("HSET", rh.id+":"+elementid, key, value)
	return err
}

// Get a value from a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (rh *HashMap) Get(elementid, key string) (string, error) {
	conn := rh.pool.Get()
	result, err := redis.String(conn.Do("HGET", rh.id+":"+elementid, key))
	if err != nil {
		return "", err
	}
	return result, nil
}

// Check if a given elementid + key is in the hash map
func (rh *HashMap) Has(elementid, key string) (bool, error) {
	conn := rh.pool.Get()
	retval, err := conn.Do("HEXISTS", rh.id+":"+elementid, key)
	if err != nil {
		panic(err)
	}
	return redis.Bool(retval, err)
}

// Check if a given elementid exists as a hash map at all
func (rh *HashMap) Exists(elementid string) (bool, error) {
	// TODO: key is not meant to be a wildcard, check for "*"
	return hasKey(rh.pool, rh.id+":"+elementid)
}

// Get all elementid's for all hash elements
func (rh *HashMap) GetAll() ([]string, error) {
	conn := rh.pool.Get()
	result, err := redis.Values(conn.Do("KEYS", rh.id+":*"))
	strs := make([]string, len(result))
	idlen := len(rh.id)
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)[idlen+1:]
	}
	return strs, err
}

// Remove a key for an entry in a hashmap (for instance the email field for a user)
func (rh *HashMap) DelKey(elementid, key string) error {
	conn := rh.pool.Get()
	_, err := conn.Do("HDEL", rh.id+":"+elementid, key)
	return err
}

// Remove a hashmap (for instance a user)
func (rh *HashMap) Del(elementid string) error {
	conn := rh.pool.Get()
	_, err := conn.Do("DEL", rh.id+":"+elementid)
	return err
}

// Remove this hashmap
func (rh *HashMap) Remove() error {
	conn := rh.pool.Get()
	_, err := conn.Do("DEL", rh.id)
	return err
}

/* --- KeyValue functions --- */

// Create a new key/value
func NewKeyValue(pool *ConnectionPool, id string) *KeyValue {
	return &KeyValue{pool, id}
}

// Set a key and value
func (rkv *KeyValue) Set(key, value string) error {
	conn := rkv.pool.Get()
	_, err := conn.Do("SET", rkv.id+":"+key, value)
	return err
}

// Get a value given a key
func (rkv *KeyValue) Get(key string) (string, error) {
	conn := rkv.pool.Get()
	result, err := redis.String(conn.Do("GET", rkv.id+":"+key))
	if err != nil {
		return "", err
	}
	return result, nil
}

// Remove a key
func (rkv *KeyValue) Del(key string) error {
	conn := rkv.pool.Get()
	_, err := conn.Do("DEL", rkv.id+":"+key)
	return err
}

// Remove this key/value
func (rkv *KeyValue) Remove() error {
	conn := rkv.pool.Get()
	_, err := conn.Do("DEL", rkv.id)
	return err
}

// --- Generic redis functions ---

// Check if a key exists. The key can be a wildcard (ie. "user*").
func hasKey(pool *ConnectionPool, wildcard string) (bool, error) {
	conn := pool.Get()
	result, err := redis.Values(conn.Do("KEYS", wildcard))
	if err != nil {
		return false, err
	}
	if len(result) > 0 {
		return true, nil
	}
	return false, nil
}
