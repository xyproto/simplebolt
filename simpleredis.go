package simpleredis

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

// Functions for dealing with string values in a simple fashion in Redis

type RedisDatastructure struct {
	pool *ConnectionPool
	id   string
}

type (
	RedisList     RedisDatastructure
	RedisSet      RedisDatastructure
	RedisHashMap  RedisDatastructure
	RedisKeyValue RedisDatastructure

	ConnectionPool redis.Pool
)

const (
	// How many connections should stay ready for requests
	MAXIMUM_NUMBER_OF_IDLE_CONNECTIONS = 3
	// The url:port that Redis is running at
	REDIS_SERVER = ":6379"
)

/* --- Helper functions --- */

// Connect to the local instance of Redis at port 6379
func newRedisConnection() (redis.Conn, error) {
	return redis.Dial("tcp", REDIS_SERVER)
}

// Get a string from a list of results at a given position
func getString(bi []interface{}, i int) string {
	return string(bi[i].([]uint8))
}

/* --- ConnectionPool functions --- */

// Create a new connection pool
func NewRedisConnectionPool() *ConnectionPool {
	// The second argument is the maximum number of idle connections
	redisPool := redis.NewPool(newRedisConnection, MAXIMUM_NUMBER_OF_IDLE_CONNECTIONS)
	pool := ConnectionPool(*redisPool)
	return &pool
}

// Get an available connection from the connection pool
func (pool *ConnectionPool) Get() redis.Conn {
	redisPool := redis.Pool(*pool)
	return redisPool.Get()
}

// Close down the connection pool
func (pool *ConnectionPool) Close() {
	redisPool := redis.Pool(*pool)
	redisPool.Close()
}

/* --- RedisList functions --- */

// Create a new list
func NewRedisList(pool *ConnectionPool, id string) *RedisList {
	return &RedisList{pool, id}
}

// Add an element to the list
func (rl *RedisList) Add(value string) error {
	conn := rl.pool.Get()
	_, err := conn.Do("RPUSH", rl.id, value)
	return err
}

// Get all elements of a list
func (rl *RedisList) GetAll() ([]string, error) {
	conn := rl.pool.Get()
	result, err := redis.Values(conn.Do("LRANGE", rl.id, "0", "-1"))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Get the last element of a list
func (rl *RedisList) GetLast() (string, error) {
	conn := rl.pool.Get()
	result, err := redis.Values(conn.Do("LRANGE", rl.id, "-1", "-1"))
	if len(result) == 1 {
		return getString(result, 0), err
	}
	return "", err
}

// Get the last N elements of a list
func (rl *RedisList) GetLastN(n int) ([]string, error) {
	conn := rl.pool.Get()
	result, err := redis.Values(conn.Do("LRANGE", rl.id, "-"+strconv.Itoa(n), "-1"))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Delete an entire list
func (rl *RedisList) DelAll() error {
	conn := rl.pool.Get()
	_, err := conn.Do("DEL", rl.id)
	return err
}

/* --- RedisSet functions --- */

// Create a new set
func NewRedisSet(pool *ConnectionPool, id string) *RedisSet {
	return &RedisSet{pool, id}
}

// Add an element to the set
func (rs *RedisSet) Add(value string) error {
	conn := rs.pool.Get()
	_, err := conn.Do("SADD", rs.id, value)
	return err
}

// Check if a given value is in the set
func (rs *RedisSet) Has(value string) (bool, error) {
	conn := rs.pool.Get()
	retval, err := conn.Do("SISMEMBER", rs.id, value)
	if err != nil {
		panic(err)
	}
	return redis.Bool(retval, err)
}

// Get all elements of the set
func (rs *RedisSet) GetAll() ([]string, error) {
	conn := rs.pool.Get()
	result, err := redis.Values(conn.Do("SMEMBERS", rs.id))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Remove an element from the set
func (rs *RedisSet) Del(value string) error {
	conn := rs.pool.Get()
	_, err := conn.Do("SREM", rs.id, value)
	return err
}

// Delete an entire set
func (rs *RedisSet) DelAll() error {
	conn := rs.pool.Get()
	_, err := conn.Do("DEL", rs.id)
	return err
}

/* --- RedisHashMap functions --- */

// Create a new hashmap
func NewRedisHashMap(pool *ConnectionPool, id string) *RedisHashMap {
	return &RedisHashMap{pool, id}
}

// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (rh *RedisHashMap) Set(elementid, key, value string) error {
	conn := rh.pool.Get()
	_, err := conn.Do("HSET", rh.id+":"+elementid, key, value)
	return err
}

// Get a value from a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (rh *RedisHashMap) Get(elementid, key string) (string, error) {
	conn := rh.pool.Get()
	result, err := redis.String(conn.Do("HGET", rh.id+":"+elementid, key))
	if err != nil {
		return "", err
	}
	return result, nil
}

// Delete an entry in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (rh *RedisHashMap) Del(elementid, key string) error {
	conn := rh.pool.Get()
	_, err := conn.Do("HDEL", rh.id+":"+elementid, key)
	return err
}

// Delete an entire hashmap
func (rh *RedisHashMap) DelAll() error {
	conn := rh.pool.Get()
	_, err := conn.Do("DEL", rh.id)
	return err
}

/* --- RedisKeyValue functions --- */

// Create a new key/value 
func NewRedisKeyValue(pool *ConnectionPool, id string) *RedisKeyValue {
	return &RedisKeyValue{pool, id}
}

// Set a key and value
func (rkv *RedisKeyValue) Set(key, value string) error {
	conn := rkv.pool.Get()
	_, err := conn.Do("SET", rkv.id+":"+key, value)
	return err
}

// Get a value given a key
func (rkv *RedisKeyValue) Get(key string) (string, error) {
	conn := rkv.pool.Get()
	result, err := redis.String(conn.Do("GET", rkv.id+":"+key))
	if err != nil {
		return "", err
	}
	return result, nil
}

// Delete a key
func (rkv *RedisKeyValue) Del(key string) error {
	conn := rkv.pool.Get()
	_, err := conn.Do("DEL", rkv.id+":"+key)
	return err
}

// Delete a key/value
func (rkv *RedisKeyValue) DelAll() error {
	conn := rkv.pool.Get()
	_, err := conn.Do("DEL", rkv.id)
	return err
}
