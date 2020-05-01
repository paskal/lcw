// Package cache implements LoadingCache.
//
// Support size-based eviction with LRC and LRU, and TTL based eviction.
package cache

import (
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// LoadingCache defines loading cache interface
type LoadingCache interface {
	Get(key string) (interface{}, bool)
	Peek(key string) (interface{}, bool)
	Set(key string, value interface{})
	Keys() []string
	ItemCount() int
	Invalidate(key string)
	InvalidateFn(fn func(key string) bool)
	Purge()
	DeleteExpired()
	Close()
}

// loadingCacheImpl provides loading cache with on-demand loading, similar to Guava LoadingCache.
// Implements cache.LoadingCache. Cache is consistent, i.e. read guaranteed to get latest write,
// and no stale writes in any place. In order to provide such
// consistency loadingCacheImpl locking on particular key, but no locks across multiple keys.
type loadingCacheImpl struct {
	purgeEvery time.Duration
	ttl        time.Duration
	maxKeys    int64
	isLRU      bool
	done       chan struct{}
	onEvicted  func(key string, value interface{})

	sync.Mutex
	data       map[string]interface{}
	expiresAt  map[string]time.Time
	lastReadAt map[string]time.Time
}

const noEvictionTTL = time.Hour * 24 * 365 * 10

// NewLoadingCache returns a new cache, activates purge with purgeEvery (0 to never purge).
// Default MaxKeys is unlimited (0).
func NewLoadingCache(options ...Option) (LoadingCache, error) {
	res := loadingCacheImpl{
		data:       map[string]interface{}{},
		expiresAt:  map[string]time.Time{},
		lastReadAt: map[string]time.Time{},
		purgeEvery: 0,
		maxKeys:    0,
		done:       make(chan struct{}),
	}

	for _, opt := range options {
		if err := opt(&res); err != nil {
			return nil, errors.Wrap(err, "failed to set cache option")
		}
	}

	if res.ttl == 0 {
		res.ttl = noEvictionTTL // very long ttl to prevent eviction
	}

	if res.maxKeys > 0 || res.purgeEvery > 0 {
		if res.purgeEvery == 0 {
			res.purgeEvery = time.Minute * 5 // non-zero purge enforced because maxKeys defined
		}
		go func(done <-chan struct{}) {
			ticker := time.NewTicker(res.purgeEvery)
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					res.Lock()
					res.purge(res.maxKeys)
					res.Unlock()
				}
			}
		}(res.done)
	}
	return &res, nil
}

// Set key
func (c *loadingCacheImpl) Set(key string, value interface{}) {
	c.Lock()
	defer c.Unlock()

	now := time.Now()
	c.data[key] = value
	c.expiresAt[key] = now.Add(c.ttl)
	if c.isLRU {
		c.lastReadAt[key] = now
	}

	if c.maxKeys > 0 && int64(len(c.data)) >= c.maxKeys*2 {
		c.purge(c.maxKeys)
	}
}

// Get returns the key value
func (c *loadingCacheImpl) Get(key string) (interface{}, bool) {
	c.Lock()
	defer c.Unlock()
	value, ok := c.getValue(key)
	if !ok {
		return nil, false
	}
	if c.isLRU {
		c.lastReadAt[key] = time.Now()
	}
	return value, ok
}

// Peek returns the key value (or undefined if not found) without updating the "recently used"-ness of the key.
func (c *loadingCacheImpl) Peek(key string) (interface{}, bool) {
	c.Lock()
	defer c.Unlock()
	value, ok := c.getValue(key)
	if !ok {
		return nil, false
	}
	return value, ok
}

// Invalidate key (item) from the cache
func (c *loadingCacheImpl) Invalidate(key string) {
	c.Lock()
	if value, ok := c.data[key]; ok {
		if c.onEvicted != nil {
			c.onEvicted(key, value)
		}
		delete(c.data, key)
		delete(c.expiresAt, key)
		if c.isLRU {
			delete(c.lastReadAt, key)
		}
	}
	c.Unlock()
}

// InvalidateFn deletes multiple keys if predicate is true
func (c *loadingCacheImpl) InvalidateFn(fn func(key string) bool) {
	c.Lock()
	for key, value := range c.data {
		if fn(key) {
			if c.onEvicted != nil {
				c.onEvicted(key, value)
			}
			delete(c.data, key)
			delete(c.expiresAt, key)
			if c.isLRU {
				delete(c.lastReadAt, key)
			}
		}
	}
	c.Unlock()
}

// Keys return slice of current keys in the cache
func (c *loadingCacheImpl) Keys() []string {
	c.Lock()
	defer c.Unlock()
	keys := make([]string, len(c.data))
	var i = 0
	for k := range c.data {
		keys[i] = k
		i++
	}
	return keys
}

// get value respecting the expiration, should be called with lock
func (c *loadingCacheImpl) getValue(key string) (interface{}, bool) {
	value, ok := c.data[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(c.expiresAt[key]) {
		return nil, false
	}
	return value, ok
}

// Purge clears the cache completely.
func (c *loadingCacheImpl) Purge() {
	c.Lock()
	defer c.Unlock()
	c.purge(-1)
}

// DeleteExpired clears cache of expired items
func (c *loadingCacheImpl) DeleteExpired() {
	c.Lock()
	defer c.Unlock()
	c.purge(0)
}

// ItemCount return count of items in cache
func (c *loadingCacheImpl) ItemCount() int {
	c.Lock()
	n := len(c.data)
	c.Unlock()
	return n
}

// Close cleans the cache and destroys running goroutines
func (c *loadingCacheImpl) Close() {
	c.Lock()
	defer c.Unlock()
	close(c.done)
	c.purge(-1)
}

// keysWithTs includes list of keys with ts. This is for sorting keys
// in order to provide least recently added sorting for size-based eviction
type keysWithTs []struct {
	key string
	ts  time.Time
}

// purge records > maxKeys. Has to be called with lock!
// call with maxKeys 0 will only clear expired entries, with -1 will clear everything.
func (c *loadingCacheImpl) purge(maxKeys int64) {
	kts := keysWithTs{}

	for key, value := range c.data {
		// ttl eviction
		if time.Now().After(c.expiresAt[key]) {
			if c.onEvicted != nil {
				c.onEvicted(key, value)
			}
			delete(c.data, key)
			delete(c.expiresAt, key)
			if c.isLRU {
				delete(c.lastReadAt, key)
			}
		}

		// prepare list of keysWithTs for size eviction
		if maxKeys == -1 || (maxKeys > 0 && int64(len(c.data)) > maxKeys) {
			ts := c.expiresAt[key] // for no-LRU sort by expiration time
			if c.isLRU {           // for LRU sort by read time
				ts = c.lastReadAt[key]
			}

			kts = append(kts, struct {
				key string
				ts  time.Time
			}{key, ts})
		}
	}

	// size eviction
	size := int64(len(c.data))
	if maxKeys == -1 { // clean everything in case maxKeys is -1
		maxKeys = 0
	}
	if len(kts) > 0 {
		sort.Slice(kts, func(i int, j int) bool { return kts[i].ts.Before(kts[j].ts) })
		for d := 0; int64(d) < size-maxKeys; d++ {
			key := kts[d].key
			if c.onEvicted != nil {
				c.onEvicted(key, c.data[key])
			}
			delete(c.data, key)
			delete(c.expiresAt, key)
			if c.isLRU {
				delete(c.lastReadAt, key)
			}
		}
	}
}
