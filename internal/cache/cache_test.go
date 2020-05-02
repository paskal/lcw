package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestLoadingCacheNoPurge(t *testing.T) {
	lc, err := NewLoadingCache()
	assert.NoError(t, err)

	lc.Set("key1", "val1")
	assert.Equal(t, 1, lc.ItemCount())

	v, ok := lc.Peek("key1")
	assert.Equal(t, "val1", v)
	assert.True(t, ok)

	v, ok = lc.Peek("key2")
	assert.Empty(t, v)
	assert.False(t, ok)

	assert.Equal(t, []string{"key1"}, lc.Keys())
}

func TestLoadingCacheWithPurge(t *testing.T) {
	var evicted int
	lc, err := NewLoadingCache(
		LRU(),
		PurgeEvery(time.Millisecond*100),
		TTL(150*time.Millisecond),
		OnEvicted(func(_ string, _ interface{}) { evicted++ }),
	)
	assert.NoError(t, err)
	defer lc.Close()

	lc.Set("key1", "val1")

	time.Sleep(100 * time.Millisecond) // not enough to expire
	assert.Equal(t, 1, lc.ItemCount())

	v, ok := lc.Get("key1")
	assert.Equal(t, "val1", v)
	assert.True(t, ok)

	time.Sleep(200 * time.Millisecond) // expire
	v, ok = lc.Get("key1")
	assert.False(t, ok)
	assert.Nil(t, v)

	assert.Equal(t, 0, lc.ItemCount())
	assert.Equal(t, 1, evicted)

	// add new entry and  everything
	lc.Set("key2", "val2")
	assert.Equal(t, 1, lc.ItemCount())

	// DeleteExpired, nothing deleted
	lc.DeleteExpired()
	assert.Equal(t, 1, lc.ItemCount())
	assert.Equal(t, 1, evicted)

	// Purge, cache should be clear
	lc.Purge()
	assert.Equal(t, 0, lc.ItemCount())
	assert.Equal(t, 2, evicted)
}

func TestLoadingCacheWithPurgeEnforcedBySize(t *testing.T) {
	lc, err := NewLoadingCache(MaxKeys(10))
	assert.NoError(t, err)
	defer lc.Close()

	for i := 0; i < 100; i++ {
		i := i
		lc.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
		v, ok := lc.Get(fmt.Sprintf("key%d", i))
		assert.Equal(t, fmt.Sprintf("val%d", i), v)
		assert.True(t, ok)
		assert.True(t, lc.ItemCount() < 20)
	}

	assert.Equal(t, 10, lc.ItemCount())
}

func TestLoadingCacheWithPurgeMax(t *testing.T) {
	lc, err := NewLoadingCache(PurgeEvery(time.Millisecond*50), MaxKeys(2))
	assert.NoError(t, err)
	defer lc.Close()

	lc.Set("key1", "val1")
	lc.Set("key2", "val2")
	lc.Set("key3", "val3")
	assert.Equal(t, 3, lc.ItemCount())

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 2, lc.ItemCount())

	_, found := lc.Get("key1")
	assert.False(t, found, "key1 should be deleted")
}

func TestLoadingCacheWithPurgeMaxLru(t *testing.T) {
	lc, err := NewLoadingCache(PurgeEvery(time.Millisecond*50), MaxKeys(2), LRU())
	assert.NoError(t, err)
	defer lc.Close()

	lc.Set("key1", "val1")
	lc.Set("key2", "val2")
	lc.Set("key3", "val3")
	assert.Equal(t, 3, lc.ItemCount())

	// read key1 again, changes LRU
	v, ok := lc.Get("key1")
	assert.Equal(t, "val1", v)
	assert.True(t, ok)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 2, lc.ItemCount())

	_, found := lc.Get("key2")
	assert.False(t, found, "key2 should be deleted")
}

func TestLoadingCacheConcurrency(t *testing.T) {
	lc, err := NewLoadingCache()
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			lc.Set(fmt.Sprintf("key-%d", i/10), fmt.Sprintf("val-%d", i/10))
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 100, lc.ItemCount())
}

func TestLoadingCacheInvalidateAndEvict(t *testing.T) {
	var evicted int
	lc, err := NewLoadingCache(LRU(), OnEvicted(func(_ string, _ interface{}) { evicted++ }))
	assert.NoError(t, err)

	lc.Set("key1", "val1")
	lc.Set("key2", "val2")

	val, ok := lc.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "val1", val)
	assert.Equal(t, 0, evicted)

	lc.Invalidate("key1")
	assert.Equal(t, 1, evicted)
	val, ok = lc.Get("key1")
	assert.Empty(t, val)
	assert.False(t, ok)

	val, ok = lc.Get("key2")
	assert.True(t, ok)
	assert.Equal(t, "val2", val)

	lc.InvalidateFn(func(key string) bool {
		return key == "key2"
	})
	assert.Equal(t, 2, evicted)
	_, ok = lc.Get("key2")
	assert.False(t, ok)
	assert.Equal(t, 0, lc.ItemCount())
}

func TestLoadingCacheBadOption(t *testing.T) {
	lc, err := NewLoadingCache(func(lc *loadingCacheImpl) error {
		return errors.New("mock err")
	})
	assert.EqualError(t, err, "failed to set cache option: mock err")
	assert.Nil(t, lc)
}

func TestLoadingExpired(t *testing.T) {
	lc, err := NewLoadingCache(TTL(time.Millisecond * 5))
	assert.NoError(t, err)

	lc.Set("key1", "val1")
	assert.Equal(t, 1, lc.ItemCount())

	v, ok := lc.Peek("key1")
	assert.Equal(t, v, "val1")
	assert.True(t, ok)

	v, ok = lc.Get("key1")
	assert.Equal(t, v, "val1")
	assert.True(t, ok)

	time.Sleep(time.Millisecond * 10)  // wait for entry to expire
	assert.Equal(t, 1, lc.ItemCount()) // but not purged

	v, ok = lc.Peek("key1")
	assert.Empty(t, v)
	assert.False(t, ok)

	v, ok = lc.Get("key1")
	assert.Empty(t, v)
	assert.False(t, ok)
}
