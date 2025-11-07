package cache

import (
	"sync"
	"time"
)

// memoryCache provides an in-memory cache layer for frequently accessed data
type memoryCache struct {
	mu      sync.RWMutex
	items   map[string]*cacheItem
	maxSize int
}

type cacheItem struct {
	value      []byte
	expiration time.Time
}

// newMemoryCache creates a new in-memory cache with specified max size
func newMemoryCache(maxSize int) *memoryCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default max size
	}
	return &memoryCache{
		items:   make(map[string]*cacheItem),
		maxSize: maxSize,
	}
}

// get retrieves a value from the cache
func (m *memoryCache) get(key string) ([]byte, bool) {
	m.mu.RLock()
	item, ok := m.items[key]
	m.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Check if expired
	now := time.Now()
	if now.After(item.expiration) {
		// Remove expired item to prevent memory leak
		m.mu.Lock()
		// Double-check after acquiring write lock to handle race conditions
		if item, ok := m.items[key]; ok && now.After(item.expiration) {
			delete(m.items, key)
		}
		m.mu.Unlock()
		return nil, false
	}

	return item.value, true
}

// set stores a value in the cache with a TTL
func (m *memoryCache) set(key string, value []byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If we're at capacity and adding a new key, evict oldest
	if len(m.items) >= m.maxSize {
		if _, exists := m.items[key]; !exists {
			// Simple eviction: remove first item from map iteration
			// Note: Map iteration order is non-deterministic in Go, which is acceptable
			// for this cache as we prioritize simplicity over LRU semantics
			for k := range m.items {
				delete(m.items, k)
				break
			}
		}
	}

	m.items[key] = &cacheItem{
		value:      value,
		expiration: time.Now().Add(ttl),
	}
}

// delete removes a value from the cache
func (m *memoryCache) delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
}

// clear removes all items from the cache
func (m *memoryCache) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = make(map[string]*cacheItem)
}
