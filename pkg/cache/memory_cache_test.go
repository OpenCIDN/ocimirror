package cache

import (
	"testing"
	"time"
)

func TestMemoryCacheBasicOperations(t *testing.T) {
	mc := newMemoryCache(10)

	// Test set and get
	key := "test-key"
	value := []byte("test-value")
	mc.set(key, value, 1*time.Minute)

	retrieved, ok := mc.get(key)
	if !ok {
		t.Fatal("Expected to retrieve value from cache")
	}
	if string(retrieved) != string(value) {
		t.Fatalf("Expected %s, got %s", string(value), string(retrieved))
	}

	// Test non-existent key
	_, ok = mc.get("non-existent")
	if ok {
		t.Fatal("Expected false for non-existent key")
	}
}

func TestMemoryCacheExpiration(t *testing.T) {
	mc := newMemoryCache(10)

	key := "expire-test"
	value := []byte("will-expire")
	mc.set(key, value, 100*time.Millisecond)

	// Should be available immediately
	_, ok := mc.get(key)
	if !ok {
		t.Fatal("Expected value to be in cache")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should be expired now
	_, ok = mc.get(key)
	if ok {
		t.Fatal("Expected value to be expired")
	}
}

func TestMemoryCacheDelete(t *testing.T) {
	mc := newMemoryCache(10)

	key := "delete-test"
	value := []byte("to-be-deleted")
	mc.set(key, value, 1*time.Minute)

	// Verify it's there
	_, ok := mc.get(key)
	if !ok {
		t.Fatal("Expected value to be in cache")
	}

	// Delete it
	mc.delete(key)

	// Should be gone
	_, ok = mc.get(key)
	if ok {
		t.Fatal("Expected value to be deleted from cache")
	}
}

func TestMemoryCacheClear(t *testing.T) {
	mc := newMemoryCache(10)

	// Add multiple items
	for i := 0; i < 5; i++ {
		mc.set(string(rune(i)), []byte("value"), 1*time.Minute)
	}

	// Clear all
	mc.clear()

	// All should be gone
	for i := 0; i < 5; i++ {
		_, ok := mc.get(string(rune(i)))
		if ok {
			t.Fatal("Expected all values to be cleared")
		}
	}
}

func TestMemoryCacheMaxSize(t *testing.T) {
	maxSize := 5
	mc := newMemoryCache(maxSize)

	// Add items up to max size
	for i := 0; i < maxSize; i++ {
		key := string(rune('a' + i))
		mc.set(key, []byte("value"), 1*time.Minute)
	}

	// Add one more - should evict one item
	mc.set("extra", []byte("extra-value"), 1*time.Minute)

	// Cache should not exceed max size
	mc.mu.RLock()
	size := len(mc.items)
	mc.mu.RUnlock()

	if size > maxSize {
		t.Fatalf("Cache size %d exceeds max size %d", size, maxSize)
	}
}

func TestMemoryCacheConcurrency(t *testing.T) {
	mc := newMemoryCache(100)

	// Run concurrent operations
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 100; j++ {
				key := string(rune(n))
				mc.set(key, []byte("value"), 1*time.Minute)
				mc.get(key)
				if j%10 == 0 {
					mc.delete(key)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
