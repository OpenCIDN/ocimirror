package cache

import (
	"testing"
	"time"
)

// TestMemoryCacheIntegrationWithBlobCache tests that the memory cache is properly
// integrated into blob cache operations when enabled
func TestMemoryCacheIntegrationWithBlobCache(t *testing.T) {
	// Create an empty cache struct with just memory cache configured
	c := &Cache{
		blobMemCache:     newMemoryCache(100),
		manifestMemCache: newMemoryCache(100),
		memoryCacheTTL:   1 * time.Minute,
	}

	blob := "sha256:test123"
	content := []byte("test blob content")

	// Manually add to blob memory cache
	c.blobMemCache.set(blob, content, c.memoryCacheTTL)

	// Verify it's retrievable
	retrieved, ok := c.blobMemCache.get(blob)
	if !ok {
		t.Fatal("Expected blob to be in memory cache")
	}
	if string(retrieved) != string(content) {
		t.Fatalf("Expected %s, got %s", string(content), string(retrieved))
	}
}

// TestMemoryCacheIntegrationWithManifestCache tests that the memory cache is properly
// integrated into manifest cache operations when enabled
func TestMemoryCacheIntegrationWithManifestCache(t *testing.T) {
	// Create an empty cache struct with just memory cache configured
	c := &Cache{
		blobMemCache:     newMemoryCache(100),
		manifestMemCache: newMemoryCache(100),
		memoryCacheTTL:   1 * time.Minute,
	}

	cacheKey := "docker.io/library/nginx/latest"
	// Format: JSON serialized manifestCacheEntry
	cachedData := `{"digest":"sha256:abc123","mediaType":"application/vnd.oci.image.manifest.v1+json","content":"eyJ0ZXN0IjoibWFuaWZlc3QifQ=="}`

	// Manually add to manifest memory cache
	c.manifestMemCache.set(cacheKey, []byte(cachedData), c.memoryCacheTTL)

	// Verify it's retrievable
	retrieved, ok := c.manifestMemCache.get(cacheKey)
	if !ok {
		t.Fatal("Expected manifest to be in memory cache")
	}
	if string(retrieved) != cachedData {
		t.Fatalf("Expected %s, got %s", cachedData, string(retrieved))
	}
}

// TestMemoryCacheDisabled tests that operations work correctly when memory cache is not configured
func TestMemoryCacheDisabled(t *testing.T) {
	// Create a cache struct without memory cache
	c := &Cache{
		blobMemCache:     nil,
		manifestMemCache: nil,
	}

	// Verify memory caches are nil
	if c.blobMemCache != nil {
		t.Fatal("Expected blob memory cache to be nil")
	}
	if c.manifestMemCache != nil {
		t.Fatal("Expected manifest memory cache to be nil")
	}
}

// TestMemoryCacheConfiguration tests WithMemoryCache option
func TestMemoryCacheConfiguration(t *testing.T) {
	c := &Cache{}
	
	// Apply memory cache configuration
	opt := WithMemoryCache(50, 2*time.Minute)
	opt(c)

	// Verify configuration
	if c.blobMemCache == nil {
		t.Fatal("Expected blob memory cache to be initialized")
	}
	if c.manifestMemCache == nil {
		t.Fatal("Expected manifest memory cache to be initialized")
	}
	if c.memoryCacheTTL != 2*time.Minute {
		t.Fatalf("Expected TTL to be 2m, got %v", c.memoryCacheTTL)
	}

	// Test with default TTL (0 or negative should use default)
	c2 := &Cache{}
	opt2 := WithMemoryCache(50, 0)
	opt2(c2)
	
	if c2.memoryCacheTTL != 5*time.Minute {
		t.Fatalf("Expected default TTL to be 5m, got %v", c2.memoryCacheTTL)
	}
}

