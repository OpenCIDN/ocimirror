# In-Memory Cache for Manifest and Blob

## Overview

This feature adds an optional in-memory cache layer that sits in front of the storage backend for manifest and blob operations. The cache reduces latency and I/O operations by storing frequently accessed data in memory.

## Features

- **Thread-safe**: Uses RWMutex for concurrent access
- **TTL-based expiration**: Automatically expires old entries
- **Configurable size**: Set maximum number of items to cache
- **Automatic cleanup**: Removes expired items to prevent memory leaks
- **Zero impact**: Optional feature - when disabled, behavior is unchanged

## Usage

### Enabling In-Memory Cache

To enable in-memory caching, use the `WithMemoryCache` option when creating a cache:

```go
import (
    "time"
    "github.com/OpenCIDN/ocimirror/pkg/cache"
)

// Create cache with in-memory caching enabled
c, err := cache.NewCache(
    cache.WithMemoryCache(
        1000,              // Max 1000 items
        5 * time.Minute,   // 5 minute TTL
    ),
    // ... other options
)
```

### Configuration Parameters

#### maxSize (int)
- Maximum number of items to store in memory
- When capacity is reached, oldest items are evicted
- Default: 1000 (if 0 or negative is provided)

#### ttl (time.Duration)
- Time-to-live for cached items
- Items are automatically removed after expiration
- Default: 5 minutes (if 0 or negative is provided)

### Without In-Memory Cache

If you don't want in-memory caching, simply omit the `WithMemoryCache` option:

```go
c, err := cache.NewCache(
    // ... other options, but no WithMemoryCache
)
```

## Implementation Details

### Cache Keys

- **Blob cache**: Uses the blob digest as the key
- **Manifest cache**: Uses base64-encoded composite key (host + image + tag) to prevent collisions

### Serialization

- **Blob content**: Stored directly as byte slice
- **Manifest content**: JSON-serialized with digest, mediaType, and content

### Eviction Policy

- Simple non-deterministic eviction based on Go map iteration order
- Prioritizes simplicity over strict LRU semantics
- Acceptable for cache use case where any eviction is valid

### Thread Safety

- Uses RWMutex for optimal read performance
- Read operations use RLock
- Write/delete operations use Lock
- Double-check pattern prevents race conditions during expiration cleanup

## Performance Considerations

### Memory Usage

Each cached item stores:
- Key (string)
- Value (byte slice)
- Expiration timestamp

Approximate memory per item: `len(key) + len(value) + 24 bytes`

For 1000 items with average manifest size of 10KB:
- Memory usage: ~10-15 MB

### Cache Hit Benefits

- Eliminates storage backend I/O for cached items
- Reduces latency from ~100ms (storage) to <1ms (memory)
- Particularly beneficial for frequently accessed manifests

### Recommendations

- **Small deployments**: 1000 items, 5 minute TTL
- **Medium deployments**: 5000 items, 10 minute TTL
- **Large deployments**: 10000+ items, 15 minute TTL

Adjust based on:
- Available memory
- Access patterns
- Manifest update frequency

## Testing

Comprehensive tests are included:

```bash
# Run cache tests
go test ./pkg/cache/...

# Run with race detector
go test -race ./pkg/cache/...

# Run with coverage
go test -cover ./pkg/cache/...
```

## Examples

### Basic Setup

```go
cache, err := cache.NewCache(
    cache.WithMemoryCache(1000, 5*time.Minute),
    cache.WithStorageDriver(storageDriver),
)
```

### Large Scale Setup

```go
cache, err := cache.NewCache(
    cache.WithMemoryCache(10000, 15*time.Minute),
    cache.WithStorageDriver(storageDriver),
)
```

### Disabled (Backward Compatible)

```go
cache, err := cache.NewCache(
    cache.WithStorageDriver(storageDriver),
)
```
