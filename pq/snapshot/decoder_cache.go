package snapshot

import (
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
)

// TypeDecoder wraps type decoding functionality
type TypeDecoder struct {
	oid uint32
}

// Decode decodes column data using the cached decoder
func (d *TypeDecoder) Decode(typeMap *pgtype.Map, data []byte) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(d.oid); ok {
		return dt.Codec.DecodeValue(typeMap, d.oid, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

// DecoderCache caches type decoders by OID to avoid repeated reflection lookups
// This eliminates the overhead of TypeForOID calls for every column in every row
type DecoderCache struct {
	cache map[uint32]*TypeDecoder
	mu    sync.RWMutex
}

// NewDecoderCache creates a new decoder cache
func NewDecoderCache() *DecoderCache {
	return &DecoderCache{
		cache: make(map[uint32]*TypeDecoder, 50), // Pre-allocate for common types
	}
}

// Get retrieves or creates a decoder for the given OID
func (c *DecoderCache) Get(oid uint32) *TypeDecoder {
	// Fast path: read lock for cache hit
	c.mu.RLock()
	decoder, exists := c.cache[oid]
	c.mu.RUnlock()

	if exists {
		return decoder
	}

	// Slow path: write lock for cache miss
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have added it)
	if decoder, exists := c.cache[oid]; exists {
		return decoder
	}

	// Create new decoder
	decoder = &TypeDecoder{oid: oid}
	c.cache[oid] = decoder
	return decoder
}

// Size returns the number of cached decoders
func (c *DecoderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}
