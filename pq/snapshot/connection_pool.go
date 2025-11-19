package snapshot

import (
	"context"
	"sync"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// ConnectionPool manages a pool of database connections for efficient reuse
// Eliminates the overhead of creating/destroying connections for each chunk
type ConnectionPool struct {
	dsn   string
	pool  chan pq.Connection
	mu    sync.Mutex
	conns []pq.Connection
}

// NewConnectionPool creates a new connection pool with the specified size
func NewConnectionPool(ctx context.Context, dsn string, size int) (*ConnectionPool, error) {
	if size <= 0 {
		size = 10 // Default pool size
	}

	p := &ConnectionPool{
		dsn:   dsn,
		pool:  make(chan pq.Connection, size),
		conns: make([]pq.Connection, 0, size),
	}

	// Pre-create connections
	logger.Info("[snapshot-connection-pool] creating connection pool", "size", size)
	for i := 0; i < size; i++ {
		conn, err := pq.NewConnection(ctx, dsn)
		if err != nil {
			// Cleanup already created connections
			p.Close(ctx)
			return nil, errors.Wrap(err, "create pool connection")
		}
		p.conns = append(p.conns, conn)
		p.pool <- conn
	}

	logger.Info("[snapshot-connection-pool] connection pool ready", "size", size)
	return p, nil
}

// Get retrieves a connection from the pool (blocks if pool is empty)
func (p *ConnectionPool) Get(ctx context.Context) (pq.Connection, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put returns a connection to the pool
func (p *ConnectionPool) Put(conn pq.Connection) {
	select {
	case p.pool <- conn:
		// Connection returned to pool
	default:
		// Pool is full (shouldn't happen with proper usage)
		logger.Warn("[snapshot-connection-pool] pool is full, connection not returned")
	}
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logger.Info("[snapshot-connection-pool] closing all connections", "count", len(p.conns))

	for _, conn := range p.conns {
		if err := conn.Close(ctx); err != nil {
			logger.Warn("[snapshot-connection-pool] error closing connection", "error", err)
		}
	}

	close(p.pool)
	p.conns = nil
	logger.Info("[snapshot-connection-pool] connection pool closed")
}
