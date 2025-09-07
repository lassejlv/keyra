package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ConnectionPool struct {
	maxConnections int
	activeCount    int64
	idleTimeout    time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
	mu             sync.RWMutex
	connections    map[string]*ClientConnection
	server         *Server
}

type ClientConnection struct {
	conn         *TrackedConn
	id           string
	lastActivity time.Time
	ctx          context.Context
	cancel       context.CancelFunc
	writeMu      sync.Mutex
}

type TrackedConn struct {
	net.Conn
	server *Server
}

func NewTrackedConn(conn net.Conn, server *Server) *TrackedConn {
	return &TrackedConn{
		Conn:   conn,
		server: server,
	}
}

func (tc *TrackedConn) Read(b []byte) (n int, err error) {
	n, err = tc.Conn.Read(b)
	if n > 0 {
		tc.server.networkStats.AddBytesReceived(int64(n))
	}
	return n, err
}

func (tc *TrackedConn) Write(b []byte) (n int, err error) {
	n, err = tc.Conn.Write(b)
	if n > 0 {
		tc.server.networkStats.AddBytesSent(int64(n))
	}
	return n, err
}

type ConnectionConfig struct {
	MaxConnections int
	IdleTimeout    time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		MaxConnections: 10000,
		IdleTimeout:    300 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
}

func NewConnectionPool(server *Server, config ConnectionConfig) *ConnectionPool {
	pool := &ConnectionPool{
		maxConnections: config.MaxConnections,
		idleTimeout:    config.IdleTimeout,
		readTimeout:    config.ReadTimeout,
		writeTimeout:   config.WriteTimeout,
		connections:    make(map[string]*ClientConnection),
		server:         server,
	}
	
	go pool.cleanupIdleConnections()
	
	return pool
}

func (cp *ConnectionPool) AcceptConnection(conn net.Conn) (*ClientConnection, error) {
	currentCount := atomic.LoadInt64(&cp.activeCount)
	if currentCount >= int64(cp.maxConnections) {
		return nil, fmt.Errorf("connection limit reached: %d/%d", currentCount, cp.maxConnections)
	}
	
	connID := fmt.Sprintf("%p", conn)
	ctx, cancel := context.WithCancel(context.Background())
	
	trackedConn := NewTrackedConn(conn, cp.server)
	clientConn := &ClientConnection{
		conn:         trackedConn,
		id:           connID,
		lastActivity: time.Now(),
		ctx:          ctx,
		cancel:       cancel,
	}
	
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}
	
	cp.server.networkStats.AddConnection()
	
	cp.mu.Lock()
	cp.connections[connID] = clientConn
	cp.mu.Unlock()
	
	atomic.AddInt64(&cp.activeCount, 1)
	
	return clientConn, nil
}

func (cp *ConnectionPool) RemoveConnection(connID string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	if clientConn, exists := cp.connections[connID]; exists {
		clientConn.cancel()
		delete(cp.connections, connID)
		atomic.AddInt64(&cp.activeCount, -1)
	}
}

func (cp *ConnectionPool) UpdateActivity(connID string) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	if clientConn, exists := cp.connections[connID]; exists {
		clientConn.lastActivity = time.Now()
	}
}

func (cc *ClientConnection) WriteWithTimeout(data []byte, timeout time.Duration) error {
	cc.writeMu.Lock()
	defer cc.writeMu.Unlock()
	
	if timeout > 0 {
		cc.conn.SetWriteDeadline(time.Now().Add(timeout))
		defer cc.conn.SetWriteDeadline(time.Time{})
	}
	
	_, err := cc.conn.Write(data)
	return err
}

func (cc *ClientConnection) SetReadTimeout(timeout time.Duration) error {
	if timeout > 0 {
		return cc.conn.SetReadDeadline(time.Now().Add(timeout))
	}
	return cc.conn.SetReadDeadline(time.Time{})
}

func (cp *ConnectionPool) cleanupIdleConnections() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			cp.removeIdleConnections()
		case <-cp.server.shutdownSignal:
			return
		}
	}
}

func (cp *ConnectionPool) removeIdleConnections() {
	now := time.Now()
	var toRemove []string
	
	cp.mu.RLock()
	for connID, clientConn := range cp.connections {
		if now.Sub(clientConn.lastActivity) > cp.idleTimeout {
			toRemove = append(toRemove, connID)
		}
	}
	cp.mu.RUnlock()
	
	for _, connID := range toRemove {
		cp.mu.Lock()
		if clientConn, exists := cp.connections[connID]; exists {
			clientConn.conn.Close()
			clientConn.cancel()
			delete(cp.connections, connID)
			atomic.AddInt64(&cp.activeCount, -1)
		}
		cp.mu.Unlock()
	}
	
	if len(toRemove) > 0 {
		fmt.Printf("Cleaned up %d idle connections\n", len(toRemove))
	}
}

func (cp *ConnectionPool) GetStats() ConnectionStats {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	return ConnectionStats{
		ActiveConnections: int(atomic.LoadInt64(&cp.activeCount)),
		MaxConnections:    cp.maxConnections,
		TotalConnections:  len(cp.connections),
	}
}

type ConnectionStats struct {
	ActiveConnections int
	MaxConnections    int
	TotalConnections  int
}

func (cp *ConnectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	for connID, clientConn := range cp.connections {
		clientConn.conn.Close()
		clientConn.cancel()
		delete(cp.connections, connID)
	}
	
	atomic.StoreInt64(&cp.activeCount, 0)
}
