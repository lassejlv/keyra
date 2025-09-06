package server

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"redis-go-clone/persistence"
	"redis-go-clone/protocol"
	"redis-go-clone/store"
)

type Server struct {
	address           string
	store             *store.Store
	saveInterval      time.Duration
	shutdownSignal    chan os.Signal
	startTime         time.Time
	clientCount       int
	clientMutex       sync.RWMutex
	password          string
	authenticatedConns sync.Map
}

func New(address string) *Server {
	storagePath := persistence.GetStoragePath()
	fmt.Printf("Using storage path: %s\n", storagePath)
	
	saveInterval := 30 * time.Second
	if envInterval := os.Getenv("REDIS_SAVE_INTERVAL"); envInterval != "" {
		if duration, err := time.ParseDuration(envInterval); err == nil {
			saveInterval = duration
		}
	}
	
	password := os.Getenv("REDIS_PASSWORD")
	if password != "" {
		fmt.Println("Password authentication enabled")
	} else {
		fmt.Println("WARN! Password authentication disabled")
	}
	
	return &Server{
		address:        address,
		store:          store.New(storagePath),
		saveInterval:   saveInterval,
		shutdownSignal: make(chan os.Signal, 1),
		startTime:      time.Now(),
		password:       password,
	}
}

func NewInMemory(address string) *Server {
	password := os.Getenv("REDIS_PASSWORD")
	if password != "" {
		fmt.Println("Password authentication enabled")
	}
	
	return &Server{
		address:        address,
		store:          store.NewInMemory(),
		shutdownSignal: make(chan os.Signal, 1),
		startTime:      time.Now(),
		password:       password,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer listener.Close()

	signal.Notify(s.shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Redis server listening on %s\n", s.address)

	if s.saveInterval > 0 {
		go s.periodicSave()
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go s.handleConnection(conn)
		}
	}()

	<-s.shutdownSignal
	fmt.Println("\nShutting down server...")
	
	if err := s.store.Save(); err != nil {
		fmt.Printf("Error saving data on shutdown: %v", err)
	} else {
		fmt.Println("Data saved successfully")
	}
	
	return nil
}

func (s *Server) periodicSave() {
	ticker := time.NewTicker(s.saveInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := s.store.Save(); err != nil {
				fmt.Printf("Error during periodic save: %v", err)
			}
		case <-s.shutdownSignal:
			return
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	s.addClient()
	connKey := fmt.Sprintf("%p", conn)
	
	defer func() {
		s.removeClient()
		s.authenticatedConns.Delete(connKey)
		conn.Close()
	}()

	parser := protocol.NewParser(conn)

	for {
		args, err := parser.Parse()
		if err != nil {
			return
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])
		response := s.executeCommand(command, args[1:], connKey)
		conn.Write([]byte(response))
		
		// Close connection after QUIT command
		if command == "QUIT" {
			return
		}
	}
}

func (s *Server) addClient() {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()
	s.clientCount++
}

func (s *Server) removeClient() {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()
	s.clientCount--
}

func (s *Server) getClientCount() int {
	s.clientMutex.RLock()
	defer s.clientMutex.RUnlock()
	return s.clientCount
}

func (s *Server) requiresAuth() bool {
	return s.password != ""
}

func (s *Server) isAuthenticated(connKey string) bool {
	if !s.requiresAuth() {
		return true
	}
	_, authenticated := s.authenticatedConns.Load(connKey)
	return authenticated
}

func (s *Server) authenticate(connKey string) {
	s.authenticatedConns.Store(connKey, true)
}

func (s *Server) executeCommand(command string, args []string, connKey string) string {
	// Commands that don't require authentication
	switch command {
	case "AUTH":
		return s.handleAuth(args, connKey)
	case "PING":
		return s.handlePing(args)
	}
	
	// Check authentication for all other commands
	if !s.isAuthenticated(connKey) {
		return protocol.EncodeError("NOAUTH Authentication required")
	}
	
	switch command {
	// String commands
	case "SET":
		return s.handleSet(args)
	case "GET":
		return s.handleGet(args)
	case "APPEND":
		return s.handleAppend(args)
	case "GETRANGE":
		return s.handleGetRange(args)
	case "SUBSTR":
		return s.handleGetRange(args)
	case "STRLEN":
		return s.handleStrLen(args)
	
	// Key management commands
	case "DEL":
		return s.handleDel(args)
	case "EXISTS":
		return s.handleExists(args)
	case "KEYS":
		return s.handleKeys(args)
	case "SCAN":
		return s.handleScan(args)
	case "TYPE":
		return s.handleType(args)
	case "TTL":
		return s.handleTTL(args)
	case "EXPIRE":
		return s.handleExpire(args)
	case "EXPIREAT":
		return s.handleExpireAt(args)
	case "PEXPIRE":
		return s.handlePExpire(args)
	case "PEXPIREAT":
		return s.handlePExpireAt(args)
	case "PTTL":
		return s.handlePTTL(args)
	case "RANDOMKEY":
		return s.handleRandomKey(args)
	
	// List commands
	case "LPUSH":
		return s.handleLPush(args)
	case "RPUSH":
		return s.handleRPush(args)
	case "LPOP":
		return s.handleLPop(args)
	case "RPOP":
		return s.handleRPop(args)
	case "LLEN":
		return s.handleLLen(args)
	case "LRANGE":
		return s.handleLRange(args)
	case "LINDEX":
		return s.handleLIndex(args)
	case "LSET":
		return s.handleLSet(args)
	case "LTRIM":
		return s.handleLTrim(args)
	case "LINSERT":
		return s.handleLInsert(args)
	case "BLPOP":
		return s.handleBLPop(args)
	case "BRPOP":
		return s.handleBRPop(args)
	
	// Hash commands
	case "HSET":
		return s.handleHSet(args)
	case "HGET":
		return s.handleHGet(args)
	case "HDEL":
		return s.handleHDel(args)
	case "HEXISTS":
		return s.handleHExists(args)
	case "HLEN":
		return s.handleHLen(args)
	case "HKEYS":
		return s.handleHKeys(args)
	case "HVALS":
		return s.handleHVals(args)
	case "HGETALL":
		return s.handleHGetAll(args)
	case "HINCRBY":
		return s.handleHIncrBy(args)
	case "HINCRBYFLOAT":
		return s.handleHIncrByFloat(args)
	case "HMSET":
		return s.handleHMSet(args)
	case "HMGET":
		return s.handleHMGet(args)
	case "HSETNX":
		return s.handleHSetNX(args)
	
	// Set commands
	case "SADD":
		return s.handleSAdd(args)
	case "SREM":
		return s.handleSRem(args)
	case "SISMEMBER":
		return s.handleSIsMember(args)
	case "SMEMBERS":
		return s.handleSMembers(args)
	case "SCARD":
		return s.handleSCard(args)
	case "SPOP":
		return s.handleSPop(args)
	case "SRANDMEMBER":
		return s.handleSRandMember(args)
	case "SINTER":
		return s.handleSInter(args)
	case "SUNION":
		return s.handleSUnion(args)
	case "SDIFF":
		return s.handleSDiff(args)
	case "SINTERSTORE":
		return s.handleSInterStore(args)
	case "SUNIONSTORE":
		return s.handleSUnionStore(args)
	case "SDIFFSTORE":
		return s.handleSDiffStore(args)
	case "SMOVE":
		return s.handleSMove(args)
	
	// Sorted Set commands
	case "ZADD":
		return s.handleZAdd(args)
	case "ZREM":
		return s.handleZRem(args)
	case "ZRANGE":
		return s.handleZRange(args)
	case "ZREVRANGE":
		return s.handleZRevRange(args)
	case "ZRANGEBYSCORE":
		return s.handleZRangeByScore(args)
	case "ZREVRANGEBYSCORE":
		return s.handleZRevRangeByScore(args)
	case "ZRANK":
		return s.handleZRank(args)
	case "ZREVRANK":
		return s.handleZRevRank(args)
	case "ZSCORE":
		return s.handleZScore(args)
	case "ZCARD":
		return s.handleZCard(args)
	case "ZCOUNT":
		return s.handleZCount(args)
	case "ZINCRBY":
		return s.handleZIncrBy(args)
	
	// Database management commands
	case "SELECT":
		return s.handleSelect(args)
	case "MOVE":
		return s.handleMove(args)
	case "SWAPDB":
		return s.handleSwapDB(args)
	
	// Server administration commands
	case "HELLO":
		return s.handleHello(args)
	case "QUIT":
		return s.handleQuit(args)
	case "SAVE":
		return s.handleSave(args)
	case "BGSAVE":
		return s.handleBGSave(args)
	case "DBSIZE":
		return s.handleDBSize(args)
	case "FLUSHDB":
		return s.handleFlushDB(args)
	case "FLUSHALL":
		return s.handleFlushAll(args)
	case "INFO":
		return s.handleInfo(args)
	case "CLIENT":
		return s.handleClient(args)
	
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown command '%s'", command))
	}
}
