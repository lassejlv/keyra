package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
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
		log.Printf("Error saving data on shutdown: %v", err)
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
				log.Printf("Error during periodic save: %v", err)
			} else {
				log.Println("Periodic save completed")
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
	case "SET":
		return s.handleSet(args)
	case "GET":
		return s.handleGet(args)
	case "DEL":
		return s.handleDel(args)
	case "TYPE":
		return s.handleType(args)
	case "TTL":
		return s.handleTTL(args)
	case "EXPIRE":
		return s.handleExpire(args)
	case "EXPIREAT":
		return s.handleExpireAt(args)
	case "SAVE":
		return s.handleSave(args)
	case "BGSAVE":
		return s.handleBGSave(args)
	case "STRLEN":
		return s.handleStrLen(args)
	case "EXISTS":
		return s.handleExists(args)
	case "KEYS":
		return s.handleKeys(args)
	case "SCAN":
		return s.handleScan(args)
	case "DBSIZE":
		return s.handleDBSize(args)
	case "FLUSHDB":
		return s.handleFlushDB(args)
	case "FLUSHALL":
		return s.handleFlushAll(args)
	case "INFO":
		return s.handleInfo(args)
	case "PEXPIRE":
		return s.handlePExpire(args)
	case "PEXPIREAT":
		return s.handlePExpireAt(args)
	case "PTTL":
		return s.handlePTTL(args)
	case "APPEND":
		return s.handleAppend(args)
	case "GETRANGE":
		return s.handleGetRange(args)
	case "SUBSTR":
		return s.handleGetRange(args)
	case "CLIENT":
		return s.handleClient(args)
	case "SELECT":
		return s.handleSelect(args)
	case "MOVE":
		return s.handleMove(args)
	case "SWAPDB":
		return s.handleSwapDB(args)
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
	case "RANDOMKEY":
		return s.handleRandomKey(args)
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
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown command '%s'", command))
	}
}

func (s *Server) handleAuth(args []string, connKey string) string {
	if len(args) != 1 {
		return protocol.EncodeError("wrong number of arguments for 'auth' command")
	}

	if !s.requiresAuth() {
		return protocol.EncodeError("ERR Client sent AUTH, but no password is set")
	}

	providedPassword := args[0]
	if providedPassword == s.password {
		s.authenticate(connKey)
		return protocol.EncodeSimpleString("OK")
	}

	return protocol.EncodeError("ERR invalid password")
}

func (s *Server) handleSet(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'set' command")
	}

	key, value := args[0], args[1]
	
	if len(args) > 2 {
		for i := 2; i < len(args); i += 2 {
			if i+1 >= len(args) {
				return protocol.EncodeError("syntax error")
			}
			
			option := strings.ToUpper(args[i])
			optionValue := args[i+1]
			
			switch option {
			case "EX":
				seconds, err := strconv.Atoi(optionValue)
				if err != nil || seconds <= 0 {
					return protocol.EncodeError("invalid expire time in set")
				}
				expiration := time.Now().Add(time.Duration(seconds) * time.Second)
				s.store.SetWithExpiration(key, value, expiration)
				return protocol.EncodeSimpleString("OK")
			case "PX":
				milliseconds, err := strconv.Atoi(optionValue)
				if err != nil || milliseconds <= 0 {
					return protocol.EncodeError("invalid expire time in set")
				}
				expiration := time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
				s.store.SetWithExpiration(key, value, expiration)
				return protocol.EncodeSimpleString("OK")
			default:
				return protocol.EncodeError("syntax error")
			}
		}
	}
	
	s.store.Set(key, value)
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleGet(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'get' command")
	}

	key := args[0]
	value, exists := s.store.Get(key)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(value)
}

func (s *Server) handleDel(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'del' command")
	}

	count := 0
	for _, key := range args {
		if s.store.Del(key) {
			count++
		}
	}
	return protocol.EncodeInteger(count)
}

func (s *Server) handlePing(args []string) string {
	if len(args) == 0 {
		return protocol.EncodeSimpleString("PONG")
	}
	return protocol.EncodeBulkString(args[0])
}

func (s *Server) handleType(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'type' command")
	}

	key := args[0]
	dataType := s.store.GetType(key)
	if dataType == -1 {
		return protocol.EncodeSimpleString("none")
	}
	return protocol.EncodeSimpleString(dataType.String())
}

func (s *Server) handleTTL(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'ttl' command")
	}

	key := args[0]
	ttl := s.store.TTL(key)
	return protocol.EncodeInteger(ttl)
}

func (s *Server) handleExpire(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'expire' command")
	}

	key := args[0]
	seconds, err := strconv.Atoi(args[1])
	if err != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	success := s.store.Expire(key, seconds)
	if success {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handleExpireAt(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'expireat' command")
	}

	key := args[0]
	timestamp, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	success := s.store.ExpireAt(key, timestamp)
	if success {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handleSave(args []string) string {
	if len(args) > 0 {
		return protocol.EncodeError("wrong number of arguments for 'save' command")
	}

	err := s.store.Save()
	if err != nil {
		return protocol.EncodeError("save failed")
	}
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleBGSave(args []string) string {
	if len(args) > 0 {
		return protocol.EncodeError("wrong number of arguments for 'bgsave' command")
	}

	go func() {
		if err := s.store.Save(); err != nil {
			log.Printf("Background save failed: %v", err)
		} else {
			log.Println("Background save completed")
		}
	}()

	return protocol.EncodeSimpleString("Background saving started")
}

func (s *Server) handleStrLen(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'strlen' command")
	}

	key := args[0]
	value, exists := s.store.Get(key)
	if !exists {
		return protocol.EncodeInteger(0)
	}
	return protocol.EncodeInteger(len(value))
}

func (s *Server) handleExists(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'exists' command")
	}

	count := 0
	for _, key := range args {
		if s.store.Exists(key) {
			count++
		}
	}
	return protocol.EncodeInteger(count)
}

func (s *Server) handleKeys(args []string) string {
	pattern := "*"
	if len(args) > 0 {
		pattern = args[0]
	}

	keys := s.store.Keys(pattern)
	
	result := fmt.Sprintf("*%d\r\n", len(keys))
	for _, key := range keys {
		result += protocol.EncodeBulkString(key)
	}
	return result
}

func (s *Server) handleScan(args []string) string {
	cursor := 0
	pattern := "*"
	count := 10

	if len(args) > 0 {
		if c, err := strconv.Atoi(args[0]); err == nil {
			cursor = c
		}
	}

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			pattern = args[i+1]
		case "COUNT":
			if c, err := strconv.Atoi(args[i+1]); err == nil && c > 0 {
				count = c
			}
		}
	}

	allKeys := s.store.Keys(pattern)
	
	start := cursor
	end := cursor + count
	if end > len(allKeys) {
		end = len(allKeys)
	}
	
	var keys []string
	if start < len(allKeys) {
		keys = allKeys[start:end]
	}
	
	nextCursor := 0
	if end < len(allKeys) {
		nextCursor = end
	}

	result := "*2\r\n"
	result += protocol.EncodeBulkString(strconv.Itoa(nextCursor))
	
	result += fmt.Sprintf("*%d\r\n", len(keys))
	for _, key := range keys {
		result += protocol.EncodeBulkString(key)
	}
	
	return result
}

func (s *Server) handleDBSize(args []string) string {
	if len(args) > 0 {
		return protocol.EncodeError("wrong number of arguments for 'dbsize' command")
	}

	size := s.store.DBSize()
	return protocol.EncodeInteger(size)
}

func (s *Server) handleFlushDB(args []string) string {
	s.store.FlushDB()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleFlushAll(args []string) string {
	s.store.FlushDB()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleInfo(args []string) string {
	section := "default"
	if len(args) > 0 {
		section = strings.ToLower(args[0])
	}

	var info strings.Builder
	
	if section == "default" || section == "server" {
		info.WriteString("# Server\r\n")
		info.WriteString("redis_version:7.0.0-compatible\r\n")
		info.WriteString("redis_mode:standalone\r\n")
		info.WriteString("arch_bits:64\r\n")
		info.WriteString("server_time_usec:" + strconv.FormatInt(time.Now().UnixMicro(), 10) + "\r\n")
		uptime := int(time.Since(s.startTime).Seconds())
		info.WriteString("uptime_in_seconds:" + strconv.Itoa(uptime) + "\r\n")
		info.WriteString("\r\n")
	}
	
	if section == "default" || section == "clients" {
		info.WriteString("# Clients\r\n")
		clientCount := s.getClientCount()
		info.WriteString("connected_clients:" + strconv.Itoa(clientCount) + "\r\n")
		info.WriteString("client_longest_output_list:0\r\n")
		info.WriteString("client_biggest_input_buf:0\r\n")
		info.WriteString("blocked_clients:0\r\n")
		info.WriteString("\r\n")
	}
	
	if section == "default" || section == "keyspace" {
		info.WriteString("# Keyspace\r\n")
		
		// Show info for all databases with keys
		dbInfo := s.store.GetDBInfo()
		for dbIndex, keyCount := range dbInfo {
			info.WriteString(fmt.Sprintf("db%d:keys=%d,expires=0,avg_ttl=0\r\n", dbIndex, keyCount))
		}
		
		info.WriteString("\r\n")
	}
	
	if section == "default" || section == "memory" {
		info.WriteString("# Memory\r\n")
		info.WriteString("used_memory:1048576\r\n")
		info.WriteString("used_memory_human:1.00M\r\n")
		info.WriteString("used_memory_peak:1048576\r\n")
		info.WriteString("used_memory_peak_human:1.00M\r\n")
		info.WriteString("\r\n")
	}

	return protocol.EncodeBulkString(info.String())
}

func (s *Server) handlePExpire(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'pexpire' command")
	}

	key := args[0]
	milliseconds, err := strconv.Atoi(args[1])
	if err != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	success := s.store.PExpire(key, milliseconds)
	if success {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handlePExpireAt(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'pexpireat' command")
	}

	key := args[0]
	timestampMs, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	success := s.store.PExpireAt(key, timestampMs)
	if success {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handlePTTL(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'pttl' command")
	}

	key := args[0]
	pttl := s.store.PTTL(key)
	return protocol.EncodeInteger(pttl)
}

func (s *Server) handleAppend(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'append' command")
	}

	key, value := args[0], args[1]
	length := s.store.Append(key, value)
	return protocol.EncodeInteger(length)
}

func (s *Server) handleGetRange(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'getrange' command")
	}

	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	end, err2 := strconv.Atoi(args[2])
	
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	result := s.store.GetRange(key, start, end)
	return protocol.EncodeBulkString(result)
}

func (s *Server) handleClient(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'client' command")
	}

	subcommand := strings.ToUpper(args[0])
	switch subcommand {
	case "LIST":
		clientCount := s.getClientCount()
		clientInfo := fmt.Sprintf("id=1 addr=127.0.0.1:6379 fd=7 name= age=%d idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\n", 
			int(time.Since(s.startTime).Seconds()))
		
		if clientCount > 1 {
			for i := 2; i <= clientCount; i++ {
				clientInfo += fmt.Sprintf("id=%d addr=127.0.0.1:6379 fd=%d name= age=%d idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=null\n", 
					i, i+5, int(time.Since(s.startTime).Seconds()))
			}
		}
		
		return protocol.EncodeBulkString(clientInfo)
	case "SETNAME":
		return protocol.EncodeSimpleString("OK")
	case "GETNAME":
		return protocol.EncodeBulkString("")
	default:
		return protocol.EncodeError("unknown client subcommand '" + subcommand + "'")
	}
}

// Database management commands
func (s *Server) handleSelect(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'select' command")
	}

	dbIndex, err := strconv.Atoi(args[0])
	if err != nil {
		return protocol.EncodeError("invalid DB index")
	}

	if !s.store.SelectDB(dbIndex) {
		return protocol.EncodeError("invalid DB index")
	}

	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleMove(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'move' command")
	}

	key := args[0]
	destDB, err := strconv.Atoi(args[1])
	if err != nil {
		return protocol.EncodeError("invalid DB index")
	}

	if s.store.Move(key, destDB) {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handleSwapDB(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'swapdb' command")
	}

	db1, err1 := strconv.Atoi(args[0])
	db2, err2 := strconv.Atoi(args[1])
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("invalid DB index")
	}

	if s.store.SwapDB(db1, db2) {
		return protocol.EncodeSimpleString("OK")
	}
	return protocol.EncodeError("invalid DB index")
}

// List commands
func (s *Server) handleLPush(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'lpush' command")
	}

	key := args[0]
	values := args[1:]
	length := s.store.LPush(key, values...)
	if length == -1 {
		return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return protocol.EncodeInteger(length)
}

func (s *Server) handleRPush(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'rpush' command")
	}

	key := args[0]
	values := args[1:]
	length := s.store.RPush(key, values...)
	if length == -1 {
		return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return protocol.EncodeInteger(length)
}

func (s *Server) handleLPop(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'lpop' command")
	}

	key := args[0]
	value, exists := s.store.LPop(key)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(value)
}

func (s *Server) handleRPop(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'rpop' command")
	}

	key := args[0]
	value, exists := s.store.RPop(key)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(value)
}

func (s *Server) handleLLen(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'llen' command")
	}

	key := args[0]
	length := s.store.LLen(key)
	return protocol.EncodeInteger(length)
}

func (s *Server) handleLRange(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'lrange' command")
	}

	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	values := s.store.LRange(key, start, stop)
	result := fmt.Sprintf("*%d\r\n", len(values))
	for _, value := range values {
		result += protocol.EncodeBulkString(value)
	}
	return result
}

func (s *Server) handleLIndex(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'lindex' command")
	}

	key := args[0]
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	value, exists := s.store.LIndex(key, index)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(value)
}

func (s *Server) handleLSet(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'lset' command")
	}

	key := args[0]
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}
	element := args[2]

	if s.store.LSet(key, index, element) {
		return protocol.EncodeSimpleString("OK")
	}
	return protocol.EncodeError("ERR no such key")
}

func (s *Server) handleLTrim(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'ltrim' command")
	}

	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	s.store.LTrim(key, start, stop)
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleLInsert(args []string) string {
	if len(args) < 4 {
		return protocol.EncodeError("wrong number of arguments for 'linsert' command")
	}

	key := args[0]
	where := args[1]
	pivot := args[2]
	element := args[3]

	result := s.store.LInsert(key, where, pivot, element)
	return protocol.EncodeInteger(result)
}

func (s *Server) handleBLPop(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'blpop' command")
	}

	// Extract timeout (last argument)
	timeout, err := strconv.Atoi(args[len(args)-1])
	if err != nil {
		return protocol.EncodeError("timeout is not an integer or out of range")
	}

	// Extract keys (all but last argument)
	keys := args[:len(args)-1]

	resultKey, resultValue, exists := s.store.BLPop(keys, timeout)
	if !exists {
		return protocol.EncodeBulkString("")
	}

	// Return array with key and value
	result := "*2\r\n"
	result += protocol.EncodeBulkString(resultKey)
	result += protocol.EncodeBulkString(resultValue)
	return result
}

func (s *Server) handleBRPop(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'brpop' command")
	}

	// Extract timeout (last argument)
	timeout, err := strconv.Atoi(args[len(args)-1])
	if err != nil {
		return protocol.EncodeError("timeout is not an integer or out of range")
	}

	// Extract keys (all but last argument)
	keys := args[:len(args)-1]

	resultKey, resultValue, exists := s.store.BRPop(keys, timeout)
	if !exists {
		return protocol.EncodeBulkString("")
	}

	// Return array with key and value
	result := "*2\r\n"
	result += protocol.EncodeBulkString(resultKey)
	result += protocol.EncodeBulkString(resultValue)
	return result
}

// Hash commands
func (s *Server) handleHSet(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'hset' command")
	}

	key := args[0]
	field := args[1]
	value := args[2]
	
	if s.store.HSet(key, field, value) {
		return protocol.EncodeInteger(1) // New field
	}
	return protocol.EncodeInteger(0) // Updated existing field
}

func (s *Server) handleHGet(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'hget' command")
	}

	key := args[0]
	field := args[1]
	value, exists := s.store.HGet(key, field)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(value)
}

func (s *Server) handleHDel(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'hdel' command")
	}

	key := args[0]
	fields := args[1:]
	count := s.store.HDel(key, fields...)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleHExists(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'hexists' command")
	}

	key := args[0]
	field := args[1]
	if s.store.HExists(key, field) {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handleHLen(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'hlen' command")
	}

	key := args[0]
	length := s.store.HLen(key)
	return protocol.EncodeInteger(length)
}

func (s *Server) handleHKeys(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'hkeys' command")
	}

	key := args[0]
	keys := s.store.HKeys(key)
	result := fmt.Sprintf("*%d\r\n", len(keys))
	for _, k := range keys {
		result += protocol.EncodeBulkString(k)
	}
	return result
}

func (s *Server) handleHVals(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'hvals' command")
	}

	key := args[0]
	vals := s.store.HVals(key)
	result := fmt.Sprintf("*%d\r\n", len(vals))
	for _, v := range vals {
		result += protocol.EncodeBulkString(v)
	}
	return result
}

func (s *Server) handleHGetAll(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'hgetall' command")
	}

	key := args[0]
	hash := s.store.HGetAll(key)
	result := fmt.Sprintf("*%d\r\n", len(hash)*2)
	for k, v := range hash {
		result += protocol.EncodeBulkString(k)
		result += protocol.EncodeBulkString(v)
	}
	return result
}

func (s *Server) handleHIncrBy(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'hincrby' command")
	}

	key := args[0]
	field := args[1]
	increment, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	result, success := s.store.HIncrBy(key, field, increment)
	if !success {
		return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return protocol.EncodeInteger(int(result))
}

func (s *Server) handleHIncrByFloat(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'hincrbyfloat' command")
	}

	key := args[0]
	field := args[1]
	increment, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return protocol.EncodeError("value is not a valid float")
	}

	result, success := s.store.HIncrByFloat(key, field, increment)
	if !success {
		return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return protocol.EncodeBulkString(strconv.FormatFloat(result, 'g', -1, 64))
}

func (s *Server) handleHMSet(args []string) string {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.EncodeError("wrong number of arguments for 'hmset' command")
	}

	key := args[0]
	fieldValuePairs := args[1:]

	if s.store.HMSet(key, fieldValuePairs) {
		return protocol.EncodeSimpleString("OK")
	}
	return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
}

func (s *Server) handleHMGet(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'hmget' command")
	}

	key := args[0]
	fields := args[1:]
	values := s.store.HMGet(key, fields)

	result := fmt.Sprintf("*%d\r\n", len(values))
	for _, value := range values {
		if value == "" {
			result += "$-1\r\n" // Null bulk string
		} else {
			result += protocol.EncodeBulkString(value)
		}
	}
	return result
}

func (s *Server) handleHSetNX(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'hsetnx' command")
	}

	key := args[0]
	field := args[1]
	value := args[2]

	if s.store.HSetNX(key, field, value) {
		return protocol.EncodeInteger(1) // Field was set
	}
	return protocol.EncodeInteger(0) // Field already existed
}

// Set commands
func (s *Server) handleSAdd(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'sadd' command")
	}

	key := args[0]
	members := args[1:]
	count := s.store.SAdd(key, members...)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSRem(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'srem' command")
	}

	key := args[0]
	members := args[1:]
	count := s.store.SRem(key, members...)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSIsMember(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'sismember' command")
	}

	key := args[0]
	member := args[1]
	if s.store.SIsMember(key, member) {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handleSMembers(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'smembers' command")
	}

	key := args[0]
	members := s.store.SMembers(key)
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
}

func (s *Server) handleSCard(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'scard' command")
	}

	key := args[0]
	count := s.store.SCard(key)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSPop(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'spop' command")
	}

	key := args[0]
	member, exists := s.store.SPop(key)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(member)
}

func (s *Server) handleSRandMember(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'srandmember' command")
	}

	key := args[0]
	member, exists := s.store.SRandMember(key)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(member)
}

func (s *Server) handleSInter(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'sinter' command")
	}

	members := s.store.SInter(args)
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
}

func (s *Server) handleSUnion(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'sunion' command")
	}

	members := s.store.SUnion(args)
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
}

func (s *Server) handleSDiff(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'sdiff' command")
	}

	members := s.store.SDiff(args)
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
}

func (s *Server) handleSInterStore(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'sinterstore' command")
	}

	destination := args[0]
	keys := args[1:]
	count := s.store.SInterStore(destination, keys)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSUnionStore(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'sunionstore' command")
	}

	destination := args[0]
	keys := args[1:]
	count := s.store.SUnionStore(destination, keys)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSDiffStore(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'sdiffstore' command")
	}

	destination := args[0]
	keys := args[1:]
	count := s.store.SDiffStore(destination, keys)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSMove(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'smove' command")
	}

	source := args[0]
	destination := args[1]
	member := args[2]

	if s.store.SMove(source, destination, member) {
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handleRandomKey(args []string) string {
	if len(args) > 0 {
		return protocol.EncodeError("wrong number of arguments for 'randomkey' command")
	}

	key := s.store.RandomKey()
	if key == "" {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(key)
}

// Sorted Set commands
func (s *Server) handleZAdd(args []string) string {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.EncodeError("wrong number of arguments for 'zadd' command")
	}

	key := args[0]
	scoreMembers := args[1:]
	
	added := s.store.ZAdd(key, scoreMembers)
	if added == -1 {
		return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return protocol.EncodeInteger(added)
}

func (s *Server) handleZRem(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'zrem' command")
	}

	key := args[0]
	members := args[1:]
	count := s.store.ZRem(key, members)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleZRange(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'zrange' command")
	}

	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES" {
		withScores = true
	}

	members := s.store.ZRange(key, start, stop, false)
	
	if withScores {
		result := fmt.Sprintf("*%d\r\n", len(members)*2)
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
			result += protocol.EncodeBulkString(strconv.FormatFloat(member.Score, 'g', -1, 64))
		}
		return result
	} else {
		result := fmt.Sprintf("*%d\r\n", len(members))
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
		}
		return result
	}
}

func (s *Server) handleZRevRange(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'zrevrange' command")
	}

	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("value is not an integer or out of range")
	}

	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES" {
		withScores = true
	}

	members := s.store.ZRange(key, start, stop, true)
	
	if withScores {
		result := fmt.Sprintf("*%d\r\n", len(members)*2)
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
			result += protocol.EncodeBulkString(strconv.FormatFloat(member.Score, 'g', -1, 64))
		}
		return result
	} else {
		result := fmt.Sprintf("*%d\r\n", len(members))
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
		}
		return result
	}
}

func (s *Server) handleZRangeByScore(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'zrangebyscore' command")
	}

	key := args[0]
	min, err1 := strconv.ParseFloat(args[1], 64)
	max, err2 := strconv.ParseFloat(args[2], 64)
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("min or max is not a float")
	}

	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES" {
		withScores = true
	}

	members := s.store.ZRangeByScore(key, min, max, false)
	
	if withScores {
		result := fmt.Sprintf("*%d\r\n", len(members)*2)
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
			result += protocol.EncodeBulkString(strconv.FormatFloat(member.Score, 'g', -1, 64))
		}
		return result
	} else {
		result := fmt.Sprintf("*%d\r\n", len(members))
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
		}
		return result
	}
}

func (s *Server) handleZRevRangeByScore(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'zrevrangebyscore' command")
	}

	key := args[0]
	max, err1 := strconv.ParseFloat(args[1], 64)
	min, err2 := strconv.ParseFloat(args[2], 64)
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("min or max is not a float")
	}

	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES" {
		withScores = true
	}

	members := s.store.ZRangeByScore(key, min, max, true)
	
	if withScores {
		result := fmt.Sprintf("*%d\r\n", len(members)*2)
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
			result += protocol.EncodeBulkString(strconv.FormatFloat(member.Score, 'g', -1, 64))
		}
		return result
	} else {
		result := fmt.Sprintf("*%d\r\n", len(members))
		for _, member := range members {
			result += protocol.EncodeBulkString(member.Member)
		}
		return result
	}
}

func (s *Server) handleZRank(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'zrank' command")
	}

	key := args[0]
	member := args[1]
	rank := s.store.ZRank(key, member, false)
	if rank == -1 {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeInteger(rank)
}

func (s *Server) handleZRevRank(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'zrevrank' command")
	}

	key := args[0]
	member := args[1]
	rank := s.store.ZRank(key, member, true)
	if rank == -1 {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeInteger(rank)
}

func (s *Server) handleZScore(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'zscore' command")
	}

	key := args[0]
	member := args[1]
	score, exists := s.store.ZScore(key, member)
	if !exists {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(strconv.FormatFloat(score, 'g', -1, 64))
}

func (s *Server) handleZCard(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'zcard' command")
	}

	key := args[0]
	count := s.store.ZCard(key)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleZCount(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'zcount' command")
	}

	key := args[0]
	min, err1 := strconv.ParseFloat(args[1], 64)
	max, err2 := strconv.ParseFloat(args[2], 64)
	if err1 != nil || err2 != nil {
		return protocol.EncodeError("min or max is not a float")
	}

	count := s.store.ZCount(key, min, max)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleZIncrBy(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("wrong number of arguments for 'zincrby' command")
	}

	key := args[0]
	increment, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return protocol.EncodeError("value is not a valid float")
	}
	member := args[2]

	newScore, success := s.store.ZIncrBy(key, member, increment)
	if !success {
		return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return protocol.EncodeBulkString(strconv.FormatFloat(newScore, 'g', -1, 64))
}
