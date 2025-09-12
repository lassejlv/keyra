package server

import (
	"fmt"
	"strings"

	"keyra/protocol"
)

func (s *Server) handleBGRewriteAOF(args []string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'bgrewriteaof' command")
	}

	if s.aof == nil || !s.aof.IsEnabled() {
		return protocol.EncodeError("AOF is not enabled")
	}

	// Start background rewrite
	go s.performAOFRewrite()

	return protocol.EncodeSimpleString("Background AOF rewrite started")
}

func (s *Server) performAOFRewrite() {
	stats, err := s.aof.Rewrite(s.getCurrentDatabaseState)
	if err != nil {
		fmt.Printf("AOF rewrite failed: %v\n", err)
		return
	}

	fmt.Printf("AOF rewrite completed: %d -> %d bytes in %v\n",
		stats.OriginalSize, stats.RewriteSize, stats.EndTime.Sub(stats.StartTime))
}

func (s *Server) getCurrentDatabaseState() ([][]string, error) {
	var commands [][]string

	// Get all keys from current database
	keys := s.store.Keys("*")

	for _, key := range keys {
		keyType := s.store.GetType(key)
		
		switch keyType.String() {
		case "string":
			if value, exists := s.store.Get(key); exists {
				commands = append(commands, []string{"SET", key, value})
			}
			
		case "list":
			if list := s.store.GetList(key); list != nil {
				for _, value := range list {
					commands = append(commands, []string{"RPUSH", key, value})
				}
			}
			
		case "set":
			if set := s.store.GetSet(key); set != nil {
				for member := range set {
					commands = append(commands, []string{"SADD", key, member})
				}
			}
			
		case "hash":
			if hash := s.store.GetHash(key); hash != nil {
				for field, value := range hash {
					commands = append(commands, []string{"HSET", key, field, value})
				}
			}
			
		case "zset":
			if zset := s.store.GetZSet(key); zset != nil {
				for _, member := range zset.Sorted {
					commands = append(commands, []string{"ZADD", key, 
						fmt.Sprintf("%g", member.Score), member.Member})
				}
			}
		}
		
		// Add expiration if key has TTL
		if ttl := s.store.GetTTL(key); ttl > 0 {
			commands = append(commands, []string{"PEXPIRE", key, 
				fmt.Sprintf("%d", ttl.Milliseconds())})
		}
	}

	return commands, nil
}

func (s *Server) logCommandToAOF(command string, args []string) {
	if s.aof == nil || !s.aof.IsEnabled() {
		return
	}

	if !s.aof.ShouldLogCommand(command) {
		return
	}

	fullCommand := make([]string, len(args)+1)
	fullCommand[0] = command
	copy(fullCommand[1:], args)

	s.aof.WriteCommand(fullCommand)
}

func (s *Server) executeCommandWithAOF(command string, args []string, connKey string) string {
	// Execute the command first
	result := s.executeCommandWithoutAOF(command, args, connKey)
	
	// Only log if command was successful (doesn't start with -ERR)
	if !strings.HasPrefix(result, "-ERR") {
		s.logCommandToAOF(command, args)
	}
	
	return result
}

func (s *Server) executeCommandWithoutAOF(command string, args []string, connKey string) string {
	// This method executes commands without AOF logging
	// Used during AOF loading and for read-only commands
	
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
	case "HSCAN":
		return s.handleHScan(args)
	case "HSTRLEN":
		return s.handleHStrLen(args)
	case "HRANDFIELD":
		return s.handleHRandField(args)
	
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
	case "BGREWRITEAOF":
		return s.handleBGRewriteAOF(args)
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
	case "CONFIG":
		return s.handleConfig(args)
	case "MONITOR":
		return s.handleMonitor(args, connKey, nil)
	case "SLOWLOG":
		return s.handleSlowlog(args)
	
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown command '%s'", command))
	}
}
