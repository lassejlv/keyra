package server

import (
	"fmt"
	"keyra/protocol"
	"sync"
)

type TransactionState int

const (
	NoTransaction TransactionState = iota
	InTransaction
)

type QueuedCommand struct {
	Command string
	Args    []string
}

type TransactionContext struct {
	State       TransactionState
	Queue       []QueuedCommand
	WatchedKeys map[string]interface{}
	mu          sync.RWMutex
}

func NewTransactionContext() *TransactionContext {
	return &TransactionContext{
		State:       NoTransaction,
		Queue:       make([]QueuedCommand, 0),
		WatchedKeys: make(map[string]interface{}),
	}
}

func (tc *TransactionContext) IsInTransaction() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.State == InTransaction
}

func (tc *TransactionContext) StartTransaction() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.State = InTransaction
	tc.Queue = make([]QueuedCommand, 0)
}

func (tc *TransactionContext) QueueCommand(command string, args []string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.State == InTransaction {
		tc.Queue = append(tc.Queue, QueuedCommand{
			Command: command,
			Args:    args,
		})
	}
}

func (tc *TransactionContext) GetQueuedCommands() []QueuedCommand {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	commands := make([]QueuedCommand, len(tc.Queue))
	copy(commands, tc.Queue)
	return commands
}

func (tc *TransactionContext) ClearTransaction() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.State = NoTransaction
	tc.Queue = make([]QueuedCommand, 0)
}

func (tc *TransactionContext) AddWatchedKey(key string, value interface{}) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.WatchedKeys[key] = value
}

func (tc *TransactionContext) ClearWatchedKeys() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.WatchedKeys = make(map[string]interface{})
}

func (tc *TransactionContext) CheckWatchedKeys(s *Server) bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	for key, expectedValue := range tc.WatchedKeys {
		currentValue, exists := s.store.GetRaw(key)
		if !exists && expectedValue != nil {
			return false
		}
		if exists && !compareValues(expectedValue, currentValue) {
			return false
		}
	}
	return true
}

func compareValues(expected, current interface{}) bool {
	if expected == nil && current == nil {
		return true
	}
	if expected == nil || current == nil {
		return false
	}
	return fmt.Sprintf("%v", expected) == fmt.Sprintf("%v", current)
}

func (s *Server) getTransactionContext(connKey string) *TransactionContext {
	if ctx, exists := s.transactionContexts.Load(connKey); exists {
		return ctx.(*TransactionContext)
	}
	
	newCtx := NewTransactionContext()
	s.transactionContexts.Store(connKey, newCtx)
	return newCtx
}

func (s *Server) handleMulti(args []string, connKey string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'multi' command")
	}
	
	txCtx := s.getTransactionContext(connKey)
	if txCtx.IsInTransaction() {
		return protocol.EncodeError("MULTI calls can not be nested")
	}
	
	txCtx.StartTransaction()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleExec(args []string, connKey string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'exec' command")
	}
	
	txCtx := s.getTransactionContext(connKey)
	if !txCtx.IsInTransaction() {
		return protocol.EncodeError("EXEC without MULTI")
	}
	
	defer txCtx.ClearTransaction()
	defer txCtx.ClearWatchedKeys()
	
	
	commands := txCtx.GetQueuedCommands()
	results := make([]string, len(commands))
	
	for i, cmd := range commands {
		result := s.executeCommandWithoutTransactionCheck(cmd.Command, cmd.Args, connKey)
		results[i] = result
	}
	
	response := fmt.Sprintf("*%d\r\n", len(results))
	for _, result := range results {
		response += result
	}
	
	return response
}

func (s *Server) handleDiscard(args []string, connKey string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'discard' command")
	}
	
	txCtx := s.getTransactionContext(connKey)
	if !txCtx.IsInTransaction() {
		return protocol.EncodeError("DISCARD without MULTI")
	}
	
	txCtx.ClearTransaction()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleWatch(args []string, connKey string) string {
	if len(args) == 0 {
		return protocol.EncodeError("wrong number of arguments for 'watch' command")
	}
	
	txCtx := s.getTransactionContext(connKey)
	if txCtx.IsInTransaction() {
		return protocol.EncodeError("WATCH inside MULTI is not allowed")
	}
	
	for _, key := range args {
		value, _ := s.store.GetRaw(key)
		txCtx.AddWatchedKey(key, value)
	}
	
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleUnwatch(args []string, connKey string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'unwatch' command")
	}
	
	txCtx := s.getTransactionContext(connKey)
	txCtx.ClearWatchedKeys()
	
	return protocol.EncodeSimpleString("OK")
}


func (s *Server) handleSetDirect(args []string) string {
	return s.handleSet(args)
}

func (s *Server) executeCommandWithoutTransactionCheck(command string, args []string, connKey string) string {
	switch command {
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
	case "SELECT":
		return s.handleSelect(args)
	case "MOVE":
		return s.handleMove(args)
	case "SWAPDB":
		return s.handleSwapDB(args)
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
	case "PUBLISH":
		return s.handlePublish(args)
	case "PUBSUB":
		return s.handlePubSub(args)
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown command '%s'", command))
	}
}
