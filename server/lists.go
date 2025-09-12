package server

import (
	"fmt"
	"strconv"

	"keyra/protocol"
)

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
