package server

import (
	"fmt"
	"strconv"

	"redis-go-clone/protocol"
)

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

	result, success := s.store.HIncrBy(key, field, int(increment))
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

	if len(fieldValuePairs)%2 != 0 {
		return protocol.EncodeError("wrong number of arguments for 'hmset' command")
	}

	fieldMap := make(map[string]string)
	for i := 0; i < len(fieldValuePairs); i += 2 {
		fieldMap[fieldValuePairs[i]] = fieldValuePairs[i+1]
	}

	if s.store.HMSet(key, fieldMap) {
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
	values := s.store.HMGet(key, fields...)

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
