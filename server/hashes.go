package server

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"redis-go-clone/protocol"
)

// Hash commands
func (s *Server) handleHSet(args []string) string {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.EncodeError("wrong number of arguments for 'hset' command")
	}

	key := args[0]
	fieldValuePairs := args[1:]
	
	if len(fieldValuePairs)%2 != 0 {
		return protocol.EncodeError("wrong number of arguments for 'hset' command")
	}

	if len(fieldValuePairs) == 2 {
		field := fieldValuePairs[0]
		value := fieldValuePairs[1]
		
		if s.store.HSet(key, field, value) {
			return protocol.EncodeInteger(1)
		}
		return protocol.EncodeInteger(0)
	}

	existingHash := s.store.HGetAll(key)
	newFields := 0
	fieldMap := make(map[string]string)
	
	for i := 0; i < len(fieldValuePairs); i += 2 {
		field := fieldValuePairs[i]
		value := fieldValuePairs[i+1]
		fieldMap[field] = value
		
		if _, exists := existingHash[field]; !exists {
			newFields++
		}
	}
	
	if !s.store.HMSet(key, fieldMap) {
		return protocol.EncodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	return protocol.EncodeInteger(newFields)
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
		return protocol.EncodeInteger(1)
	}
	return protocol.EncodeInteger(0)
}

func (s *Server) handleHScan(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'hscan' command")
	}

	key := args[0]
	cursor, err := strconv.Atoi(args[1])
	if err != nil {
		return protocol.EncodeError("invalid cursor")
	}

	pattern := "*"
	count := 10

	for i := 2; i < len(args); i += 2 {
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

	hash := s.store.HGetAll(key)
	
	var matchedFields []string
	for field := range hash {
		if matchPattern(pattern, field) {
			matchedFields = append(matchedFields, field)
		}
	}

	start := cursor
	end := cursor + count
	if end > len(matchedFields) {
		end = len(matchedFields)
	}

	var fields []string
	if start < len(matchedFields) {
		fields = matchedFields[start:end]
	}

	nextCursor := 0
	if end < len(matchedFields) {
		nextCursor = end
	}

	result := "*2\r\n"
	result += protocol.EncodeBulkString(strconv.Itoa(nextCursor))
	
	result += fmt.Sprintf("*%d\r\n", len(fields)*2)
	for _, field := range fields {
		result += protocol.EncodeBulkString(field)
		result += protocol.EncodeBulkString(hash[field])
	}

	return result
}

func (s *Server) handleHStrLen(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'hstrlen' command")
	}

	key := args[0]
	field := args[1]
	value, exists := s.store.HGet(key, field)
	if !exists {
		return protocol.EncodeInteger(0)
	}
	return protocol.EncodeInteger(len(value))
}

func (s *Server) handleHRandField(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'hrandfield' command")
	}

	key := args[0]
	count := 1
	withValues := false

	if len(args) >= 2 {
		var err error
		count, err = strconv.Atoi(args[1])
		if err != nil {
			return protocol.EncodeError("value is not an integer or out of range")
		}
	}

	if len(args) >= 3 && strings.ToUpper(args[2]) == "WITHVALUES" {
		withValues = true
	}

	hash := s.store.HGetAll(key)
	if len(hash) == 0 {
		if count == 1 {
			return protocol.EncodeBulkString("")
		}
		return "*0\r\n"
	}

	fields := make([]string, 0, len(hash))
	for field := range hash {
		fields = append(fields, field)
	}

	if count == 1 {
		randomField := fields[rand.Intn(len(fields))]
		if withValues {
			result := "*2\r\n"
			result += protocol.EncodeBulkString(randomField)
			result += protocol.EncodeBulkString(hash[randomField])
			return result
		}
		return protocol.EncodeBulkString(randomField)
	}

	if count < 0 {
		count = -count
	}
	if count > len(fields) {
		count = len(fields)
	}

	selectedFields := make([]string, 0, count)
	for i := 0; i < count; i++ {
		idx := rand.Intn(len(fields))
		selectedFields = append(selectedFields, fields[idx])
	}

	if withValues {
		result := fmt.Sprintf("*%d\r\n", len(selectedFields)*2)
		for _, field := range selectedFields {
			result += protocol.EncodeBulkString(field)
			result += protocol.EncodeBulkString(hash[field])
		}
		return result
	}

	result := fmt.Sprintf("*%d\r\n", len(selectedFields))
	for _, field := range selectedFields {
		result += protocol.EncodeBulkString(field)
	}
	return result
}

func matchPattern(pattern, str string) bool {
	if pattern == "*" {
		return true
	}
	
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		return pattern == str
	}
	
	return simpleGlobMatch(pattern, str)
}

func simpleGlobMatch(pattern, str string) bool {
	if pattern == "" {
		return str == ""
	}
	if pattern == "*" {
		return true
	}
	
	if len(pattern) > 0 && pattern[0] == '*' {
		for i := 0; i <= len(str); i++ {
			if simpleGlobMatch(pattern[1:], str[i:]) {
				return true
			}
		}
		return false
	}
	
	if len(str) == 0 {
		return false
	}
	
	if len(pattern) > 0 && (pattern[0] == '?' || pattern[0] == str[0]) {
		return simpleGlobMatch(pattern[1:], str[1:])
	}
	
	return false
}
