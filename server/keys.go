package server

import (
	"fmt"
	"strconv"
	"strings"

	"keyra/protocol"
)

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
