package server

import (
	"strconv"
	"strings"
	"time"

	"redis-go-clone/protocol"
)

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
