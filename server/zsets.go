package server

import (
	"fmt"
	"strconv"
	"strings"

	"keyra/protocol"
)

// Sorted Set commands
func (s *Server) handleZAdd(args []string) string {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.EncodeError("wrong number of arguments for 'zadd' command")
	}

	key := args[0]
	scoreMembers := args[1:]
	
	if len(scoreMembers)%2 != 0 {
		return protocol.EncodeError("wrong number of arguments for 'zadd' command")
	}
	
	members := make(map[string]float64)
	for i := 0; i < len(scoreMembers); i += 2 {
		score, err := strconv.ParseFloat(scoreMembers[i], 64)
		if err != nil {
			return protocol.EncodeError("value is not a valid float")
		}
		members[scoreMembers[i+1]] = score
	}
	
	added := s.store.ZAdd(key, members)
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
	count := s.store.ZRem(key, members...)
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

	members := s.store.ZRange(key, start, stop, withScores)
	
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
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

	members := s.store.ZRevRange(key, start, stop, withScores)
	
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
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

	members := s.store.ZRangeByScore(key, min, max, withScores)
	
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
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

	members := s.store.ZRevRangeByScore(key, max, min, withScores)
	
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += protocol.EncodeBulkString(member)
	}
	return result
}

func (s *Server) handleZRank(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'zrank' command")
	}

	key := args[0]
	member := args[1]
	rank, exists := s.store.ZRank(key, member)
	if !exists {
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
	rank, exists := s.store.ZRevRank(key, member)
	if !exists {
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
