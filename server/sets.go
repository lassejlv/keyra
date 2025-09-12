package server

import (
	"fmt"

	"keyra/protocol"
)

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
	members := s.store.SPop(key, 1)
	if len(members) == 0 {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(members[0])
}

func (s *Server) handleSRandMember(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'srandmember' command")
	}

	key := args[0]
	members := s.store.SRandMember(key, 1)
	if len(members) == 0 {
		return protocol.EncodeBulkString("")
	}
	return protocol.EncodeBulkString(members[0])
}

func (s *Server) handleSInter(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'sinter' command")
	}

	members := s.store.SInter(args...)
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

	members := s.store.SUnion(args...)
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

	members := s.store.SDiff(args...)
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
	count := s.store.SInterStore(destination, keys...)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSUnionStore(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'sunionstore' command")
	}

	destination := args[0]
	keys := args[1:]
	count := s.store.SUnionStore(destination, keys...)
	return protocol.EncodeInteger(count)
}

func (s *Server) handleSDiffStore(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("wrong number of arguments for 'sdiffstore' command")
	}

	destination := args[0]
	keys := args[1:]
	count := s.store.SDiffStore(destination, keys...)
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
