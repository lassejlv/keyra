package server

import (
	"strconv"

	"redis-go-clone/protocol"
)

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
