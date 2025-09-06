package server

import (
	"redis-go-clone/protocol"
)

func (s *Server) handleAuth(args []string, connKey string) string {
	if len(args) != 1 {
		return protocol.EncodeError("wrong number of arguments for 'auth' command")
	}

	if !s.requiresAuth() {
		return protocol.EncodeError("ERR Client sent AUTH, but no password is set")
	}

	providedPassword := args[0]
	if providedPassword == s.password {
		s.authenticate(connKey)
		return protocol.EncodeSimpleString("OK")
	}

	return protocol.EncodeError("ERR invalid password")
}

func (s *Server) handlePing(args []string) string {
	if len(args) == 0 {
		return protocol.EncodeSimpleString("PONG")
	}
	return protocol.EncodeBulkString(args[0])
}
