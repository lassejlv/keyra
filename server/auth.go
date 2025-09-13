package server

import (
	"keyra/protocol"
)

func (s *Server) handleAuth(args []string, connKey string) string {
	if len(args) < 1 || len(args) > 2 {
		return protocol.EncodeError("wrong number of arguments for 'auth' command")
	}

	if !s.requiresAuth() {
		return protocol.EncodeError("ERR Client sent AUTH, but no password is set")
	}

	var providedPassword string
	
	// Handle both Redis 6.0+ ACL format (AUTH <username> <password>) 
	// and legacy format (AUTH <password>)
	if len(args) == 2 {
		// Redis 6.0+ format: AUTH <username> <password>
		// For now, we ignore the username since we only support single password auth
		// username := args[0]
		providedPassword = args[1]
	} else {
		// Legacy format: AUTH <password>
		providedPassword = args[0]
	}

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
