package server

import (
	"keyra/protocol"
)

func (s *Server) handleAuth(args []string, connKey string) string {
	if len(args) < 1 || len(args) > 2 {
		return protocol.EncodeError("ERR wrong number of arguments for 'auth' command")
	}

	if !s.requiresAuth() {
		return protocol.EncodeError("ERR Client sent AUTH, but no password is set")
	}

	var providedPassword string
	
	// Handle both Redis 6.0+ ACL format (AUTH <username> <password>) 
	// and legacy format (AUTH <password>)
	if len(args) == 2 {
		// Redis 6.0+ format: AUTH <username> <password>
		username := args[0]
		providedPassword = args[1]
		
		// Only "default" user is supported for now
		if username != "default" && username != "" {
			return protocol.EncodeError("WRONGPASS invalid username-password pair or user is disabled.")
		}
	} else {
		// Legacy format: AUTH <password>
		providedPassword = args[0]
	}

	if providedPassword == s.password {
		s.authenticate(connKey)
		return protocol.EncodeSimpleString("OK")
	}

	return protocol.EncodeError("WRONGPASS invalid username-password pair or user is disabled.")
}

func (s *Server) handlePing(args []string) string {
	if len(args) == 0 {
		return protocol.EncodeSimpleString("PONG")
	}
	return protocol.EncodeBulkString(args[0])
}
