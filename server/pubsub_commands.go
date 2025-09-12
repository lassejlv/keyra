package server

import (
	"fmt"
	"net"
	"strings"

	"keyra/protocol"
)

func (s *Server) handlePublish(args []string) string {
	if len(args) != 2 {
		return protocol.EncodeError("wrong number of arguments for 'publish' command")
	}

	channel := args[0]
	message := args[1]

	recipients := s.pubsub.Publish(channel, message)
	return protocol.EncodeInteger(recipients)
}

func (s *Server) handleSubscribe(args []string, connKey string, conn net.Conn) string {
	if len(args) == 0 {
		return protocol.EncodeError("wrong number of arguments for 'subscribe' command")
	}

	// If conn is nil, try to find it from the connection pool
	if conn == nil {
		if clientConn := s.connPool.GetConnection(connKey); clientConn != nil {
			conn = clientConn.conn
		}
	}

	responses := s.pubsub.Subscribe(connKey, conn, args)
	
	var result strings.Builder
	for _, resp := range responses {
		result.WriteString(fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(resp.Channel), resp.Channel, resp.Count))
	}

	// Put connection into subscriber mode
	return result.String()
}

func (s *Server) handleUnsubscribe(args []string, connKey string) string {
	responses := s.pubsub.Unsubscribe(connKey, args)
	
	if len(responses) == 0 {
		// If no channels specified and no subscriptions exist
		return "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
	}
	
	var result strings.Builder
	for _, resp := range responses {
		result.WriteString(fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(resp.Channel), resp.Channel, resp.Count))
	}

	return result.String()
}

func (s *Server) handlePSubscribe(args []string, connKey string, conn net.Conn) string {
	if len(args) == 0 {
		return protocol.EncodeError("wrong number of arguments for 'psubscribe' command")
	}

	// If conn is nil, try to find it from the connection pool
	if conn == nil {
		if clientConn := s.connPool.GetConnection(connKey); clientConn != nil {
			conn = clientConn.conn
		}
	}

	responses := s.pubsub.PSubscribe(connKey, conn, args)
	
	var result strings.Builder
	for _, resp := range responses {
		result.WriteString(fmt.Sprintf("*3\r\n$10\r\npsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(resp.Pattern), resp.Pattern, resp.Count))
	}

	return result.String()
}

func (s *Server) handlePUnsubscribe(args []string, connKey string) string {
	responses := s.pubsub.PUnsubscribe(connKey, args)
	
	if len(responses) == 0 {
		// If no patterns specified and no subscriptions exist
		return "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n"
	}
	
	var result strings.Builder
	for _, resp := range responses {
		result.WriteString(fmt.Sprintf("*3\r\n$12\r\npunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(resp.Pattern), resp.Pattern, resp.Count))
	}

	return result.String()
}

func (s *Server) handlePubSub(args []string) string {
	if len(args) == 0 {
		return protocol.EncodeError("wrong number of arguments for 'pubsub' command")
	}

	subcommand := strings.ToUpper(args[0])
	subArgs := args[1:]

	switch subcommand {
	case "CHANNELS":
		return s.handlePubSubChannels(subArgs)
	case "NUMSUB":
		return s.handlePubSubNumSub(subArgs)
	case "NUMPAT":
		return s.handlePubSubNumPat(subArgs)
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown pubsub subcommand '%s'", subcommand))
	}
}

func (s *Server) handlePubSubChannels(args []string) string {
	pattern := "*"
	if len(args) > 0 {
		pattern = args[0]
	}

	channels := s.pubsub.GetChannels(pattern)
	
	response := fmt.Sprintf("*%d\r\n", len(channels))
	for _, channel := range channels {
		response += protocol.EncodeBulkString(channel)
	}

	return response
}

func (s *Server) handlePubSubNumSub(args []string) string {
	if len(args) == 0 {
		return "*0\r\n"
	}

	numSub := s.pubsub.GetNumSub(args)
	
	response := fmt.Sprintf("*%d\r\n", len(args)*2)
	for _, channel := range args {
		response += protocol.EncodeBulkString(channel)
		response += protocol.EncodeInteger(numSub[channel])
	}

	return response
}

func (s *Server) handlePubSubNumPat(args []string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'pubsub numpat' command")
	}

	numPat := s.pubsub.GetNumPat()
	return protocol.EncodeInteger(numPat)
}

func (s *Server) isSubscriberConnection(connKey string) bool {
	s.pubsub.mu.RLock()
	defer s.pubsub.mu.RUnlock()
	
	if sub, exists := s.pubsub.subscribers[connKey]; exists {
		return sub.IsSubscribed()
	}
	return false
}

func (s *Server) handleSubscriberCommand(command string, args []string, connKey string, conn net.Conn) string {
	// In subscriber mode, only certain commands are allowed
	switch strings.ToUpper(command) {
	case "SUBSCRIBE":
		return s.handleSubscribe(args, connKey, conn)
	case "UNSUBSCRIBE":
		return s.handleUnsubscribe(args, connKey)
	case "PSUBSCRIBE":
		return s.handlePSubscribe(args, connKey, conn)
	case "PUNSUBSCRIBE":
		return s.handlePUnsubscribe(args, connKey)
	case "PING":
		if len(args) == 0 {
			return "*2\r\n$4\r\npong\r\n$0\r\n\r\n"
		} else {
			return fmt.Sprintf("*2\r\n$4\r\npong\r\n$%d\r\n%s\r\n", len(args[0]), args[0])
		}
	case "QUIT":
		return s.handleQuit(args)
	default:
		return protocol.EncodeError("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
	}
}
