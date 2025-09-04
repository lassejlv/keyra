package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"redis-go-clone/persistence"
	"redis-go-clone/protocol"
	"redis-go-clone/store"
)

type Server struct {
	address        string
	store          *store.Store
	saveInterval   time.Duration
	shutdownSignal chan os.Signal
}

func New(address string) *Server {
	storagePath := persistence.GetStoragePath()
	fmt.Printf("Using storage path: %s\n", storagePath)
	
	saveInterval := 30 * time.Second
	if envInterval := os.Getenv("REDIS_SAVE_INTERVAL"); envInterval != "" {
		if duration, err := time.ParseDuration(envInterval); err == nil {
			saveInterval = duration
		}
	}
	
	return &Server{
		address:        address,
		store:          store.New(storagePath),
		saveInterval:   saveInterval,
		shutdownSignal: make(chan os.Signal, 1),
	}
}

func NewInMemory(address string) *Server {
	return &Server{
		address:        address,
		store:          store.NewInMemory(),
		shutdownSignal: make(chan os.Signal, 1),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer listener.Close()

	signal.Notify(s.shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Redis server listening on %s\n", s.address)

	if s.saveInterval > 0 {
		go s.periodicSave()
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go s.handleConnection(conn)
		}
	}()

	<-s.shutdownSignal
	fmt.Println("\nShutting down server...")
	
	if err := s.store.Save(); err != nil {
		log.Printf("Error saving data on shutdown: %v", err)
	} else {
		fmt.Println("Data saved successfully")
	}
	
	return nil
}

func (s *Server) periodicSave() {
	ticker := time.NewTicker(s.saveInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := s.store.Save(); err != nil {
				log.Printf("Error during periodic save: %v", err)
			} else {
				log.Println("Periodic save completed")
			}
		case <-s.shutdownSignal:
			return
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	parser := protocol.NewParser(conn)

	for {
		args, err := parser.Parse()
		if err != nil {
			return
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])
		response := s.executeCommand(command, args[1:])
		conn.Write([]byte(response))
	}
}

func (s *Server) executeCommand(command string, args []string) string {
	switch command {
	case "SET":
		return s.handleSet(args)
	case "GET":
		return s.handleGet(args)
	case "DEL":
		return s.handleDel(args)
	case "PING":
		return s.handlePing(args)
	case "TYPE":
		return s.handleType(args)
	case "TTL":
		return s.handleTTL(args)
	case "EXPIRE":
		return s.handleExpire(args)
	case "EXPIREAT":
		return s.handleExpireAt(args)
	case "SAVE":
		return s.handleSave(args)
	case "BGSAVE":
		return s.handleBGSave(args)
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown command '%s'", command))
	}
}

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

func (s *Server) handlePing(args []string) string {
	if len(args) == 0 {
		return protocol.EncodeSimpleString("PONG")
	}
	return protocol.EncodeBulkString(args[0])
}

func (s *Server) handleType(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'type' command")
	}

	key := args[0]
	_, exists := s.store.Get(key)
	if !exists {
		return protocol.EncodeSimpleString("none")
	}
	return protocol.EncodeSimpleString("string")
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

func (s *Server) handleSave(args []string) string {
	if len(args) > 0 {
		return protocol.EncodeError("wrong number of arguments for 'save' command")
	}

	err := s.store.Save()
	if err != nil {
		return protocol.EncodeError("save failed")
	}
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleBGSave(args []string) string {
	if len(args) > 0 {
		return protocol.EncodeError("wrong number of arguments for 'bgsave' command")
	}

	go func() {
		if err := s.store.Save(); err != nil {
			log.Printf("Background save failed: %v", err)
		} else {
			log.Println("Background save completed")
		}
	}()

	return protocol.EncodeSimpleString("Background saving started")
}
