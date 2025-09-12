package server

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"keyra/protocol"
)

type ConfigValue struct {
	Value       string
	Description string
	ReadOnly    bool
}

type RuntimeConfig struct {
	mu     sync.RWMutex
	values map[string]*ConfigValue
}

func NewRuntimeConfig() *RuntimeConfig {
	rc := &RuntimeConfig{
		values: make(map[string]*ConfigValue),
	}
	rc.initializeDefaults()
	return rc
}

func (rc *RuntimeConfig) initializeDefaults() {
	defaults := map[string]*ConfigValue{
		"maxmemory": {
			Value:       "1073741824",
			Description: "Memory usage limit in bytes",
			ReadOnly:    false,
		},
		"maxmemory-policy": {
			Value:       "noeviction",
			Description: "How Redis will select what to remove when maxmemory is reached",
			ReadOnly:    false,
		},
		"timeout": {
			Value:       "0",
			Description: "Close connection after client is idle for N seconds (0 to disable)",
			ReadOnly:    false,
		},
		"tcp-keepalive": {
			Value:       "300",
			Description: "TCP keepalive time in seconds",
			ReadOnly:    false,
		},
		"databases": {
			Value:       "16",
			Description: "Number of databases",
			ReadOnly:    true,
		},
		"save": {
			Value:       "900 1 300 10 60 10000",
			Description: "Save the dataset if both conditions are met: after N seconds and at least M changes",
			ReadOnly:    false,
		},
		"dir": {
			Value:       "./",
			Description: "Working directory for RDB/AOF files",
			ReadOnly:    false,
		},
		"dbfilename": {
			Value:       "redis_data.rdb",
			Description: "Filename for RDB persistence",
			ReadOnly:    false,
		},
		"port": {
			Value:       "6379",
			Description: "Server port",
			ReadOnly:    true,
		},
		"bind": {
			Value:       "127.0.0.1",
			Description: "Bind address",
			ReadOnly:    true,
		},
		"protected-mode": {
			Value:       "yes",
			Description: "Protected mode prevents connections from non-loopback interfaces",
			ReadOnly:    false,
		},
		"requirepass": {
			Value:       "",
			Description: "Require password for client connections",
			ReadOnly:    false,
		},
		"maxclients": {
			Value:       "10000",
			Description: "Maximum number of connected clients",
			ReadOnly:    false,
		},
		"slowlog-log-slower-than": {
			Value:       "10000",
			Description: "Log queries slower than this many microseconds",
			ReadOnly:    false,
		},
		"slowlog-max-len": {
			Value:       "128",
			Description: "Maximum length of slow log",
			ReadOnly:    false,
		},
		"loglevel": {
			Value:       "notice",
			Description: "Log level (debug, verbose, notice, warning)",
			ReadOnly:    false,
		},
		"tcp-backlog": {
			Value:       "511",
			Description: "TCP listen() backlog",
			ReadOnly:    false,
		},
		"client-output-buffer-limit": {
			Value:       "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60",
			Description: "Client output buffer limits",
			ReadOnly:    false,
		},
		"hz": {
			Value:       "10",
			Description: "Background task frequency",
			ReadOnly:    false,
		},
		"rdbcompression": {
			Value:       "yes",
			Description: "Compress RDB files using LZF",
			ReadOnly:    false,
		},
		"rdbchecksum": {
			Value:       "yes",
			Description: "Add checksum to RDB files",
			ReadOnly:    false,
		},
		"appendonly": {
			Value:       "no",
			Description: "Enable AOF (Append Only File) persistence",
			ReadOnly:    false,
		},
		"appendfilename": {
			Value:       "appendonly.aof",
			Description: "AOF filename",
			ReadOnly:    false,
		},
		"appendfsync": {
			Value:       "everysec",
			Description: "AOF sync policy (always, everysec, no)",
			ReadOnly:    false,
		},
		"auto-aof-rewrite-percentage": {
			Value:       "100",
			Description: "AOF rewrite percentage threshold",
			ReadOnly:    false,
		},
		"auto-aof-rewrite-min-size": {
			Value:       "67108864",
			Description: "AOF rewrite minimum size (64MB)",
			ReadOnly:    false,
		},
		"aof-load-truncated": {
			Value:       "yes",
			Description: "Load truncated AOF files",
			ReadOnly:    false,
		},
	}

	for key, value := range defaults {
		rc.values[key] = value
	}
}

func (rc *RuntimeConfig) Get(key string) (*ConfigValue, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	value, exists := rc.values[key]
	if exists {
		return &ConfigValue{
			Value:       value.Value,
			Description: value.Description,
			ReadOnly:    value.ReadOnly,
		}, true
	}
	return nil, false
}

func (rc *RuntimeConfig) Set(key, value string) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	
	configValue, exists := rc.values[key]
	if !exists {
		return fmt.Errorf("unknown config parameter: %s", key)
	}
	
	if configValue.ReadOnly {
		return fmt.Errorf("config parameter '%s' is read-only", key)
	}
	
	if err := rc.validateValue(key, value); err != nil {
		return err
	}
	
	rc.values[key].Value = value
	return nil
}

func (rc *RuntimeConfig) validateValue(key, value string) error {
	switch key {
	case "maxmemory":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return fmt.Errorf("invalid value for maxmemory: must be a number")
		}
	case "timeout", "tcp-keepalive", "slowlog-log-slower-than", "slowlog-max-len", "maxclients", "tcp-backlog", "hz":
		if _, err := strconv.Atoi(value); err != nil {
			return fmt.Errorf("invalid value for %s: must be a number", key)
		}
	case "maxmemory-policy":
		validPolicies := []string{"noeviction", "allkeys-lru", "volatile-lru", "allkeys-random", "volatile-random", "volatile-ttl"}
		valid := false
		for _, policy := range validPolicies {
			if value == policy {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid maxmemory-policy: %s", value)
		}
	case "loglevel":
		validLevels := []string{"debug", "verbose", "notice", "warning"}
		valid := false
		for _, level := range validLevels {
			if value == level {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid loglevel: %s", value)
		}
	case "protected-mode", "rdbcompression", "rdbchecksum", "appendonly", "aof-load-truncated":
		if value != "yes" && value != "no" {
			return fmt.Errorf("invalid value for %s: must be 'yes' or 'no'", key)
		}
	case "appendfsync":
		validPolicies := []string{"always", "everysec", "no"}
		valid := false
		for _, policy := range validPolicies {
			if value == policy {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid appendfsync policy: %s", value)
		}
	case "auto-aof-rewrite-percentage", "auto-aof-rewrite-min-size":
		if _, err := strconv.Atoi(value); err != nil {
			return fmt.Errorf("invalid value for %s: must be a number", key)
		}
	}
	return nil
}

func (rc *RuntimeConfig) GetAll() map[string]*ConfigValue {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	
	result := make(map[string]*ConfigValue)
	for key, value := range rc.values {
		result[key] = &ConfigValue{
			Value:       value.Value,
			Description: value.Description,
			ReadOnly:    value.ReadOnly,
		}
	}
	return result
}

func (rc *RuntimeConfig) GetMatching(pattern string) map[string]*ConfigValue {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	
	result := make(map[string]*ConfigValue)
	pattern = strings.ToLower(pattern)
	
	for key, value := range rc.values {
		if pattern == "*" || strings.Contains(strings.ToLower(key), pattern) {
			result[key] = &ConfigValue{
				Value:       value.Value,
				Description: value.Description,
				ReadOnly:    value.ReadOnly,
			}
		}
	}
	return result
}

func (s *Server) handleConfigGet(args []string) string {
	if len(args) != 1 {
		return protocol.EncodeError("wrong number of arguments for 'config get' command")
	}
	
	pattern := args[0]
	configs := s.runtimeConfig.GetMatching(pattern)
	
	result := make([]string, 0, len(configs)*2)
	for key, value := range configs {
		result = append(result, key, value.Value)
	}
	
	response := fmt.Sprintf("*%d\r\n", len(result))
	for _, item := range result {
		response += protocol.EncodeBulkString(item)
	}
	
	return response
}

func (s *Server) handleConfigSet(args []string) string {
	if len(args) != 2 {
		return protocol.EncodeError("wrong number of arguments for 'config set' command")
	}
	
	key := strings.ToLower(args[0])
	value := args[1]
	
	if err := s.runtimeConfig.Set(key, value); err != nil {
		return protocol.EncodeError(err.Error())
	}
	
	s.applyConfigChange(key, value)
	
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleConfigRewrite(args []string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'config rewrite' command")
	}
	
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleConfigResetstat(args []string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'config resetstat' command")
	}
	
	s.networkStats = NewNetworkStats()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleConfig(args []string) string {
	if len(args) == 0 {
		return protocol.EncodeError("wrong number of arguments for 'config' command")
	}
	
	subcommand := strings.ToUpper(args[0])
	subArgs := args[1:]
	
	switch subcommand {
	case "GET":
		return s.handleConfigGet(subArgs)
	case "SET":
		return s.handleConfigSet(subArgs)
	case "REWRITE":
		return s.handleConfigRewrite(subArgs)
	case "RESETSTAT":
		return s.handleConfigResetstat(subArgs)
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown config subcommand '%s'", subcommand))
	}
}

func (s *Server) applyConfigChange(key, value string) {
	switch key {
	case "maxmemory":
		if maxMem, err := strconv.ParseInt(value, 10, 64); err == nil {
			s.config.MaxMemoryMB = int(maxMem / (1024 * 1024))
		}
	case "maxclients":
		if maxClients, err := strconv.Atoi(value); err == nil {
			s.config.ConnectionConfig.MaxConnections = maxClients
		}
	case "timeout":
		if timeout, err := strconv.Atoi(value); err == nil {
			s.config.ConnectionConfig.IdleTimeout = time.Duration(timeout) * time.Second
		}
	case "tcp-keepalive":
		if keepalive, err := strconv.Atoi(value); err == nil {
			s.config.ConnectionConfig.ReadTimeout = time.Duration(keepalive) * time.Second
		}
	case "requirepass":
		s.password = value
		if value != "" {
			fmt.Println("Password authentication enabled via CONFIG SET")
		} else {
			fmt.Println("Password authentication disabled via CONFIG SET")
		}
	case "slowlog-max-len":
		if maxLen, err := strconv.Atoi(value); err == nil && maxLen >= 0 {
			s.slowLog.SetMaxLen(maxLen)
		}
	case "appendonly":
		if value == "yes" {
			if s.aof != nil {
				s.aof.Enable()
				fmt.Println("AOF enabled via CONFIG SET")
			}
		} else {
			if s.aof != nil {
				s.aof.Disable()
				fmt.Println("AOF disabled via CONFIG SET")
			}
		}
	case "appendfsync":
		if s.aof != nil {
			s.aof.SetSyncPolicy(value)
			fmt.Printf("AOF sync policy set to %s\n", value)
		}
	}
}
