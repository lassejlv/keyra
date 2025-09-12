package server

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"keyra/persistence"
	"keyra/protocol"
)

type MonitorConnection struct {
	conn   net.Conn
	active bool
}

type SlowLogEntry struct {
	ID        int64
	Timestamp time.Time
	Duration  time.Duration
	Command   []string
	ClientIP  string
}

type SlowLog struct {
	mu      sync.RWMutex
	entries []SlowLogEntry
	maxLen  int
	nextID  int64
}

func NewSlowLog(maxLen int) *SlowLog {
	return &SlowLog{
		entries: make([]SlowLogEntry, 0),
		maxLen:  maxLen,
		nextID:  1,
	}
}

func (sl *SlowLog) AddEntry(duration time.Duration, command []string, clientIP string, threshold time.Duration) {
	if duration < threshold {
		return
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	entry := SlowLogEntry{
		ID:        sl.nextID,
		Timestamp: time.Now(),
		Duration:  duration,
		Command:   make([]string, len(command)),
		ClientIP:  clientIP,
	}
	copy(entry.Command, command)

	sl.entries = append(sl.entries, entry)
	if len(sl.entries) > sl.maxLen {
		sl.entries = sl.entries[1:]
	}

	sl.nextID++
}

func (sl *SlowLog) GetEntries(count int) []SlowLogEntry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if count < 0 || count > len(sl.entries) {
		count = len(sl.entries)
	}

	result := make([]SlowLogEntry, count)
	start := len(sl.entries) - count
	for i := 0; i < count; i++ {
		result[i] = sl.entries[start+i]
	}

	// Return in reverse order (newest first)
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

func (sl *SlowLog) Reset() {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.entries = sl.entries[:0]
}

func (sl *SlowLog) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return len(sl.entries)
}

func (sl *SlowLog) SetMaxLen(maxLen int) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.maxLen = maxLen
	if len(sl.entries) > maxLen {
		start := len(sl.entries) - maxLen
		sl.entries = sl.entries[start:]
	}
}

func (s *Server) initializeMonitoring() {
	s.monitorConnections = make(map[string]*MonitorConnection)
	s.slowLog = NewSlowLog(128)
}

func (s *Server) initializeAOF() {
	aofEnabled := false
	if aofConfig, exists := s.runtimeConfig.Get("appendonly"); exists {
		aofEnabled = aofConfig.Value == "yes"
	}
	
	aofFilename := "appendonly.aof"
	if dirConfig, exists := s.runtimeConfig.Get("dir"); exists {
		aofFilename = filepath.Join(dirConfig.Value, aofFilename)
	}
	
	s.aof = persistence.NewAOF(aofFilename, aofEnabled)
	
	// Set sync policy
	if syncConfig, exists := s.runtimeConfig.Get("appendfsync"); exists {
		s.aof.SetSyncPolicy(syncConfig.Value)
	}
	
	// Load AOF commands if enabled and file exists
	if aofEnabled {
		s.loadFromAOF()
	}
}

func (s *Server) loadFromAOF() error {
	commands, err := s.aof.LoadCommands()
	if err != nil {
		fmt.Printf("Error loading AOF: %v\n", err)
		return err
	}
	
	fmt.Printf("Loading %d commands from AOF...\n", len(commands))
	
	for _, command := range commands {
		if len(command) > 0 {
			// Execute command directly against store, bypassing AOF logging
			s.executeCommandDirectly(command[0], command[1:])
		}
	}
	
	fmt.Printf("AOF loaded successfully\n")
	return nil
}

func (s *Server) executeCommandDirectly(command string, args []string) {
	// Execute commands directly against the store without logging to AOF
	// This is used when loading from AOF to avoid duplicate logging
	command = strings.ToUpper(command)
	
	switch command {
	case "SET":
		if len(args) >= 2 {
			s.store.Set(args[0], args[1])
		}
	case "DEL":
		for _, key := range args {
			s.store.Del(key)
		}
	case "EXPIRE":
		if len(args) >= 2 {
			s.handleExpire(args)
		}
	case "SELECT":
		if len(args) >= 1 {
			s.handleSelect(args)
		}
	case "FLUSHDB":
		s.store.FlushDB()
	case "FLUSHALL":
		for i := 0; i < 16; i++ {
			currentDB := s.store.GetCurrentDB()
			s.store.SelectDB(i)
			s.store.FlushDB()
			s.store.SelectDB(currentDB)
		}
	}
}

func (s *Server) addMonitorConnection(connKey string, conn net.Conn) {
	s.monitorMutex.Lock()
	defer s.monitorMutex.Unlock()
	s.monitorConnections[connKey] = &MonitorConnection{
		conn:   conn,
		active: true,
	}
}

func (s *Server) removeMonitorConnection(connKey string) {
	s.monitorMutex.Lock()
	defer s.monitorMutex.Unlock()
	delete(s.monitorConnections, connKey)
}

func (s *Server) broadcastToMonitors(timestamp time.Time, command []string, clientInfo string) {
	s.monitorMutex.RLock()
	connections := make([]*MonitorConnection, 0, len(s.monitorConnections))
	for _, conn := range s.monitorConnections {
		if conn.active {
			connections = append(connections, conn)
		}
	}
	s.monitorMutex.RUnlock()

	if len(connections) == 0 {
		return
	}

	timestampStr := fmt.Sprintf("%.6f", float64(timestamp.UnixNano())/1e9)
	commandStr := strings.Join(command, " ")
	message := fmt.Sprintf("%s [0 %s] \"%s\"\r\n", timestampStr, clientInfo, commandStr)

	for _, conn := range connections {
		conn.conn.Write([]byte(message))
	}
}

func (s *Server) handleMonitor(args []string, connKey string, conn net.Conn) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'monitor' command")
	}

	// For now, just acknowledge the command - actual monitoring requires connection context
	// that will be properly implemented when connection tracking is enhanced
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleSlowlog(args []string) string {
	if len(args) == 0 {
		return protocol.EncodeError("wrong number of arguments for 'slowlog' command")
	}

	subcommand := strings.ToUpper(args[0])
	subArgs := args[1:]

	switch subcommand {
	case "GET":
		return s.handleSlowlogGet(subArgs)
	case "RESET":
		return s.handleSlowlogReset(subArgs)
	case "LEN":
		return s.handleSlowlogLen(subArgs)
	default:
		return protocol.EncodeError(fmt.Sprintf("unknown slowlog subcommand '%s'", subcommand))
	}
}

func (s *Server) handleSlowlogGet(args []string) string {
	count := -1
	var err error

	if len(args) > 1 {
		return protocol.EncodeError("wrong number of arguments for 'slowlog get' command")
	}

	if len(args) == 1 {
		count, err = strconv.Atoi(args[0])
		if err != nil {
			return protocol.EncodeError("value is not an integer or out of range")
		}
	}

	entries := s.slowLog.GetEntries(count)
	response := fmt.Sprintf("*%d\r\n", len(entries))

	for _, entry := range entries {
		// Each entry is an array: [id, timestamp, duration_microseconds, command_array, client_info]
		response += "*5\r\n"
		response += protocol.EncodeInteger(int(entry.ID))
		response += protocol.EncodeInteger(int(entry.Timestamp.Unix()))
		response += protocol.EncodeInteger(int(entry.Duration.Microseconds()))

		// Command array
		response += fmt.Sprintf("*%d\r\n", len(entry.Command))
		for _, cmd := range entry.Command {
			response += protocol.EncodeBulkString(cmd)
		}

		// Client info
		response += protocol.EncodeBulkString(entry.ClientIP)
	}

	return response
}

func (s *Server) handleSlowlogReset(args []string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'slowlog reset' command")
	}

	s.slowLog.Reset()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleSlowlogLen(args []string) string {
	if len(args) != 0 {
		return protocol.EncodeError("wrong number of arguments for 'slowlog len' command")
	}

	return protocol.EncodeInteger(s.slowLog.Len())
}

func (s *Server) executeCommandWithTiming(command string, args []string, connKey string, clientIP string) string {
	start := time.Now()
	
	// Build full command for monitoring and slowlog
	fullCommand := make([]string, len(args)+1)
	fullCommand[0] = command
	copy(fullCommand[1:], args)

	// Execute the command with AOF logging
	result := s.executeCommandWithAOF(command, args, connKey)
	
	duration := time.Since(start)

	// Broadcast to monitors
	s.broadcastToMonitors(start, fullCommand, clientIP)

	// Add to slowlog if enabled
	slowlogThreshold, _ := s.runtimeConfig.Get("slowlog-log-slower-than")
	if thresholdMicros, err := strconv.Atoi(slowlogThreshold.Value); err == nil && thresholdMicros >= 0 {
		threshold := time.Duration(thresholdMicros) * time.Microsecond
		s.slowLog.AddEntry(duration, fullCommand, clientIP, threshold)
	}

	return result
}
