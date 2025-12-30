package persistence

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type AOF struct {
	filename     string
	file         *os.File
	writer       *bufio.Writer
	mu           sync.Mutex
	enabled      bool
	syncPolicy   string
	lastSync     time.Time
	autoRewrite  bool
	rewriteSize  int64
}

type AOFRewriteStats struct {
	OriginalSize int64
	RewriteSize  int64
	StartTime    time.Time
	EndTime      time.Time
}

func NewAOF(filename string, enabled bool) *AOF {
	if dir := filepath.Dir(filename); dir != "." {
		os.MkdirAll(dir, 0755)
	}
	
	aof := &AOF{
		filename:    filename,
		enabled:     enabled,
		syncPolicy:  "everysec",
		autoRewrite: true,
		rewriteSize: 64 * 1024 * 1024, // 64MB
	}
	
	if enabled {
		aof.open()
	}
	
	return aof
}

func (aof *AOF) open() error {
	file, err := os.OpenFile(aof.filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	
	aof.file = file
	aof.writer = bufio.NewWriter(file)
	return nil
}

func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	if aof.writer != nil {
		aof.writer.Flush()
		aof.writer = nil
	}
	
	if aof.file != nil {
		err := aof.file.Close()
		aof.file = nil
		return err
	}
	
	return nil
}

func (aof *AOF) Enable() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	if aof.enabled {
		return nil
	}
	
	aof.enabled = true
	return aof.open()
}

func (aof *AOF) Disable() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	aof.enabled = false
	
	if aof.writer != nil {
		aof.writer.Flush()
		aof.writer = nil
	}
	
	if aof.file != nil {
		err := aof.file.Close()
		aof.file = nil
		return err
	}
	
	return nil
}

func (aof *AOF) IsEnabled() bool {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.enabled
}

func (aof *AOF) WriteCommand(command []string) error {
	if !aof.enabled || aof.writer == nil {
		return nil
	}
	
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	// Write in Redis protocol format
	aof.writer.WriteString(fmt.Sprintf("*%d\r\n", len(command)))
	for _, arg := range command {
		aof.writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	
	// Sync based on policy
	switch aof.syncPolicy {
	case "always":
		aof.writer.Flush()
		aof.file.Sync()
	case "everysec":
		if time.Since(aof.lastSync) >= time.Second {
			aof.writer.Flush()
			aof.file.Sync()
			aof.lastSync = time.Now()
		}
	case "no":
		// Let OS decide when to sync
	}
	
	return nil
}

func (aof *AOF) Flush() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	if aof.writer != nil {
		aof.writer.Flush()
		if aof.file != nil {
			return aof.file.Sync()
		}
	}
	return nil
}

func (aof *AOF) SetSyncPolicy(policy string) {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	aof.syncPolicy = policy
}

func (aof *AOF) GetSyncPolicy() string {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.syncPolicy
}

func (aof *AOF) Size() (int64, error) {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	if aof.file != nil {
		if stat, err := aof.file.Stat(); err == nil {
			return stat.Size(), nil
		}
	}
	
	if stat, err := os.Stat(aof.filename); err == nil {
		return stat.Size(), nil
	}
	
	return 0, nil
}

func (aof *AOF) NeedsRewrite() bool {
	if !aof.autoRewrite {
		return false
	}
	
	size, err := aof.Size()
	if err != nil {
		return false
	}
	
	return size > aof.rewriteSize
}

func (aof *AOF) LoadCommands() ([][]string, error) {
	file, err := os.Open(aof.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return [][]string{}, nil
		}
		return nil, err
	}
	defer file.Close()
	
	var commands [][]string
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "*") {
			// Parse Redis protocol format
			var argCount int
			fmt.Sscanf(line, "*%d", &argCount)
			
			command := make([]string, 0, argCount)
			
			for i := 0; i < argCount; i++ {
				// Read $length line
				if !scanner.Scan() {
					break
				}
				lengthLine := strings.TrimSpace(scanner.Text())
				var length int
				fmt.Sscanf(lengthLine, "$%d", &length)
				
				// Read actual argument
				if !scanner.Scan() {
					break
				}
				arg := scanner.Text()
				command = append(command, arg)
			}
			
			if len(command) > 0 {
				commands = append(commands, command)
			}
		}
	}
	
	return commands, scanner.Err()
}

func (aof *AOF) Rewrite(getCurrentState func() ([][]string, error)) (*AOFRewriteStats, error) {
	stats := &AOFRewriteStats{
		StartTime: time.Now(),
	}
	
	// Get original file size
	if originalSize, err := aof.Size(); err == nil {
		stats.OriginalSize = originalSize
	}
	
	// Get current database state as commands
	commands, err := getCurrentState()
	if err != nil {
		return stats, err
	}
	
	// Write to temporary file
	tempFile := aof.filename + ".temp"
	tmpFile, err := os.Create(tempFile)
	if err != nil {
		return stats, err
	}
	
	tmpWriter := bufio.NewWriter(tmpFile)
	
	// Write all commands to temp file
	for _, command := range commands {
		tmpWriter.WriteString(fmt.Sprintf("*%d\r\n", len(command)))
		for _, arg := range command {
			tmpWriter.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
		}
	}
	
	tmpWriter.Flush()
	tmpFile.Sync()
	
	// Get new file size
	if stat, err := tmpFile.Stat(); err == nil {
		stats.RewriteSize = stat.Size()
	}
	
	tmpFile.Close()
	
	// Atomically replace old AOF with new one
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	// Close current AOF
	if aof.writer != nil {
		aof.writer.Flush()
		aof.writer = nil
	}
	if aof.file != nil {
		aof.file.Close()
		aof.file = nil
	}
	
	// Replace with new file
	err = os.Rename(tempFile, aof.filename)
	if err != nil {
		os.Remove(tempFile)
		return stats, err
	}
	
	// Reopen AOF if it was enabled
	if aof.enabled {
		aof.open()
	}
	
	stats.EndTime = time.Now()
	return stats, nil
}

func (aof *AOF) ShouldLogCommand(command string) bool {
	// Only log write commands, not read-only commands
	writeCommands := map[string]bool{
		"SET":       true,
		"DEL":       true,
		"EXPIRE":    true,
		"EXPIREAT":  true,
		"PEXPIRE":   true,
		"PEXPIREAT": true,
		"LPUSH":     true,
		"RPUSH":     true,
		"LPOP":      true,
		"RPOP":      true,
		"LSET":      true,
		"LTRIM":     true,
		"LINSERT":   true,
		"SADD":      true,
		"SREM":      true,
		"SPOP":      true,
		"SMOVE":     true,
		"HSET":      true,
		"HDEL":      true,
		"HINCRBY":   true,
		"HINCRBYFLOAT": true,
		"HMSET":     true,
		"HSETNX":    true,
		"ZADD":      true,
		"ZREM":      true,
		"ZINCRBY":   true,
		"FLUSHDB":   true,
		"FLUSHALL":  true,
		"MOVE":      true,
		"SWAPDB":    true,
		"SELECT":    true,
		// JSON commands
		"JSON.SET":       true,
		"JSON.DEL":       true,
		"JSON.FORGET":    true,
		"JSON.NUMINCRBY": true,
		"JSON.NUMMULTBY": true,
		"JSON.STRAPPEND": true,
		"JSON.ARRAPPEND": true,
		"JSON.ARRPOP":    true,
		"JSON.ARRINSERT": true,
		"JSON.ARRTRIM":   true,
	}
	
	return writeCommands[strings.ToUpper(command)]
}
