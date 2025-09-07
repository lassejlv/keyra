package server

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"redis-go-clone/protocol"
)

func (s *Server) handleQuit(args []string) string {
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleHello(args []string) string {
	// Default to RESP2 if no version specified
	protocolVersion := 2
	
	if len(args) > 0 {
		if version, err := strconv.Atoi(args[0]); err == nil && (version == 2 || version == 3) {
			protocolVersion = version
		}
	}
	
	if protocolVersion == 3 {
		var info strings.Builder
		
		// Read version from .version file
		version := "unknown"
		if versionBytes, err := os.ReadFile(".version"); err == nil {
			version = strings.Replace(strings.TrimSpace(string(versionBytes)), "v", "", 1)
		}
		
		// Return RESP3 map format
		info.WriteString("%7\r\n") 
		info.WriteString("+server\r\n+redis\r\n")
		info.WriteString("+version\r\n+")
		info.WriteString(version)
		info.WriteString("\r\n")
		info.WriteString("+proto\r\n:3\r\n")
		info.WriteString("+id\r\n:1\r\n")
		info.WriteString("+mode\r\n+standalone\r\n")
		info.WriteString("+role\r\n+master\r\n")
		info.WriteString("+modules\r\n*0\r\n") 
		
		return info.String()
	}
	
	// RESP2 response - return simple OK
	return protocol.EncodeSimpleString("OK")
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

func (s *Server) handleDBSize(args []string) string {
	if len(args) > 0 {
		return protocol.EncodeError("wrong number of arguments for 'dbsize' command")
	}

	size := s.store.DBSize()
	return protocol.EncodeInteger(size)
}

func (s *Server) handleFlushDB(args []string) string {
	s.store.FlushDB()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleFlushAll(args []string) string {
	s.store.FlushDB()
	return protocol.EncodeSimpleString("OK")
}

func (s *Server) handleInfo(args []string) string {
	section := "default"
	if len(args) > 0 {
		section = strings.ToLower(args[0])
	}

	var info strings.Builder

	var version string

	// Read from .version file
	versionBytes, err := os.ReadFile(".version")
	if err != nil {
		version = "unknown"
	} else {
		version = strings.Replace(strings.TrimSpace(string(versionBytes)), "v", "", 1)
		}
	
	if section == "default" || section == "server" {
		info.WriteString("# Server\r\n")
		info.WriteString("redis_version:" + version + "\r\n")
		info.WriteString("redis_mode:standalone\r\n")
		info.WriteString("arch_bits:64\r\n")
		info.WriteString("server_time_usec:" + strconv.FormatInt(time.Now().UnixMicro(), 10) + "\r\n")
		uptime := int(time.Since(s.startTime).Seconds())
		info.WriteString("uptime_in_seconds:" + strconv.Itoa(uptime) + "\r\n")
		info.WriteString("\r\n")
	}
	
	if section == "default" || section == "clients" {
		info.WriteString("# Clients\r\n")
		connStats := s.connPool.GetStats()
		info.WriteString("connected_clients:" + strconv.Itoa(connStats.ActiveConnections) + "\r\n")
		info.WriteString("max_clients:" + strconv.Itoa(connStats.MaxConnections) + "\r\n")
		info.WriteString("total_connections:" + strconv.Itoa(connStats.TotalConnections) + "\r\n")
		info.WriteString("client_longest_output_list:0\r\n")
		info.WriteString("client_biggest_input_buf:0\r\n")
		info.WriteString("blocked_clients:0\r\n")
		info.WriteString("\r\n")
	}
	
	if section == "default" || section == "keyspace" {
		info.WriteString("# Keyspace\r\n")
		
		// Show info for all databases with keys
		dbInfo := s.store.GetDBInfo()
		for dbIndex, keyCount := range dbInfo {
			info.WriteString(fmt.Sprintf("db%d:keys=%d,expires=0,avg_ttl=0\r\n", dbIndex, keyCount))
		}
		
		info.WriteString("\r\n")
	}
	
	if section == "default" || section == "memory" {
		info.WriteString("# Memory\r\n")
		info.WriteString("used_memory:1048576\r\n")
		info.WriteString("used_memory_human:1.00M\r\n")
		info.WriteString("used_memory_peak:1048576\r\n")
		info.WriteString("used_memory_peak_human:1.00M\r\n")
		info.WriteString("maxmemory:" + strconv.Itoa(s.config.MaxMemoryMB*1024*1024) + "\r\n")
		info.WriteString("maxmemory_human:" + strconv.Itoa(s.config.MaxMemoryMB) + "M\r\n")
		info.WriteString("maxmemory_policy:noeviction\r\n")
		info.WriteString("mem_fragmentation_ratio:1.00\r\n")
		info.WriteString("\r\n")
	}

	return protocol.EncodeBulkString(info.String())
}

func (s *Server) handleClient(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("wrong number of arguments for 'client' command")
	}

	subcommand := strings.ToUpper(args[0])
	switch subcommand {
	case "LIST":
		clientCount := s.getClientCount()
		clientInfo := fmt.Sprintf("id=1 addr=127.0.0.1:6379 fd=7 name= age=%d idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\n", 
			int(time.Since(s.startTime).Seconds()))
		
		if clientCount > 1 {
			for i := 2; i <= clientCount; i++ {
				clientInfo += fmt.Sprintf("id=%d addr=127.0.0.1:6379 fd=%d name= age=%d idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=null\n", 
					i, i+5, int(time.Since(s.startTime).Seconds()))
			}
		}
		
		return protocol.EncodeBulkString(clientInfo)
	case "SETNAME":
		return protocol.EncodeSimpleString("OK")
	case "GETNAME":
		return protocol.EncodeBulkString("")
	default:
		return protocol.EncodeError("unknown client subcommand '" + subcommand + "'")
	}
}
