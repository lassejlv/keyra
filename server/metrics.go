package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type MetricsResponse struct {
	Timestamp        time.Time `json:"timestamp"`
	NetworkInput     int64     `json:"network_input_bytes"`
	NetworkOutput    int64     `json:"network_output_bytes"`
	InputRate        float64   `json:"input_rate_kbps"`
	OutputRate       float64   `json:"output_rate_kbps"`
	CommandsRate     float64   `json:"commands_per_sec"`
	TotalConnections int64     `json:"total_connections"`
	TotalCommands    int64     `json:"total_commands"`
	ActiveClients    int       `json:"active_clients"`
	Uptime           int64     `json:"uptime_seconds"`
	Memory           int64     `json:"used_memory_bytes"`
	Keys             int       `json:"total_keys"`
}

func (s *Server) StartMetricsServer(port int) error {
	http.HandleFunc("/metrics", s.handleMetrics)
	http.HandleFunc("/metrics/network", s.handleNetworkMetrics)
	
	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Metrics server starting on %s\n", addr)
	
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Printf("Metrics server error: %v\n", err)
		}
	}()
	
	return nil
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	bytesReceived, bytesSent, totalConns, totalCmds, rxRate, txRate, cmdRate := s.networkStats.GetStats()
	
	metrics := MetricsResponse{
		Timestamp:        time.Now(),
		NetworkInput:     bytesReceived,
		NetworkOutput:    bytesSent,
		InputRate:        rxRate / 1024,
		OutputRate:       txRate / 1024,
		CommandsRate:     cmdRate,
		TotalConnections: totalConns,
		TotalCommands:    totalCmds,
		ActiveClients:    s.getClientCount(),
		Uptime:           int64(time.Since(s.startTime).Seconds()),
		Memory:           1048576,
		Keys:             s.store.DBSize(),
	}
	
	json.NewEncoder(w).Encode(metrics)
}

func (s *Server) handleNetworkMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	bytesReceived, bytesSent, totalConns, totalCmds, rxRate, txRate, cmdRate := s.networkStats.GetStats()
	
	networkMetrics := map[string]interface{}{
		"timestamp":           time.Now(),
		"network_input_bytes": bytesReceived,
		"network_output_bytes": bytesSent,
		"input_rate_bps":      rxRate,
		"output_rate_bps":     txRate,
		"input_rate_kbps":     rxRate / 1024,
		"output_rate_kbps":    txRate / 1024,
		"commands_per_sec":    cmdRate,
		"total_connections":   totalConns,
		"total_commands":      totalCmds,
	}
	
	json.NewEncoder(w).Encode(networkMetrics)
}
