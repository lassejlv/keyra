package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type HTTPResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// StartHTTPServer starts the Upstash-compatible HTTP API server
func (s *Server) StartHTTPServer(port int) {
	mux := http.NewServeMux()
	
	// Main endpoint for Redis commands
	mux.HandleFunc("/", s.handleHTTPCommand)
	
	// Health check endpoint
	mux.HandleFunc("/health", s.handleHealth)
	
	// Pipeline endpoint for multiple commands
	mux.HandleFunc("/pipeline", s.handlePipeline)
	
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	
	fmt.Printf("HTTP API server listening on port %d\n", port)
	
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()
}

func (s *Server) handleHTTPCommand(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	
	if r.Method != "POST" {
		s.writeHTTPError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Authenticate request
	if !s.authenticateHTTPRequest(r) {
		s.writeHTTPError(w, "Authentication failed", http.StatusUnauthorized)
		return
	}
	
	// Parse command from JSON body
	var command []string
	if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
		s.writeHTTPError(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	
	if len(command) == 0 {
		s.writeHTTPError(w, "Empty command", http.StatusBadRequest)
		return
	}
	
	// Execute command
	connKey := fmt.Sprintf("http-%p", r)
	
	// For HTTP requests, we auto-authenticate the connection if auth is required
	if s.requiresAuth() {
		s.authenticate(connKey)
	}
	
	result := s.executeHTTPCommand(command, connKey)
	
	// Write response
	s.writeHTTPResponse(w, result)
}

func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	
	if r.Method != "POST" {
		s.writeHTTPError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Authenticate request
	if !s.authenticateHTTPRequest(r) {
		s.writeHTTPError(w, "Authentication failed", http.StatusUnauthorized)
		return
	}
	
	// Parse multiple commands from JSON body
	var commands [][]string
	if err := json.NewDecoder(r.Body).Decode(&commands); err != nil {
		s.writeHTTPError(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	
	if len(commands) == 0 {
		s.writeHTTPError(w, "Empty pipeline", http.StatusBadRequest)
		return
	}
	
	// Execute commands in pipeline
	connKey := fmt.Sprintf("http-pipeline-%p", r)
	
	// For HTTP requests, we auto-authenticate the connection if auth is required
	if s.requiresAuth() {
		s.authenticate(connKey)
	}
	
	results := make([]interface{}, len(commands))
	
	for i, command := range commands {
		if len(command) == 0 {
			results[i] = map[string]interface{}{"error": "Empty command"}
			continue
		}
		results[i] = s.executeHTTPCommand(command, connKey)
	}
	
	// Write pipeline response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status": "ok",
		"uptime": time.Since(s.startTime).Seconds(),
		"connections": s.getClientCount(),
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) authenticateHTTPRequest(r *http.Request) bool {
	// If no password is required, allow all requests
	if !s.requiresAuth() {
		return true
	}
	
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}
	
	// Support Bearer token format
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		return token == s.password
	}
	
	// Support Basic auth format
	if strings.HasPrefix(authHeader, "Basic ") {
		// For Basic auth, we expect the password to be the password field
		// Username can be anything (following Upstash pattern)
		username, password, ok := r.BasicAuth()
		if !ok {
			return false
		}
		_ = username // Ignore username for now
		return password == s.password
	}
	
	return false
}

func (s *Server) executeHTTPCommand(command []string, connKey string) interface{} {
	if len(command) == 0 {
		return map[string]interface{}{"error": "Empty command"}
	}
	
	cmdName := strings.ToUpper(command[0])
	args := command[1:]
	
	// Execute the command using existing Redis logic
	response := s.executeCommand(cmdName, args, connKey)
	
	// Parse the RESP response and convert to JSON-friendly format
	return s.parseRESPToJSON(response)
}

func (s *Server) parseRESPToJSON(resp string) interface{} {
	if len(resp) == 0 {
		return nil
	}
	
	switch resp[0] {
	case '+': // Simple string
		return strings.TrimSuffix(resp[1:], "\r\n")
	case '-': // Error
		errorMsg := strings.TrimSuffix(resp[1:], "\r\n")
		if strings.HasPrefix(errorMsg, "ERR ") {
			errorMsg = errorMsg[4:]
		}
		return map[string]interface{}{"error": errorMsg}
	case ':': // Integer
		numStr := strings.TrimSuffix(resp[1:], "\r\n")
		if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			return num
		}
		return 0
	case '$': // Bulk string
		return s.parseBulkString(resp)
	case '*': // Array
		return s.parseArray(resp)
	default:
		return resp
	}
}

func (s *Server) parseBulkString(resp string) interface{} {
	lines := strings.Split(resp, "\r\n")
	if len(lines) < 2 {
		return nil
	}
	
	lengthStr := lines[0][1:] // Remove '$'
	length, err := strconv.Atoi(lengthStr)
	if err != nil || length < 0 {
		return nil
	}
	
	if len(lines) < 2 {
		return nil
	}
	
	return lines[1]
}

func (s *Server) parseArray(resp string) interface{} {
	lines := strings.Split(resp, "\r\n")
	if len(lines) == 0 {
		return []interface{}{}
	}
	
	countStr := lines[0][1:] // Remove '*'
	count, err := strconv.Atoi(countStr)
	if err != nil || count < 0 {
		return []interface{}{}
	}
	
	if count == 0 {
		return []interface{}{}
	}
	
	result := make([]interface{}, 0, count)
	lineIdx := 1
	
	for i := 0; i < count && lineIdx < len(lines); i++ {
		if lineIdx >= len(lines) {
			break
		}
		
		line := lines[lineIdx]
		if len(line) == 0 {
			lineIdx++
			continue
		}
		
		switch line[0] {
		case '$': // Bulk string
			lengthStr := line[1:]
			length, err := strconv.Atoi(lengthStr)
			if err != nil || length < 0 {
				result = append(result, nil)
				lineIdx++
				continue
			}
			
			lineIdx++
			if lineIdx < len(lines) {
				result = append(result, lines[lineIdx])
			} else {
				result = append(result, nil)
			}
			lineIdx++
		case ':': // Integer
			numStr := line[1:]
			if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
				result = append(result, num)
			} else {
				result = append(result, 0)
			}
			lineIdx++
		case '+': // Simple string
			result = append(result, line[1:])
			lineIdx++
		case '-': // Error
			errorMsg := line[1:]
			if strings.HasPrefix(errorMsg, "ERR ") {
				errorMsg = errorMsg[4:]
			}
			result = append(result, map[string]interface{}{"error": errorMsg})
			lineIdx++
		default:
			result = append(result, line)
			lineIdx++
		}
	}
	
	return result
}

func (s *Server) writeHTTPResponse(w http.ResponseWriter, result interface{}) {
	w.Header().Set("Content-Type", "application/json")
	
	// Check if result contains an error
	if errorMap, ok := result.(map[string]interface{}); ok {
		if errorMsg, exists := errorMap["error"]; exists {
			w.WriteHeader(http.StatusBadRequest)
			response := HTTPResponse{Error: fmt.Sprintf("%v", errorMsg)}
			json.NewEncoder(w).Encode(response)
			return
		}
	}
	
	w.WriteHeader(http.StatusOK)
	response := HTTPResponse{Result: result}
	json.NewEncoder(w).Encode(response)
}

func (s *Server) writeHTTPError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	response := HTTPResponse{Error: message}
	json.NewEncoder(w).Encode(response)
}
