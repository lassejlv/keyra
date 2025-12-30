package server

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"keyra/protocol"
)

// JSON.SET key path value [NX | XX]
func (s *Server) handleJSONSet(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.SET' command")
	}

	key := args[0]
	path := args[1]
	valueStr := args[2]

	// Parse options
	nx := false
	xx := false
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		}
	}

	// Check NX/XX conditions
	exists := s.store.Exists(key)
	if nx && exists {
		return protocol.EncodeNull()
	}
	if xx && !exists {
		return protocol.EncodeNull()
	}

	// Parse the JSON value
	var value interface{}
	if err := json.Unmarshal([]byte(valueStr), &value); err != nil {
		return protocol.EncodeError("ERR invalid JSON value")
	}

	if s.store.JSONSet(key, path, value) {
		return protocol.EncodeSimpleString("OK")
	}
	return protocol.EncodeError("ERR could not set JSON value")
}

// JSON.GET key [path [path ...]]
func (s *Server) handleJSONGet(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.GET' command")
	}

	key := args[0]
	paths := args[1:]

	result, ok := s.store.JSONGet(key, paths...)
	if !ok {
		return protocol.EncodeNull()
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return protocol.EncodeNull()
	}

	return protocol.EncodeBulkString(string(jsonBytes))
}

// JSON.DEL key [path]
func (s *Server) handleJSONDel(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.DEL' command")
	}

	key := args[0]
	path := "$"
	if len(args) > 1 {
		path = args[1]
	}

	count := s.store.JSONDel(key, path)
	return protocol.EncodeInteger(count)
}

// JSON.FORGET is an alias for JSON.DEL
func (s *Server) handleJSONForget(args []string) string {
	return s.handleJSONDel(args)
}

// JSON.TYPE key [path]
func (s *Server) handleJSONType(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.TYPE' command")
	}

	key := args[0]
	path := "$"
	if len(args) > 1 {
		path = args[1]
	}

	jsonType := s.store.JSONType(key, path)
	if jsonType == "" {
		return protocol.EncodeNull()
	}

	return protocol.EncodeBulkString(jsonType)
}

// JSON.NUMINCRBY key path value
func (s *Server) handleJSONNumIncrBy(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.NUMINCRBY' command")
	}

	key := args[0]
	path := args[1]
	increment, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return protocol.EncodeError("ERR value is not a valid float")
	}

	result, ok := s.store.JSONNumIncrBy(key, path, increment)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeBulkString(strconv.FormatFloat(result, 'f', -1, 64))
}

// JSON.NUMMULTBY key path value
func (s *Server) handleJSONNumMultBy(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.NUMMULTBY' command")
	}

	key := args[0]
	path := args[1]
	multiplier, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return protocol.EncodeError("ERR value is not a valid float")
	}

	result, ok := s.store.JSONNumMultBy(key, path, multiplier)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeBulkString(strconv.FormatFloat(result, 'f', -1, 64))
}

// JSON.STRAPPEND key [path] value
func (s *Server) handleJSONStrAppend(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.STRAPPEND' command")
	}

	key := args[0]
	var path, appendStr string

	if len(args) == 2 {
		path = "$"
		appendStr = args[1]
	} else {
		path = args[1]
		appendStr = args[2]
	}

	// Parse the JSON string value
	var str string
	if err := json.Unmarshal([]byte(appendStr), &str); err != nil {
		// Try using it as a raw string
		str = appendStr
	}

	result, ok := s.store.JSONStrAppend(key, path, str)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.STRLEN key [path]
func (s *Server) handleJSONStrLen(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.STRLEN' command")
	}

	key := args[0]
	path := "$"
	if len(args) > 1 {
		path = args[1]
	}

	result, ok := s.store.JSONStrLen(key, path)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.ARRAPPEND key path value [value ...]
func (s *Server) handleJSONArrAppend(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.ARRAPPEND' command")
	}

	key := args[0]
	path := args[1]
	
	values := make([]interface{}, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		var value interface{}
		if err := json.Unmarshal([]byte(args[i]), &value); err != nil {
			return protocol.EncodeError("ERR invalid JSON value")
		}
		values = append(values, value)
	}

	result, ok := s.store.JSONArrAppend(key, path, values...)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.ARRLEN key [path]
func (s *Server) handleJSONArrLen(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.ARRLEN' command")
	}

	key := args[0]
	path := "$"
	if len(args) > 1 {
		path = args[1]
	}

	result, ok := s.store.JSONArrLen(key, path)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.ARRPOP key [path [index]]
func (s *Server) handleJSONArrPop(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.ARRPOP' command")
	}

	key := args[0]
	path := "$"
	index := -1 // Default to last element

	if len(args) > 1 {
		path = args[1]
	}
	if len(args) > 2 {
		var err error
		index, err = strconv.Atoi(args[2])
		if err != nil {
			return protocol.EncodeError("ERR index is not an integer")
		}
	}

	result, ok := s.store.JSONArrPop(key, path, index)
	if !ok {
		return protocol.EncodeNull()
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return protocol.EncodeNull()
	}

	return protocol.EncodeBulkString(string(jsonBytes))
}

// JSON.ARRINDEX key path value [start [stop]]
func (s *Server) handleJSONArrIndex(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.ARRINDEX' command")
	}

	key := args[0]
	path := args[1]

	var value interface{}
	if err := json.Unmarshal([]byte(args[2]), &value); err != nil {
		return protocol.EncodeError("ERR invalid JSON value")
	}

	result, ok := s.store.JSONArrIndex(key, path, value)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.ARRINSERT key path index value [value ...]
func (s *Server) handleJSONArrInsert(args []string) string {
	if len(args) < 4 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.ARRINSERT' command")
	}

	key := args[0]
	path := args[1]
	index, err := strconv.Atoi(args[2])
	if err != nil {
		return protocol.EncodeError("ERR index is not an integer")
	}

	values := make([]interface{}, 0, len(args)-3)
	for i := 3; i < len(args); i++ {
		var value interface{}
		if err := json.Unmarshal([]byte(args[i]), &value); err != nil {
			return protocol.EncodeError("ERR invalid JSON value")
		}
		values = append(values, value)
	}

	result, ok := s.store.JSONArrInsert(key, path, index, values...)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.ARRTRIM key path start stop
func (s *Server) handleJSONArrTrim(args []string) string {
	if len(args) < 4 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.ARRTRIM' command")
	}

	key := args[0]
	path := args[1]
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return protocol.EncodeError("ERR start is not an integer")
	}
	stop, err := strconv.Atoi(args[3])
	if err != nil {
		return protocol.EncodeError("ERR stop is not an integer")
	}

	result, ok := s.store.JSONArrTrim(key, path, start, stop)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.OBJKEYS key [path]
func (s *Server) handleJSONObjKeys(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.OBJKEYS' command")
	}

	key := args[0]
	path := "$"
	if len(args) > 1 {
		path = args[1]
	}

	keys, ok := s.store.JSONObjKeys(key, path)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeStringArray(keys)
}

// JSON.OBJLEN key [path]
func (s *Server) handleJSONObjLen(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.OBJLEN' command")
	}

	key := args[0]
	path := "$"
	if len(args) > 1 {
		path = args[1]
	}

	result, ok := s.store.JSONObjLen(key, path)
	if !ok {
		return protocol.EncodeNull()
	}

	return protocol.EncodeInteger(result)
}

// JSON.MGET key [key ...] path
func (s *Server) handleJSONMGet(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.MGET' command")
	}

	// Last argument is the path
	path := args[len(args)-1]
	keys := args[:len(args)-1]

	results := make([]string, len(keys))
	for i, key := range keys {
		result, ok := s.store.JSONGet(key, path)
		if !ok {
			results[i] = ""
		} else {
			jsonBytes, err := json.Marshal(result)
			if err != nil {
				results[i] = ""
			} else {
				results[i] = string(jsonBytes)
			}
		}
	}

	// Encode as array with null for missing values
	var response strings.Builder
	response.WriteString(fmt.Sprintf("*%d\r\n", len(results)))
	for _, r := range results {
		if r == "" {
			response.WriteString("$-1\r\n")
		} else {
			response.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(r), r))
		}
	}
	return response.String()
}

// JSON.RESP key [path]
func (s *Server) handleJSONResp(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'JSON.RESP' command")
	}

	key := args[0]
	path := "$"
	if len(args) > 1 {
		path = args[1]
	}

	result, ok := s.store.JSONGet(key, path)
	if !ok {
		return protocol.EncodeNull()
	}

	return encodeJSONAsRESP(result)
}

// encodeJSONAsRESP converts a JSON value to RESP format
func encodeJSONAsRESP(v interface{}) string {
	if v == nil {
		return protocol.EncodeNull()
	}

	switch val := v.(type) {
	case bool:
		if val {
			return protocol.EncodeBulkString("true")
		}
		return protocol.EncodeBulkString("false")
	case float64:
		return protocol.EncodeBulkString(strconv.FormatFloat(val, 'f', -1, 64))
	case string:
		return protocol.EncodeBulkString(val)
	case []interface{}:
		var response strings.Builder
		response.WriteString(fmt.Sprintf("*%d\r\n", len(val)+1))
		response.WriteString("$1\r\n[\r\n")
		for _, item := range val {
			response.WriteString(encodeJSONAsRESP(item))
		}
		return response.String()
	case map[string]interface{}:
		var response strings.Builder
		response.WriteString(fmt.Sprintf("*%d\r\n", len(val)*2+1))
		response.WriteString("$1\r\n{\r\n")
		for k, item := range val {
			response.WriteString(protocol.EncodeBulkString(k))
			response.WriteString(encodeJSONAsRESP(item))
		}
		return response.String()
	default:
		return protocol.EncodeNull()
	}
}

