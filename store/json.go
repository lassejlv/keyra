package store

import (
	"encoding/json"
	"strconv"
	"strings"
)

// JSONSet sets a JSON value at the specified path
// If path is "$" or ".", sets the root value
func (s *Store) JSONSet(key, path string, value interface{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	// Normalize path
	path = normalizePath(path)

	if path == "$" || path == "." {
		// Setting root value
		db.data[key] = JSONValue(value)
		return true
	}

	// Get existing value or create new object
	existing, exists := db.data[key]
	var root interface{}
	if exists && existing.Type == JSONType {
		root = deepCopy(existing.JSON())
	} else if !exists {
		root = make(map[string]interface{})
	} else {
		return false // Wrong type
	}

	// Set value at path
	if setAtPath(root, path, value) {
		db.data[key] = JSONValue(root)
		return true
	}
	return false
}

// JSONGet gets a JSON value at the specified path
func (s *Store) JSONGet(key string, paths ...string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return nil, false
	}

	root := value.JSON()

	if len(paths) == 0 || (len(paths) == 1 && (paths[0] == "$" || paths[0] == ".")) {
		return root, true
	}

	// Handle multiple paths
	if len(paths) == 1 {
		path := normalizePath(paths[0])
		return getAtPath(root, path)
	}

	// Multiple paths - return map of results
	results := make(map[string]interface{})
	for _, path := range paths {
		path = normalizePath(path)
		if val, ok := getAtPath(root, path); ok {
			results[path] = val
		}
	}
	return results, true
}

// JSONDel deletes a value at the specified path
func (s *Store) JSONDel(key string, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0
	}

	path = normalizePath(path)

	if path == "$" || path == "." {
		delete(db.data, key)
		return 1
	}

	root := deepCopy(value.JSON())
	if deleteAtPath(root, path) {
		db.data[key] = JSONValue(root)
		return 1
	}
	return 0
}

// JSONType returns the type of the value at the path
func (s *Store) JSONType(key string, path string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return ""
	}

	path = normalizePath(path)
	val, ok := getAtPath(value.JSON(), path)
	if !ok {
		return ""
	}

	return getJSONType(val)
}

// JSONNumIncrBy increments a number at the path
func (s *Store) JSONNumIncrBy(key, path string, increment float64) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	root := deepCopy(value.JSON())

	val, ok := getAtPath(root, path)
	if !ok {
		return 0, false
	}

	num, ok := toFloat64(val)
	if !ok {
		return 0, false
	}

	newVal := num + increment
	if setAtPath(root, path, newVal) {
		db.data[key] = JSONValue(root)
		return newVal, true
	}
	return 0, false
}

// JSONNumMultBy multiplies a number at the path
func (s *Store) JSONNumMultBy(key, path string, multiplier float64) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	root := deepCopy(value.JSON())

	val, ok := getAtPath(root, path)
	if !ok {
		return 0, false
	}

	num, ok := toFloat64(val)
	if !ok {
		return 0, false
	}

	newVal := num * multiplier
	if setAtPath(root, path, newVal) {
		db.data[key] = JSONValue(root)
		return newVal, true
	}
	return 0, false
}

// JSONStrAppend appends to a string at the path
func (s *Store) JSONStrAppend(key, path, appendStr string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	root := deepCopy(value.JSON())

	val, ok := getAtPath(root, path)
	if !ok {
		return 0, false
	}

	str, ok := val.(string)
	if !ok {
		return 0, false
	}

	newStr := str + appendStr
	if setAtPath(root, path, newStr) {
		db.data[key] = JSONValue(root)
		return len(newStr), true
	}
	return 0, false
}

// JSONStrLen returns the length of a string at the path
func (s *Store) JSONStrLen(key, path string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	val, ok := getAtPath(value.JSON(), path)
	if !ok {
		return 0, false
	}

	str, ok := val.(string)
	if !ok {
		return 0, false
	}

	return len(str), true
}

// JSONArrAppend appends values to an array at the path
func (s *Store) JSONArrAppend(key, path string, values ...interface{}) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	root := deepCopy(value.JSON())

	val, ok := getAtPath(root, path)
	if !ok {
		return 0, false
	}

	arr, ok := val.([]interface{})
	if !ok {
		return 0, false
	}

	arr = append(arr, values...)
	if setAtPath(root, path, arr) {
		db.data[key] = JSONValue(root)
		return len(arr), true
	}
	return 0, false
}

// JSONArrLen returns the length of an array at the path
func (s *Store) JSONArrLen(key, path string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	val, ok := getAtPath(value.JSON(), path)
	if !ok {
		return 0, false
	}

	arr, ok := val.([]interface{})
	if !ok {
		return 0, false
	}

	return len(arr), true
}

// JSONArrPop removes and returns an element from an array
func (s *Store) JSONArrPop(key, path string, index int) (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return nil, false
	}

	path = normalizePath(path)
	root := deepCopy(value.JSON())

	val, ok := getAtPath(root, path)
	if !ok {
		return nil, false
	}

	arr, ok := val.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, false
	}

	// Handle negative index
	if index < 0 {
		index = len(arr) + index
	}
	if index < 0 || index >= len(arr) {
		index = len(arr) - 1 // Default to last element
	}

	popped := arr[index]
	newArr := append(arr[:index], arr[index+1:]...)

	if setAtPath(root, path, newArr) {
		db.data[key] = JSONValue(root)
		return popped, true
	}
	return nil, false
}

// JSONArrIndex finds the index of a value in an array
func (s *Store) JSONArrIndex(key, path string, value interface{}) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	val, exists := db.data[key]
	if !exists || val.Type != JSONType {
		return -1, false
	}

	path = normalizePath(path)
	arrVal, ok := getAtPath(val.JSON(), path)
	if !ok {
		return -1, false
	}

	arr, ok := arrVal.([]interface{})
	if !ok {
		return -1, false
	}

	// Compare values
	valueJSON, _ := json.Marshal(value)
	for i, item := range arr {
		itemJSON, _ := json.Marshal(item)
		if string(valueJSON) == string(itemJSON) {
			return i, true
		}
	}

	return -1, true
}

// JSONArrInsert inserts values at an index in an array
func (s *Store) JSONArrInsert(key, path string, index int, values ...interface{}) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	root := deepCopy(value.JSON())

	val, ok := getAtPath(root, path)
	if !ok {
		return 0, false
	}

	arr, ok := val.([]interface{})
	if !ok {
		return 0, false
	}

	// Handle negative index
	if index < 0 {
		index = len(arr) + index + 1
	}
	if index < 0 {
		index = 0
	}
	if index > len(arr) {
		index = len(arr)
	}

	// Insert values at index
	newArr := make([]interface{}, 0, len(arr)+len(values))
	newArr = append(newArr, arr[:index]...)
	newArr = append(newArr, values...)
	newArr = append(newArr, arr[index:]...)

	if setAtPath(root, path, newArr) {
		db.data[key] = JSONValue(root)
		return len(newArr), true
	}
	return 0, false
}

// JSONArrTrim trims an array to the specified range
func (s *Store) JSONArrTrim(key, path string, start, stop int) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	root := deepCopy(value.JSON())

	val, ok := getAtPath(root, path)
	if !ok {
		return 0, false
	}

	arr, ok := val.([]interface{})
	if !ok {
		return 0, false
	}

	length := len(arr)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		newArr := []interface{}{}
		if setAtPath(root, path, newArr) {
			db.data[key] = JSONValue(root)
			return 0, true
		}
		return 0, false
	}

	newArr := arr[start : stop+1]
	if setAtPath(root, path, newArr) {
		db.data[key] = JSONValue(root)
		return len(newArr), true
	}
	return 0, false
}

// JSONObjKeys returns the keys of an object at the path
func (s *Store) JSONObjKeys(key, path string) ([]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return nil, false
	}

	path = normalizePath(path)
	val, ok := getAtPath(value.JSON(), path)
	if !ok {
		return nil, false
	}

	obj, ok := val.(map[string]interface{})
	if !ok {
		return nil, false
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	return keys, true
}

// JSONObjLen returns the number of keys in an object at the path
func (s *Store) JSONObjLen(key, path string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != JSONType {
		return 0, false
	}

	path = normalizePath(path)
	val, ok := getAtPath(value.JSON(), path)
	if !ok {
		return 0, false
	}

	obj, ok := val.(map[string]interface{})
	if !ok {
		return 0, false
	}

	return len(obj), true
}

// Helper functions

func normalizePath(path string) string {
	if path == "" || path == "." {
		return "$"
	}
	// Convert dot notation to JSONPath
	if !strings.HasPrefix(path, "$") && !strings.HasPrefix(path, ".") {
		path = "$." + path
	}
	if strings.HasPrefix(path, ".") {
		path = "$" + path
	}
	return path
}

func getAtPath(root interface{}, path string) (interface{}, bool) {
	if path == "$" || path == "." {
		return root, true
	}

	// Parse path - remove $ prefix
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")

	parts := parsePath(path)
	current := root

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Check for array index
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			indexStr := part[1 : len(part)-1]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return nil, false
			}

			arr, ok := current.([]interface{})
			if !ok {
				return nil, false
			}

			if index < 0 {
				index = len(arr) + index
			}
			if index < 0 || index >= len(arr) {
				return nil, false
			}
			current = arr[index]
		} else {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, false
			}
			val, exists := obj[part]
			if !exists {
				return nil, false
			}
			current = val
		}
	}

	return current, true
}

func setAtPath(root interface{}, path string, value interface{}) bool {
	if path == "$" || path == "." {
		return false // Can't set root this way
	}

	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")

	parts := parsePath(path)
	if len(parts) == 0 {
		return false
	}

	// Navigate to parent
	current := root
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		if part == "" {
			continue
		}

		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			indexStr := part[1 : len(part)-1]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return false
			}

			arr, ok := current.([]interface{})
			if !ok {
				return false
			}

			if index < 0 {
				index = len(arr) + index
			}
			if index < 0 || index >= len(arr) {
				return false
			}
			current = arr[index]
		} else {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return false
			}
			if _, exists := obj[part]; !exists {
				// Create intermediate object
				obj[part] = make(map[string]interface{})
			}
			current = obj[part]
		}
	}

	// Set the final value
	lastPart := parts[len(parts)-1]

	if strings.HasPrefix(lastPart, "[") && strings.HasSuffix(lastPart, "]") {
		indexStr := lastPart[1 : len(lastPart)-1]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return false
		}

		arr, ok := current.([]interface{})
		if !ok {
			return false
		}

		if index < 0 {
			index = len(arr) + index
		}
		if index < 0 || index >= len(arr) {
			return false
		}
		arr[index] = value
	} else {
		obj, ok := current.(map[string]interface{})
		if !ok {
			return false
		}
		obj[lastPart] = value
	}

	return true
}

func deleteAtPath(root interface{}, path string) bool {
	if path == "$" || path == "." {
		return false
	}

	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")

	parts := parsePath(path)
	if len(parts) == 0 {
		return false
	}

	// Navigate to parent
	current := root
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		if part == "" {
			continue
		}

		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			indexStr := part[1 : len(part)-1]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return false
			}

			arr, ok := current.([]interface{})
			if !ok {
				return false
			}

			if index < 0 {
				index = len(arr) + index
			}
			if index < 0 || index >= len(arr) {
				return false
			}
			current = arr[index]
		} else {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return false
			}
			val, exists := obj[part]
			if !exists {
				return false
			}
			current = val
		}
	}

	// Delete the final value
	lastPart := parts[len(parts)-1]

	if strings.HasPrefix(lastPart, "[") && strings.HasSuffix(lastPart, "]") {
		// Can't delete array element with this simple implementation
		return false
	}

	obj, ok := current.(map[string]interface{})
	if !ok {
		return false
	}
	if _, exists := obj[lastPart]; !exists {
		return false
	}
	delete(obj, lastPart)
	return true
}

func parsePath(path string) []string {
	var parts []string
	var current strings.Builder
	inBracket := false

	for _, ch := range path {
		switch ch {
		case '.':
			if !inBracket {
				if current.Len() > 0 {
					parts = append(parts, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(ch)
			}
		case '[':
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			current.WriteRune(ch)
			inBracket = true
		case ']':
			current.WriteRune(ch)
			parts = append(parts, current.String())
			current.Reset()
			inBracket = false
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

func deepCopy(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, v := range val {
			result[k] = deepCopy(v)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, v := range val {
			result[i] = deepCopy(v)
		}
		return result
	default:
		return v
	}
}

func getJSONType(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case bool:
		return "boolean"
	case float64, int, int64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

