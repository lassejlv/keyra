package store

import (
	"strings"
)

// List operations
func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    ListType,
			ListVal: make([]string, 0),
		}
		db.data[key] = value
	} else if value.Type != ListType {
		return -1 // Wrong type error
	}
	
	// Prepend values in reverse order to maintain order
	for i := len(values) - 1; i >= 0; i-- {
		value.ListVal = append([]string{values[i]}, value.ListVal...)
	}
	
	return len(value.ListVal)
}

func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    ListType,
			ListVal: make([]string, 0),
		}
		db.data[key] = value
	} else if value.Type != ListType {
		return -1 // Wrong type error
	}
	
	value.ListVal = append(value.ListVal, values...)
	return len(value.ListVal)
}

func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType || len(value.ListVal) == 0 {
		return "", false
	}
	
	result := value.ListVal[0]
	value.ListVal = value.ListVal[1:]
	
	if len(value.ListVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
	}
	
	return result, true
}

func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType || len(value.ListVal) == 0 {
		return "", false
	}
	
	length := len(value.ListVal)
	result := value.ListVal[length-1]
	value.ListVal = value.ListVal[:length-1]
	
	if len(value.ListVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
	}
	
	return result, true
}

func (s *Store) LLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return 0
	}
	
	return len(value.ListVal)
}

func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return []string{}
	}
	
	length := len(value.ListVal)
	if length == 0 {
		return []string{}
	}
	
	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	
	// Bounds checking
	if start < 0 {
		start = 0
	}
	if start >= length {
		return []string{}
	}
	if stop >= length {
		stop = length - 1
	}
	if stop < start {
		return []string{}
	}
	
	return value.ListVal[start : stop+1]
}

func (s *Store) LIndex(key string, index int) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return "", false
	}
	
	length := len(value.ListVal)
	if length == 0 {
		return "", false
	}
	
	// Handle negative indices
	if index < 0 {
		index = length + index
	}
	
	// Bounds checking
	if index < 0 || index >= length {
		return "", false
	}
	
	return value.ListVal[index], true
}

func (s *Store) LSet(key string, index int, element string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return false
	}
	
	length := len(value.ListVal)
	if length == 0 {
		return false
	}
	
	// Handle negative indices
	if index < 0 {
		index = length + index
	}
	
	// Bounds checking
	if index < 0 || index >= length {
		return false
	}
	
	value.ListVal[index] = element
	return true
}

func (s *Store) LTrim(key string, start, stop int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return false
	}
	
	length := len(value.ListVal)
	if length == 0 {
		return true
	}
	
	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	
	// Bounds checking
	if start < 0 {
		start = 0
	}
	if start >= length {
		// Trim to empty list
		value.ListVal = []string{}
		return true
	}
	if stop >= length {
		stop = length - 1
	}
	if stop < start {
		// Trim to empty list
		value.ListVal = []string{}
		return true
	}
	
	// Trim the list
	value.ListVal = value.ListVal[start : stop+1]
	
	// If list becomes empty, remove the key
	if len(value.ListVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
	}
	
	return true
}

func (s *Store) LInsert(key, where, pivot, element string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return 0 // Key doesn't exist or wrong type
	}
	
	// Find pivot element
	for i, val := range value.ListVal {
		if val == pivot {
			if strings.ToUpper(where) == "BEFORE" {
				// Insert before pivot
				value.ListVal = append(value.ListVal[:i], append([]string{element}, value.ListVal[i:]...)...)
			} else if strings.ToUpper(where) == "AFTER" {
				// Insert after pivot
				if i+1 >= len(value.ListVal) {
					value.ListVal = append(value.ListVal, element)
				} else {
					value.ListVal = append(value.ListVal[:i+1], append([]string{element}, value.ListVal[i+1:]...)...)
				}
			} else {
				return -1 // Invalid where parameter
			}
			return len(value.ListVal)
		}
	}
	
	return -1 // Pivot not found
}

// Basic implementation of blocking operations (non-blocking for now)
// TODO: Implement true blocking with client connection state management
func (s *Store) BLPop(keys []string, timeout int) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	// Try each key in order
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == ListType && len(value.ListVal) > 0 {
			result := value.ListVal[0]
			value.ListVal = value.ListVal[1:]
			
			if len(value.ListVal) == 0 {
				delete(db.data, key)
				delete(db.expiration, key)
			}
			
			return key, result, true
		}
	}
	
	// No data available (in a real implementation, this would block)
	return "", "", false
}

func (s *Store) BRPop(keys []string, timeout int) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	// Try each key in order
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == ListType && len(value.ListVal) > 0 {
			length := len(value.ListVal)
			result := value.ListVal[length-1]
			value.ListVal = value.ListVal[:length-1]
			
			if len(value.ListVal) == 0 {
				delete(db.data, key)
				delete(db.expiration, key)
			}
			
			return key, result, true
		}
	}
	
	// No data available (in a real implementation, this would block)
	return "", "", false
}
