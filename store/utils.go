package store

import (
	"math/rand"
	"strings"
)

func (s *Store) RandomKey() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db := s.getCurrentDB()
	
	if len(db.data) == 0 {
		return ""
	}
	
	// Get a random key
	keys := make([]string, 0, len(db.data))
	for key := range db.data {
		s.cleanupExpired(key)
		if _, exists := db.data[key]; exists {
			keys = append(keys, key)
		}
	}
	
	if len(keys) == 0 {
		return ""
	}
	
	return keys[rand.Intn(len(keys))]
}

func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		return pattern == key
	}
	
	return simpleGlobMatch(pattern, key)
}

func simpleGlobMatch(pattern, str string) bool {
	if pattern == "" {
		return str == ""
	}
	if pattern == "*" {
		return true
	}
	
	if len(pattern) > 0 && pattern[0] == '*' {
		for i := 0; i <= len(str); i++ {
			if simpleGlobMatch(pattern[1:], str[i:]) {
				return true
			}
		}
		return false
	}
	
	if len(str) == 0 {
		return false
	}
	
	if len(pattern) > 0 && (pattern[0] == '?' || pattern[0] == str[0]) {
		return simpleGlobMatch(pattern[1:], str[1:])
	}
	
	return false
}
