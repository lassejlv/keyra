package store

import (
	"time"
)

func (s *Store) SetWithExpiration(key, value string, expiration time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	db.data[key] = &RedisValue{
		Type:      StringType,
		StringVal: value,
	}
	db.expiration[key] = expiration
}

func (s *Store) Append(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	if existing, exists := db.data[key]; exists && existing.Type == StringType {
		existing.StringVal += value
		return len(existing.StringVal)
	} else {
		db.data[key] = &RedisValue{
			Type:      StringType,
			StringVal: value,
		}
		return len(value)
	}
}

func (s *Store) GetRange(key string, start, end int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != StringType {
		return ""
	}
	
	length := len(value.StringVal)
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return ""
	}
	
	return value.StringVal[start : end+1]
}
