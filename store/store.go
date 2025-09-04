package store

import (
	"redis-go-clone/persistence"
	"strings"
	"sync"
	"time"
)

type Store struct {
	data        map[string]string
	expiration  map[string]time.Time
	mu          sync.RWMutex
	persistence *persistence.Persistence
}

func New(persistenceFile string) *Store {
	s := &Store{
		data:        make(map[string]string),
		expiration:  make(map[string]time.Time),
		persistence: persistence.New(persistenceFile),
	}
	s.Load()
	return s
}

func NewInMemory() *Store {
	return &Store{
		data:       make(map[string]string),
		expiration: make(map[string]time.Time),
	}
}

func (s *Store) isExpired(key string) bool {
	if expTime, exists := s.expiration[key]; exists {
		return time.Now().After(expTime)
	}
	return false
}

func (s *Store) cleanupExpired(key string) {
	if s.isExpired(key) {
		delete(s.data, key)
		delete(s.expiration, key)
	}
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	delete(s.expiration, key)
}

func (s *Store) SetWithExpiration(key, value string, expiration time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.expiration[key] = expiration
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
		delete(s.expiration, key)
	}
	return exists
}

func (s *Store) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; !exists {
		return false
	}
	s.expiration[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	return true
}

func (s *Store) ExpireAt(key string, timestamp int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; !exists {
		return false
	}
	s.expiration[key] = time.Unix(timestamp, 0)
	return true
}

func (s *Store) TTL(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	if _, exists := s.data[key]; !exists {
		return -2
	}
	if expTime, hasExpiration := s.expiration[key]; hasExpiration {
		remaining := int(time.Until(expTime).Seconds())
		if remaining < 0 {
			return -2
		}
		return remaining
	}
	return -1
}

func (s *Store) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.persistence == nil {
		return nil
	}
	
	dataCopy := make(map[string]string)
	for k, v := range s.data {
		dataCopy[k] = v
	}
	
	expCopy := make(map[string]time.Time)
	for k, v := range s.expiration {
		expCopy[k] = v
	}
	
	return s.persistence.Save(dataCopy, expCopy)
}

func (s *Store) Load() error {
	if s.persistence == nil {
		return nil
	}
	
	data, expiration, err := s.persistence.Load()
	if err != nil {
		return err
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
	s.expiration = expiration
	return nil
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	_, exists := s.data[key]
	return exists
}

func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var keys []string
	for key := range s.data {
		s.cleanupExpired(key)
		if _, exists := s.data[key]; exists {
			if pattern == "*" || matchPattern(pattern, key) {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

func (s *Store) DBSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	count := 0
	for key := range s.data {
		s.cleanupExpired(key)
		if _, exists := s.data[key]; exists {
			count++
		}
	}
	return count
}

func (s *Store) FlushDB() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string)
	s.expiration = make(map[string]time.Time)
}

func (s *Store) Append(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	
	if existing, exists := s.data[key]; exists {
		s.data[key] = existing + value
	} else {
		s.data[key] = value
	}
	return len(s.data[key])
}

func (s *Store) GetRange(key string, start, end int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	
	value, exists := s.data[key]
	if !exists {
		return ""
	}
	
	length := len(value)
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
	
	return value[start : end+1]
}

func (s *Store) PExpire(key string, milliseconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; !exists {
		return false
	}
	s.expiration[key] = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
	return true
}

func (s *Store) PExpireAt(key string, timestampMs int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; !exists {
		return false
	}
	s.expiration[key] = time.Unix(0, timestampMs*int64(time.Millisecond))
	return true
}

func (s *Store) PTTL(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	if _, exists := s.data[key]; !exists {
		return -2
	}
	if expTime, hasExpiration := s.expiration[key]; hasExpiration {
		remaining := int(time.Until(expTime).Milliseconds())
		if remaining < 0 {
			return -2
		}
		return remaining
	}
	return -1
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
