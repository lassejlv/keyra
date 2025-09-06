package store

import (
	"time"
)

func (s *Store) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	if _, exists := db.data[key]; !exists {
		return false
	}
	db.expiration[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	return true
}

func (s *Store) ExpireAt(key string, timestamp int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	if _, exists := db.data[key]; !exists {
		return false
	}
	db.expiration[key] = time.Unix(timestamp, 0)
	return true
}

func (s *Store) TTL(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	if _, exists := db.data[key]; !exists {
		return -2
	}
	if expTime, hasExpiration := db.expiration[key]; hasExpiration {
		remaining := int(time.Until(expTime).Seconds())
		if remaining < 0 {
			return -2
		}
		return remaining
	}
	return -1
}

func (s *Store) PExpire(key string, milliseconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	if _, exists := db.data[key]; !exists {
		return false
	}
	db.expiration[key] = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
	return true
}

func (s *Store) PExpireAt(key string, timestampMs int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	if _, exists := db.data[key]; !exists {
		return false
	}
	db.expiration[key] = time.Unix(0, timestampMs*int64(time.Millisecond))
	return true
}

func (s *Store) PTTL(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	if _, exists := db.data[key]; !exists {
		return -2
	}
	if expTime, hasExpiration := db.expiration[key]; hasExpiration {
		remaining := int(time.Until(expTime).Milliseconds())
		if remaining < 0 {
			return -2
		}
		return remaining
	}
	return -1
}
