package store

import (
	"time"
)

// Database management methods
func (s *Store) SelectDB(dbIndex int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dbIndex < 0 || dbIndex >= 16 {
		return false
	}
	s.currentDB = dbIndex
	return true
}

func (s *Store) GetCurrentDBIndex() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentDB
}

func (s *Store) Move(key string, destDB int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if destDB < 0 || destDB >= 16 || destDB == s.currentDB {
		return false
	}
	
	sourceDB := s.getCurrentDB()
	targetDB := s.databases[destDB]
	
	value, exists := sourceDB.data[key]
	if !exists {
		return false
	}
	
	// Check if key already exists in destination
	if _, exists := targetDB.data[key]; exists {
		return false
	}
	
	// Move the key
	targetDB.data[key] = value
	if expTime, hasExp := sourceDB.expiration[key]; hasExp {
		targetDB.expiration[key] = expTime
	}
	
	// Remove from source
	delete(sourceDB.data, key)
	delete(sourceDB.expiration, key)
	
	return true
}

func (s *Store) SwapDB(db1, db2 int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if db1 < 0 || db1 >= 16 || db2 < 0 || db2 >= 16 {
		return false
	}
	
	s.databases[db1], s.databases[db2] = s.databases[db2], s.databases[db1]
	return true
}

func (s *Store) GetDBInfo() map[int]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	dbInfo := make(map[int]int)
	for i := 0; i < 16; i++ {
		db := s.databases[i]
		count := 0
		for key := range db.data {
			// Quick expiration check without cleanup
			if expTime, exists := db.expiration[key]; exists {
				if time.Now().After(expTime) {
					continue // Skip expired keys
				}
			}
			count++
		}
		if count > 0 {
			dbInfo[i] = count
		}
	}
	return dbInfo
}
