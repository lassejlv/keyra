package store

import (
	"strconv"
)

func (s *Store) HSet(key, field, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	redisValue, exists := db.data[key]
	if !exists {
		hashMap := make(map[string]string)
		hashMap[field] = value
		db.data[key] = HashValue(hashMap)
		return true
	} else if redisValue.Type != HashType {
		return false
	}
	
	hash := redisValue.Hash()
	_, fieldExists := hash[field]
	newHash := make(map[string]string)
	for k, v := range hash {
		newHash[k] = v
	}
	newHash[field] = value
	db.data[key] = HashValue(newHash)
	return !fieldExists
}

func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return "", false
	}
	
	fieldValue, fieldExists := value.Hash()[field]
	return fieldValue, fieldExists
}

func (s *Store) HDel(key string, fields ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return 0
	}
	
	hash := value.Hash()
	count := 0
	newHash := make(map[string]string)
	for k, v := range hash {
		newHash[k] = v
	}
	
	for _, field := range fields {
		if _, exists := newHash[field]; exists {
			delete(newHash, field)
			count++
		}
	}
	
	if len(newHash) == 0 {
		delete(db.data, key)
	} else {
		db.data[key] = HashValue(newHash)
	}
	
	return count
}

func (s *Store) HExists(key, field string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return false
	}
	
	_, fieldExists := value.Hash()[field]
	return fieldExists
}

func (s *Store) HLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return 0
	}
	
	return len(value.Hash())
}

func (s *Store) HKeys(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return []string{}
	}
	
	hash := value.Hash()
	keys := make([]string, 0, len(hash))
	for k := range hash {
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) HVals(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return []string{}
	}
	
	hash := value.Hash()
	values := make([]string, 0, len(hash))
	for _, v := range hash {
		values = append(values, v)
	}
	return values
}

func (s *Store) HGetAll(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return make(map[string]string)
	}
	
	hash := value.Hash()
	result := make(map[string]string)
	for k, v := range hash {
		result[k] = v
	}
	return result
}

func (s *Store) HIncrBy(key, field string, increment int) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		newHash := make(map[string]string)
		newHash[field] = strconv.Itoa(increment)
		db.data[key] = HashValue(newHash)
		return increment, true
	} else if value.Type != HashType {
		return 0, false
	}
	
	hash := value.Hash()
	currentStr, fieldExists := hash[field]
	current := 0
	
	if fieldExists {
		var err error
		current, err = strconv.Atoi(currentStr)
		if err != nil {
			return 0, false
		}
	}
	
	newValue := current + increment
	newHash := make(map[string]string)
	for k, v := range hash {
		newHash[k] = v
	}
	newHash[field] = strconv.Itoa(newValue)
	db.data[key] = HashValue(newHash)
	
	return newValue, true
}

func (s *Store) HIncrByFloat(key, field string, increment float64) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		newHash := make(map[string]string)
		newHash[field] = strconv.FormatFloat(increment, 'f', -1, 64)
		db.data[key] = HashValue(newHash)
		return increment, true
	} else if value.Type != HashType {
		return 0, false
	}
	
	hash := value.Hash()
	currentStr, fieldExists := hash[field]
	current := 0.0
	
	if fieldExists {
		var err error
		current, err = strconv.ParseFloat(currentStr, 64)
		if err != nil {
			return 0, false
		}
	}
	
	newValue := current + increment
	newHash := make(map[string]string)
	for k, v := range hash {
		newHash[k] = v
	}
	newHash[field] = strconv.FormatFloat(newValue, 'f', -1, 64)
	db.data[key] = HashValue(newHash)
	
	return newValue, true
}

func (s *Store) HMSet(key string, fieldValues map[string]string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	var hash map[string]string
	
	if !exists {
		hash = make(map[string]string)
	} else if value.Type != HashType {
		return false
	} else {
		existingHash := value.Hash()
		hash = make(map[string]string)
		for k, v := range existingHash {
			hash[k] = v
		}
	}
	
	for field, val := range fieldValues {
		hash[field] = val
	}
	
	db.data[key] = HashValue(hash)
	return true
}

func (s *Store) HMGet(key string, fields ...string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		result := make([]string, len(fields))
		return result
	}
	
	hash := value.Hash()
	result := make([]string, len(fields))
	for i, field := range fields {
		if val, ok := hash[field]; ok {
			result[i] = val
		}
	}
	
	return result
}

func (s *Store) HSetNX(key, field, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	redisValue, exists := db.data[key]
	if !exists {
		newHash := make(map[string]string)
		newHash[field] = value
		db.data[key] = HashValue(newHash)
		return true
	} else if redisValue.Type != HashType {
		return false
	}
	
	hash := redisValue.Hash()
	if _, fieldExists := hash[field]; fieldExists {
		return false
	}
	
	newHash := make(map[string]string)
	for k, v := range hash {
		newHash[k] = v
	}
	newHash[field] = value
	db.data[key] = HashValue(newHash)
	return true
}