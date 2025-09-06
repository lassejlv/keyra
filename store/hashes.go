package store

import (
	"strconv"
)

// Hash operations
func (s *Store) HSet(key, field, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	redisValue, exists := db.data[key]
	if !exists {
		redisValue = &RedisValue{
			Type:    HashType,
			HashVal: make(map[string]string),
		}
		db.data[key] = redisValue
	} else if redisValue.Type != HashType {
		return false // Wrong type error
	}
	
	_, fieldExists := redisValue.HashVal[field]
	redisValue.HashVal[field] = value
	return !fieldExists // Return true if it's a new field
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
	
	fieldValue, fieldExists := value.HashVal[field]
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
	
	count := 0
	for _, field := range fields {
		if _, exists := value.HashVal[field]; exists {
			delete(value.HashVal, field)
			count++
		}
	}
	
	if len(value.HashVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
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
	
	_, fieldExists := value.HashVal[field]
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
	
	return len(value.HashVal)
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
	
	keys := make([]string, 0, len(value.HashVal))
	for k := range value.HashVal {
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
	
	vals := make([]string, 0, len(value.HashVal))
	for _, v := range value.HashVal {
		vals = append(vals, v)
	}
	return vals
}

func (s *Store) HGetAll(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return map[string]string{}
	}
	
	result := make(map[string]string)
	for k, v := range value.HashVal {
		result[k] = v
	}
	return result
}

func (s *Store) HIncrBy(key, field string, increment int64) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    HashType,
			HashVal: make(map[string]string),
		}
		db.data[key] = value
	} else if value.Type != HashType {
		return 0, false // Wrong type error
	}
	
	currentStr, fieldExists := value.HashVal[field]
	var current int64 = 0
	if fieldExists {
		var err error
		current, err = strconv.ParseInt(currentStr, 10, 64)
		if err != nil {
			return 0, false // Not an integer
		}
	}
	
	newValue := current + increment
	value.HashVal[field] = strconv.FormatInt(newValue, 10)
	return newValue, true
}

func (s *Store) HIncrByFloat(key, field string, increment float64) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    HashType,
			HashVal: make(map[string]string),
		}
		db.data[key] = value
	} else if value.Type != HashType {
		return 0, false // Wrong type error
	}
	
	currentStr, fieldExists := value.HashVal[field]
	var current float64 = 0
	if fieldExists {
		var err error
		current, err = strconv.ParseFloat(currentStr, 64)
		if err != nil {
			return 0, false // Not a number
		}
	}
	
	newValue := current + increment
	value.HashVal[field] = strconv.FormatFloat(newValue, 'g', -1, 64)
	return newValue, true
}

func (s *Store) HMSet(key string, fieldValuePairs []string) bool {
	if len(fieldValuePairs)%2 != 0 {
		return false // Must have even number of arguments
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    HashType,
			HashVal: make(map[string]string),
		}
		db.data[key] = value
	} else if value.Type != HashType {
		return false // Wrong type error
	}
	
	for i := 0; i < len(fieldValuePairs); i += 2 {
		field := fieldValuePairs[i]
		val := fieldValuePairs[i+1]
		value.HashVal[field] = val
	}
	
	return true
}

func (s *Store) HMGet(key string, fields []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		// Return array of nils
		result := make([]string, len(fields))
		for i := range result {
			result[i] = "" // Will be encoded as nil
		}
		return result
	}
	
	result := make([]string, len(fields))
	for i, field := range fields {
		if val, exists := value.HashVal[field]; exists {
			result[i] = val
		} else {
			result[i] = "" // Will be encoded as nil
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
		redisValue = &RedisValue{
			Type:    HashType,
			HashVal: make(map[string]string),
		}
		db.data[key] = redisValue
	} else if redisValue.Type != HashType {
		return false // Wrong type error
	}
	
	if _, fieldExists := redisValue.HashVal[field]; fieldExists {
		return false // Field already exists
	}
	
	redisValue.HashVal[field] = value
	return true
}
