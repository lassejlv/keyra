package store

import (
	"strings"
)

func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		db.data[key] = ListValue(values)
		return len(values)
	} else if value.Type != ListType {
		return -1
	}
	
	oldList := value.List()
	newList := make([]string, 0, len(oldList)+len(values))
	for i := len(values) - 1; i >= 0; i-- {
		newList = append(newList, values[i])
	}
	newList = append(newList, oldList...)
	db.data[key] = ListValue(newList)
	
	return len(newList)
}

func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		db.data[key] = ListValue(values)
		return len(values)
	} else if value.Type != ListType {
		return -1
	}
	
	oldList := value.List()
	newList := append(oldList, values...)
	db.data[key] = ListValue(newList)
	return len(newList)
}

func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return "", false
	}
	
	list := value.List()
	if len(list) == 0 {
		return "", false
	}
	
	result := list[0]
	newList := list[1:]
	if len(newList) == 0 {
		delete(db.data, key)
	} else {
		db.data[key] = ListValue(newList)
	}
	
	return result, true
}

func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return "", false
	}
	
	list := value.List()
	if len(list) == 0 {
		return "", false
	}
	
	lastIndex := len(list) - 1
	result := list[lastIndex]
	newList := list[:lastIndex]
	if len(newList) == 0 {
		delete(db.data, key)
	} else {
		db.data[key] = ListValue(newList)
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
	
	return len(value.List())
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
	
	list := value.List()
	length := len(list)
	
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	
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
	
	return list[start : stop+1]
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
	
	list := value.List()
	length := len(list)
	
	if index < 0 {
		index = length + index
	}
	
	if index < 0 || index >= length {
		return "", false
	}
	
	return list[index], true
}

func (s *Store) LSet(key string, index int, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	redisValue, exists := db.data[key]
	if !exists || redisValue.Type != ListType {
		return false
	}
	
	list := redisValue.List()
	length := len(list)
	
	if index < 0 {
		index = length + index
	}
	
	if index < 0 || index >= length {
		return false
	}
	
	newList := make([]string, len(list))
	copy(newList, list)
	newList[index] = value
	db.data[key] = ListValue(newList)
	return true
}

func (s *Store) LTrim(key string, start, stop int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return false
	}
	
	list := value.List()
	length := len(list)
	
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	
	if start < 0 {
		start = 0
	}
	if start >= length || stop < start {
		delete(db.data, key)
		return true
	}
	if stop >= length {
		stop = length - 1
	}
	
	newList := list[start : stop+1]
	db.data[key] = ListValue(newList)
	return true
}

func (s *Store) LInsert(key, where, pivot, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	redisValue, exists := db.data[key]
	if !exists || redisValue.Type != ListType {
		return 0
	}
	
	list := redisValue.List()
	
	for i, element := range list {
		if element == pivot {
			newList := make([]string, 0, len(list)+1)
			if strings.ToUpper(where) == "BEFORE" {
				newList = append(newList, list[:i]...)
				newList = append(newList, value)
				newList = append(newList, list[i:]...)
			} else {
				newList = append(newList, list[:i+1]...)
				newList = append(newList, value)
				newList = append(newList, list[i+1:]...)
			}
			db.data[key] = ListValue(newList)
			return len(newList)
		}
	}
	
	return -1
}

func (s *Store) BLPop(keys []string, timeout int) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == ListType {
			list := value.List()
			if len(list) > 0 {
				result := list[0]
				newList := list[1:]
				if len(newList) == 0 {
					delete(db.data, key)
				} else {
					db.data[key] = ListValue(newList)
				}
				return key, result, true
			}
		}
	}
	
	return "", "", false
}

func (s *Store) BRPop(keys []string, timeout int) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == ListType {
			list := value.List()
			if len(list) > 0 {
				lastIndex := len(list) - 1
				result := list[lastIndex]
				newList := list[:lastIndex]
				if len(newList) == 0 {
					delete(db.data, key)
				} else {
					db.data[key] = ListValue(newList)
				}
				return key, result, true
			}
		}
	}
	
	return "", "", false
}