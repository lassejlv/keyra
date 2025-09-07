package store

import (
	"math/rand"
)

func (s *Store) SAdd(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	var set map[string]bool
	
	if !exists {
		set = make(map[string]bool)
	} else if value.Type != SetType {
		return 0
	} else {
		existingSet := value.Set()
		set = make(map[string]bool)
		for k, v := range existingSet {
			set[k] = v
		}
	}
	
	count := 0
	for _, member := range members {
		if !set[member] {
			set[member] = true
			count++
		}
	}
	
	db.data[key] = SetValue(set)
	return count
}

func (s *Store) SRem(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType {
		return 0
	}
	
	set := value.Set()
	newSet := make(map[string]bool)
	for k, v := range set {
		newSet[k] = v
	}
	
	count := 0
	for _, member := range members {
		if newSet[member] {
			delete(newSet, member)
			count++
		}
	}
	
	if len(newSet) == 0 {
		delete(db.data, key)
	} else {
		db.data[key] = SetValue(newSet)
	}
	
	return count
}

func (s *Store) SIsMember(key, member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType {
		return false
	}
	
	return value.Set()[member]
}

func (s *Store) SMembers(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType {
		return []string{}
	}
	
	set := value.Set()
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	
	return members
}

func (s *Store) SCard(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType {
		return 0
	}
	
	return len(value.Set())
}

func (s *Store) SPop(key string, count int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType {
		return []string{}
	}
	
	set := value.Set()
	if count <= 0 || len(set) == 0 {
		return []string{}
	}
	
	if count >= len(set) {
		members := make([]string, 0, len(set))
		for member := range set {
			members = append(members, member)
		}
		delete(db.data, key)
		return members
	}
	
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	
	result := make([]string, count)
	newSet := make(map[string]bool)
	for k, v := range set {
		newSet[k] = v
	}
	
	for i := 0; i < count; i++ {
		idx := rand.Intn(len(members))
		result[i] = members[idx]
		delete(newSet, members[idx])
		members = append(members[:idx], members[idx+1:]...)
	}
	
	db.data[key] = SetValue(newSet)
	return result
}

func (s *Store) SRandMember(key string, count int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType {
		return []string{}
	}
	
	set := value.Set()
	if count <= 0 || len(set) == 0 {
		return []string{}
	}
	
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	
	if count >= len(members) {
		return members
	}
	
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = members[rand.Intn(len(members))]
	}
	
	return result
}

func (s *Store) SInter(keys ...string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db := s.getCurrentDB()
	
	if len(keys) == 0 {
		return []string{}
	}
	
	firstValue, exists := db.data[keys[0]]
	if !exists || firstValue.Type != SetType {
		return []string{}
	}
	
	result := make(map[string]bool)
	for member := range firstValue.Set() {
		result[member] = true
	}
	
	for _, key := range keys[1:] {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if !exists || value.Type != SetType {
			return []string{}
		}
		
		set := value.Set()
		for member := range result {
			if !set[member] {
				delete(result, member)
			}
		}
		
		if len(result) == 0 {
			break
		}
	}
	
	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	
	return members
}

func (s *Store) SUnion(keys ...string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db := s.getCurrentDB()
	
	result := make(map[string]bool)
	
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == SetType {
			for member := range value.Set() {
				result[member] = true
			}
		}
	}
	
	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	
	return members
}

func (s *Store) SDiff(keys ...string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db := s.getCurrentDB()
	
	if len(keys) == 0 {
		return []string{}
	}
	
	firstValue, exists := db.data[keys[0]]
	if !exists || firstValue.Type != SetType {
		return []string{}
	}
	
	result := make(map[string]bool)
	for member := range firstValue.Set() {
		result[member] = true
	}
	
	for _, key := range keys[1:] {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == SetType {
			for member := range value.Set() {
				delete(result, member)
			}
		}
	}
	
	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	
	return members
}

func (s *Store) SInterStore(destination string, keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	members := s.SInter(keys...)
	newSet := make(map[string]bool)
	for _, member := range members {
		newSet[member] = true
	}
	
	db.data[destination] = SetValue(newSet)
	return len(newSet)
}

func (s *Store) SUnionStore(destination string, keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	members := s.SUnion(keys...)
	newSet := make(map[string]bool)
	for _, member := range members {
		newSet[member] = true
	}
	
	db.data[destination] = SetValue(newSet)
	return len(newSet)
}

func (s *Store) SDiffStore(destination string, keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	members := s.SDiff(keys...)
	newSet := make(map[string]bool)
	for _, member := range members {
		newSet[member] = true
	}
	
	db.data[destination] = SetValue(newSet)
	return len(newSet)
}

func (s *Store) SMove(source, destination, member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	sourceValue, exists := db.data[source]
	if !exists || sourceValue.Type != SetType {
		return false
	}
	
	sourceSet := sourceValue.Set()
	if !sourceSet[member] {
		return false
	}
	
	newSourceSet := make(map[string]bool)
	for k, v := range sourceSet {
		newSourceSet[k] = v
	}
	delete(newSourceSet, member)
	
	if len(newSourceSet) == 0 {
		delete(db.data, source)
	} else {
		db.data[source] = SetValue(newSourceSet)
	}
	
	destValue, destExists := db.data[destination]
	var destSet map[string]bool
	
	if !destExists || destValue.Type != SetType {
		destSet = make(map[string]bool)
	} else {
		existingDestSet := destValue.Set()
		destSet = make(map[string]bool)
		for k, v := range existingDestSet {
			destSet[k] = v
		}
	}
	
	destSet[member] = true
	db.data[destination] = SetValue(destSet)
	
	return true
}