package store

// Set operations
func (s *Store) SAdd(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:   SetType,
			SetVal: make(map[string]bool),
		}
		db.data[key] = value
	} else if value.Type != SetType {
		return 0 // Wrong type error
	}
	
	count := 0
	for _, member := range members {
		if !value.SetVal[member] {
			value.SetVal[member] = true
			count++
		}
	}
	
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
	
	count := 0
	for _, member := range members {
		if value.SetVal[member] {
			delete(value.SetVal, member)
			count++
		}
	}
	
	if len(value.SetVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
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
	
	return value.SetVal[member]
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
	
	members := make([]string, 0, len(value.SetVal))
	for member := range value.SetVal {
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
	
	return len(value.SetVal)
}

func (s *Store) SPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType || len(value.SetVal) == 0 {
		return "", false
	}
	
	// Get a random member
	for member := range value.SetVal {
		delete(value.SetVal, member)
		
		if len(value.SetVal) == 0 {
			delete(db.data, key)
			delete(db.expiration, key)
		}
		
		return member, true
	}
	
	return "", false
}

func (s *Store) SRandMember(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != SetType || len(value.SetVal) == 0 {
		return "", false
	}
	
	// Get a random member without removing it
	for member := range value.SetVal {
		return member, true
	}
	
	return "", false
}

func (s *Store) SInter(keys []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if len(keys) == 0 {
		return []string{}
	}
	
	db := s.getCurrentDB()
	
	// Start with first set
	s.cleanupExpired(keys[0])
	firstValue, exists := db.data[keys[0]]
	if !exists || firstValue.Type != SetType {
		return []string{}
	}
	
	// Create result set from first set
	result := make(map[string]bool)
	for member := range firstValue.SetVal {
		result[member] = true
	}
	
	// Intersect with remaining sets
	for i := 1; i < len(keys); i++ {
		s.cleanupExpired(keys[i])
		value, exists := db.data[keys[i]]
		if !exists || value.Type != SetType {
			return []string{} // Empty intersection
		}
		
		// Keep only members that exist in this set
		for member := range result {
			if !value.SetVal[member] {
				delete(result, member)
			}
		}
		
		// If result is empty, no point continuing
		if len(result) == 0 {
			break
		}
	}
	
	// Convert to slice
	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	return members
}

func (s *Store) SUnion(keys []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if len(keys) == 0 {
		return []string{}
	}
	
	db := s.getCurrentDB()
	result := make(map[string]bool)
	
	// Union all sets
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == SetType {
			for member := range value.SetVal {
				result[member] = true
			}
		}
	}
	
	// Convert to slice
	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	return members
}

func (s *Store) SDiff(keys []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if len(keys) == 0 {
		return []string{}
	}
	
	db := s.getCurrentDB()
	
	// Start with first set
	s.cleanupExpired(keys[0])
	firstValue, exists := db.data[keys[0]]
	if !exists || firstValue.Type != SetType {
		return []string{}
	}
	
	// Create result set from first set
	result := make(map[string]bool)
	for member := range firstValue.SetVal {
		result[member] = true
	}
	
	// Remove members that exist in other sets
	for i := 1; i < len(keys); i++ {
		s.cleanupExpired(keys[i])
		value, exists := db.data[keys[i]]
		if exists && value.Type == SetType {
			for member := range value.SetVal {
				delete(result, member)
			}
		}
	}
	
	// Convert to slice
	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}
	return members
}

func (s *Store) SInterStore(destination string, keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Get intersection
	members := s.SInter(keys)
	
	// Store result in destination
	db := s.getCurrentDB()
	if len(members) == 0 {
		// Remove destination key if it exists
		delete(db.data, destination)
		delete(db.expiration, destination)
		return 0
	}
	
	newSet := &RedisValue{
		Type:   SetType,
		SetVal: make(map[string]bool),
	}
	
	for _, member := range members {
		newSet.SetVal[member] = true
	}
	
	db.data[destination] = newSet
	delete(db.expiration, destination) // Remove any expiration
	
	return len(members)
}

func (s *Store) SUnionStore(destination string, keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Get union
	members := s.SUnion(keys)
	
	// Store result in destination
	db := s.getCurrentDB()
	if len(members) == 0 {
		// Remove destination key if it exists
		delete(db.data, destination)
		delete(db.expiration, destination)
		return 0
	}
	
	newSet := &RedisValue{
		Type:   SetType,
		SetVal: make(map[string]bool),
	}
	
	for _, member := range members {
		newSet.SetVal[member] = true
	}
	
	db.data[destination] = newSet
	delete(db.expiration, destination) // Remove any expiration
	
	return len(members)
}

func (s *Store) SDiffStore(destination string, keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Get difference
	members := s.SDiff(keys)
	
	// Store result in destination
	db := s.getCurrentDB()
	if len(members) == 0 {
		// Remove destination key if it exists
		delete(db.data, destination)
		delete(db.expiration, destination)
		return 0
	}
	
	newSet := &RedisValue{
		Type:   SetType,
		SetVal: make(map[string]bool),
	}
	
	for _, member := range members {
		newSet.SetVal[member] = true
	}
	
	db.data[destination] = newSet
	delete(db.expiration, destination) // Remove any expiration
	
	return len(members)
}

func (s *Store) SMove(source, destination, member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	// Check if member exists in source
	sourceValue, exists := db.data[source]
	if !exists || sourceValue.Type != SetType || !sourceValue.SetVal[member] {
		return false // Member doesn't exist in source
	}
	
	// Remove from source
	delete(sourceValue.SetVal, member)
	if len(sourceValue.SetVal) == 0 {
		delete(db.data, source)
		delete(db.expiration, source)
	}
	
	// Add to destination
	destValue, exists := db.data[destination]
	if !exists {
		destValue = &RedisValue{
			Type:   SetType,
			SetVal: make(map[string]bool),
		}
		db.data[destination] = destValue
	} else if destValue.Type != SetType {
		// Destination is wrong type, but we already moved from source
		return true
	}
	
	destValue.SetVal[member] = true
	return true
}
