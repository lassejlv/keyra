package store

import (
	"sort"
	"strconv"
)

func newZSet() *ZSet {
	return &ZSet{
		Members: make(map[string]float64),
		Sorted:  make([]ZSetMember, 0),
	}
}

func (zs *ZSet) add(member string, score float64) bool {
	_, exists := zs.Members[member]
	zs.Members[member] = score
	
	if exists {
		for i, m := range zs.Sorted {
			if m.Member == member {
				zs.Sorted = append(zs.Sorted[:i], zs.Sorted[i+1:]...)
				break
			}
		}
	}
	
	newMember := ZSetMember{Member: member, Score: score}
	pos := sort.Search(len(zs.Sorted), func(i int) bool {
		if zs.Sorted[i].Score == score {
			return zs.Sorted[i].Member >= member
		}
		return zs.Sorted[i].Score > score
	})
	
	zs.Sorted = append(zs.Sorted, ZSetMember{})
	copy(zs.Sorted[pos+1:], zs.Sorted[pos:])
	zs.Sorted[pos] = newMember
	
	return !exists 
}

func (zs *ZSet) remove(member string) bool {
	_, exists := zs.Members[member]
	if !exists {
		return false
	}
	
	delete(zs.Members, member)
	
	for i, m := range zs.Sorted {
		if m.Member == member {
			zs.Sorted = append(zs.Sorted[:i], zs.Sorted[i+1:]...)
			break
		}
	}
	
	return true
}

func (zs *ZSet) getRank(member string, reverse bool) int {
	for i, m := range zs.Sorted {
		if m.Member == member {
			if reverse {
				return len(zs.Sorted) - 1 - i
			}
			return i
		}
	}
	return -1 
}

func (zs *ZSet) getByRank(start, stop int, reverse bool) []ZSetMember {
	length := len(zs.Sorted)
	if length == 0 {
		return []ZSetMember{}
	}
	
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
		return []ZSetMember{}
	}
	if stop >= length {
		stop = length - 1
	}
	if stop < start {
		return []ZSetMember{}
	}
	
	result := make([]ZSetMember, stop-start+1)
	if reverse {
		for i := 0; i <= stop-start; i++ {
			result[i] = zs.Sorted[length-1-start-i]
		}
	} else {
		copy(result, zs.Sorted[start:stop+1])
	}
	
	return result
}

func (zs *ZSet) getByScore(min, max float64, reverse bool) []ZSetMember {
	var result []ZSetMember
	
	for _, member := range zs.Sorted {
		if member.Score >= min && member.Score <= max {
			result = append(result, member)
		}
	}
	
	if reverse {
		// Reverse the result
		for i := 0; i < len(result)/2; i++ {
			result[i], result[len(result)-1-i] = result[len(result)-1-i], result[i]
		}
	}
	
	return result
}

func (s *Store) ZAdd(key string, scoreMembers []string) int {
	if len(scoreMembers)%2 != 0 {
		return -1 
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    ZSetType,
			ZSetVal: newZSet(),
		}
		db.data[key] = value
	} else if value.Type != ZSetType {
		return -1
	}
	
	added := 0
	for i := 0; i < len(scoreMembers); i += 2 {
		score, err := strconv.ParseFloat(scoreMembers[i], 64)
		if err != nil {
			continue 
		}
		member := scoreMembers[i+1]
		
		if value.ZSetVal.add(member, score) {
			added++
		}
	}
	
	return added
}

func (s *Store) ZRem(key string, members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return 0
	}
	
	removed := 0
	for _, member := range members {
		if value.ZSetVal.remove(member) {
			removed++
		}
	}
	
	// Remove key if empty
	if len(value.ZSetVal.Members) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
	}
	
	return removed
}

func (s *Store) ZRange(key string, start, stop int, reverse bool) []ZSetMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return []ZSetMember{}
	}
	
	return value.ZSetVal.getByRank(start, stop, reverse)
}

func (s *Store) ZRangeByScore(key string, min, max float64, reverse bool) []ZSetMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return []ZSetMember{}
	}
	
	return value.ZSetVal.getByScore(min, max, reverse)
}

func (s *Store) ZRank(key, member string, reverse bool) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return -1
	}
	
	return value.ZSetVal.getRank(member, reverse)
}

func (s *Store) ZScore(key, member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return 0, false
	}
	
	score, exists := value.ZSetVal.Members[member]
	return score, exists
}

func (s *Store) ZCard(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return 0
	}
	
	return len(value.ZSetVal.Members)
}

func (s *Store) ZCount(key string, min, max float64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return 0
	}
	
	count := 0
	for _, score := range value.ZSetVal.Members {
		if score >= min && score <= max {
			count++
		}
	}
	return count
}

func (s *Store) ZIncrBy(key, member string, increment float64) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    ZSetType,
			ZSetVal: newZSet(),
		}
		db.data[key] = value
	} else if value.Type != ZSetType {
		return 0, false 
	}
	
	currentScore := value.ZSetVal.Members[member]
	newScore := currentScore + increment
	
	value.ZSetVal.add(member, newScore)
	return newScore, true
}
