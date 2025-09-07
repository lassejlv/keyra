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

func (zs *ZSet) getRank(member string) int {
	for i, m := range zs.Sorted {
		if m.Member == member {
			return i
		}
	}
	return -1
}

func (zs *ZSet) getRevRank(member string) int {
	rank := zs.getRank(member)
	if rank == -1 {
		return -1
	}
	return len(zs.Sorted) - 1 - rank
}

func (zs *ZSet) getByScore(min, max float64, withScores bool) []string {
	var result []string
	for _, m := range zs.Sorted {
		if m.Score >= min && m.Score <= max {
			result = append(result, m.Member)
			if withScores {
				result = append(result, strconv.FormatFloat(m.Score, 'f', -1, 64))
			}
		}
	}
	return result
}

func (zs *ZSet) getByRank(start, stop int, withScores bool) []string {
	length := len(zs.Sorted)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []string{}
	}
	
	var result []string
	for i := start; i <= stop; i++ {
		result = append(result, zs.Sorted[i].Member)
		if withScores {
			result = append(result, strconv.FormatFloat(zs.Sorted[i].Score, 'f', -1, 64))
		}
	}
	return result
}

func (zs *ZSet) getByRevRank(start, stop int, withScores bool) []string {
	length := len(zs.Sorted)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []string{}
	}
	
	realStart := length - 1 - stop
	realStop := length - 1 - start
	
	var result []string
	for i := realStart; i <= realStop; i++ {
		result = append(result, zs.Sorted[i].Member)
		if withScores {
			result = append(result, strconv.FormatFloat(zs.Sorted[i].Score, 'f', -1, 64))
		}
	}
	
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	
	return result
}

func (s *Store) ZAdd(key string, members map[string]float64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	var zset *ZSet
	
	if !exists {
		zset = newZSet()
		db.data[key] = ZSetValue(zset)
	} else if value.Type != ZSetType {
		return 0
	} else {
		zset = value.ZSet()
	}
	
	count := 0
	for member, score := range members {
		if zset.add(member, score) {
			count++
		}
	}
	
	return count
}

func (s *Store) ZRem(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return 0
	}
	
	zset := value.ZSet()
	count := 0
	for _, member := range members {
		if zset.remove(member) {
			count++
		}
	}
	
	if len(zset.Members) == 0 {
		delete(db.data, key)
	}
	
	return count
}

func (s *Store) ZRange(key string, start, stop int, withScores bool) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return []string{}
	}
	
	return value.ZSet().getByRank(start, stop, withScores)
}

func (s *Store) ZRevRange(key string, start, stop int, withScores bool) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return []string{}
	}
	
	return value.ZSet().getByRevRank(start, stop, withScores)
}

func (s *Store) ZRangeByScore(key string, min, max float64, withScores bool) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return []string{}
	}
	
	return value.ZSet().getByScore(min, max, withScores)
}

func (s *Store) ZRevRangeByScore(key string, max, min float64, withScores bool) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return []string{}
	}
	
	result := value.ZSet().getByScore(min, max, withScores)
	
	if withScores {
		for i := 0; i < len(result)/2; i++ {
			j := len(result) - 2 - i*2
			result[i*2], result[j] = result[j], result[i*2]
			result[i*2+1], result[j+1] = result[j+1], result[i*2+1]
		}
	} else {
		for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
			result[i], result[j] = result[j], result[i]
		}
	}
	
	return result
}

func (s *Store) ZRank(key, member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return -1, false
	}
	
	rank := value.ZSet().getRank(member)
	if rank == -1 {
		return -1, false
	}
	return rank, true
}

func (s *Store) ZRevRank(key, member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return -1, false
	}
	
	rank := value.ZSet().getRevRank(member)
	if rank == -1 {
		return -1, false
	}
	return rank, true
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
	
	score, exists := value.ZSet().Members[member]
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
	
	return len(value.ZSet().Members)
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
	for _, m := range value.ZSet().Sorted {
		if m.Score >= min && m.Score <= max {
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
	var zset *ZSet
	
	if !exists {
		zset = newZSet()
		db.data[key] = ZSetValue(zset)
	} else if value.Type != ZSetType {
		return 0, false 
	} else {
		zset = value.ZSet()
	}
	
	currentScore := zset.Members[member]
	newScore := currentScore + increment
	
	zset.add(member, newScore)
	return newScore, true
}