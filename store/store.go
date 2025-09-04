package store

import (
	"redis-go-clone/persistence"
	"strings"
	"sync"
	"time"
)

type DataType int

const (
	StringType DataType = iota
	ListType
	HashType
	SetType
)

func (dt DataType) String() string {
	switch dt {
	case StringType:
		return "string"
	case ListType:
		return "list"
	case HashType:
		return "hash"
	case SetType:
		return "set"
	default:
		return "none"
	}
}

type RedisValue struct {
	Type       DataType
	StringVal  string
	ListVal    []string
	HashVal    map[string]string
	SetVal     map[string]bool
}

type Database struct {
	data       map[string]*RedisValue
	expiration map[string]time.Time
}

func newDatabase() *Database {
	return &Database{
		data:       make(map[string]*RedisValue),
		expiration: make(map[string]time.Time),
	}
}

type Store struct {
	databases   [16]*Database
	currentDB   int
	mu          sync.RWMutex
	persistence *persistence.Persistence
}

func New(persistenceFile string) *Store {
	s := &Store{
		currentDB:   0,
		persistence: persistence.New(persistenceFile),
	}
	
	// Initialize all 16 databases
	for i := 0; i < 16; i++ {
		s.databases[i] = newDatabase()
	}
	
	s.Load()
	return s
}

func NewInMemory() *Store {
	s := &Store{
		currentDB: 0,
	}
	
	// Initialize all 16 databases
	for i := 0; i < 16; i++ {
		s.databases[i] = newDatabase()
	}
	
	return s
}

func (s *Store) getCurrentDB() *Database {
	return s.databases[s.currentDB]
}

func (s *Store) isExpired(key string) bool {
	db := s.getCurrentDB()
	if expTime, exists := db.expiration[key]; exists {
		return time.Now().After(expTime)
	}
	return false
}

func (s *Store) cleanupExpired(key string) {
	if s.isExpired(key) {
		db := s.getCurrentDB()
		delete(db.data, key)
		delete(db.expiration, key)
	}
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	db.data[key] = &RedisValue{
		Type:      StringType,
		StringVal: value,
	}
	delete(db.expiration, key)
}

func (s *Store) SetWithExpiration(key, value string, expiration time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	db.data[key] = &RedisValue{
		Type:      StringType,
		StringVal: value,
	}
	db.expiration[key] = expiration
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	value, exists := db.data[key]
	if !exists || value.Type != StringType {
		return "", false
	}
	return value.StringVal, true
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	_, exists := db.data[key]
	if exists {
		delete(db.data, key)
		delete(db.expiration, key)
	}
	return exists
}

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

func (s *Store) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.persistence == nil {
		return nil
	}
	
	// For now, only save database 0 for backward compatibility
	// TODO: Support multi-database persistence
	db := s.databases[0]
	
	dataCopy := make(map[string]string)
	for k, v := range db.data {
		if v.Type == StringType {
			dataCopy[k] = v.StringVal
		}
		// TODO: Serialize other data types
	}
	
	expCopy := make(map[string]time.Time)
	for k, v := range db.expiration {
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
	
	// Load into database 0 for backward compatibility
	db := s.databases[0]
	
	// Convert string data to RedisValue format
	for k, v := range data {
		db.data[k] = &RedisValue{
			Type:      StringType,
			StringVal: v,
		}
	}
	
	db.expiration = expiration
	return nil
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	_, exists := db.data[key]
	return exists
}

func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db := s.getCurrentDB()
	
	var keys []string
	for key := range db.data {
		s.cleanupExpired(key)
		if _, exists := db.data[key]; exists {
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
	db := s.getCurrentDB()
	
	count := 0
	for key := range db.data {
		s.cleanupExpired(key)
		if _, exists := db.data[key]; exists {
			count++
		}
	}
	return count
}

func (s *Store) FlushDB() {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	db.data = make(map[string]*RedisValue)
	db.expiration = make(map[string]time.Time)
}

func (s *Store) Append(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	if existing, exists := db.data[key]; exists && existing.Type == StringType {
		existing.StringVal += value
		return len(existing.StringVal)
	} else {
		db.data[key] = &RedisValue{
			Type:      StringType,
			StringVal: value,
		}
		return len(value)
	}
}

func (s *Store) GetRange(key string, start, end int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != StringType {
		return ""
	}
	
	length := len(value.StringVal)
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
	
	return value.StringVal[start : end+1]
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

func (s *Store) GetType(key string) DataType {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	
	db := s.getCurrentDB()
	value, exists := db.data[key]
	if !exists {
		return DataType(-1) // none
	}
	return value.Type
}

// List operations
func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    ListType,
			ListVal: make([]string, 0),
		}
		db.data[key] = value
	} else if value.Type != ListType {
		return -1 // Wrong type error
	}
	
	// Prepend values in reverse order to maintain order
	for i := len(values) - 1; i >= 0; i-- {
		value.ListVal = append([]string{values[i]}, value.ListVal...)
	}
	
	return len(value.ListVal)
}

func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists {
		value = &RedisValue{
			Type:    ListType,
			ListVal: make([]string, 0),
		}
		db.data[key] = value
	} else if value.Type != ListType {
		return -1 // Wrong type error
	}
	
	value.ListVal = append(value.ListVal, values...)
	return len(value.ListVal)
}

func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType || len(value.ListVal) == 0 {
		return "", false
	}
	
	result := value.ListVal[0]
	value.ListVal = value.ListVal[1:]
	
	if len(value.ListVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
	}
	
	return result, true
}

func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType || len(value.ListVal) == 0 {
		return "", false
	}
	
	length := len(value.ListVal)
	result := value.ListVal[length-1]
	value.ListVal = value.ListVal[:length-1]
	
	if len(value.ListVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
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
	
	return len(value.ListVal)
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
	
	length := len(value.ListVal)
	if length == 0 {
		return []string{}
	}
	
	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	
	// Bounds checking
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
	
	return value.ListVal[start : stop+1]
}

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
