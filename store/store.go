package store

import (
	"math/rand"
	"redis-go-clone/persistence"
	"sort"
	"strconv"
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
	ZSetType
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
	case ZSetType:
		return "zset"
	default:
		return "none"
	}
}

type ZSetMember struct {
	Member string
	Score  float64
}

type RedisValue struct {
	Type       DataType
	StringVal  string
	ListVal    []string
	HashVal    map[string]string
	SetVal     map[string]bool
	ZSetVal    *ZSet
}

type ZSet struct {
	Members map[string]float64 // member -> score lookup
	Sorted  []ZSetMember       // sorted by score, then lexicographically
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

func (s *Store) LIndex(key string, index int) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return "", false
	}
	
	length := len(value.ListVal)
	if length == 0 {
		return "", false
	}
	
	// Handle negative indices
	if index < 0 {
		index = length + index
	}
	
	// Bounds checking
	if index < 0 || index >= length {
		return "", false
	}
	
	return value.ListVal[index], true
}

func (s *Store) LSet(key string, index int, element string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return false
	}
	
	length := len(value.ListVal)
	if length == 0 {
		return false
	}
	
	// Handle negative indices
	if index < 0 {
		index = length + index
	}
	
	// Bounds checking
	if index < 0 || index >= length {
		return false
	}
	
	value.ListVal[index] = element
	return true
}

func (s *Store) LTrim(key string, start, stop int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return false
	}
	
	length := len(value.ListVal)
	if length == 0 {
		return true
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
		// Trim to empty list
		value.ListVal = []string{}
		return true
	}
	if stop >= length {
		stop = length - 1
	}
	if stop < start {
		// Trim to empty list
		value.ListVal = []string{}
		return true
	}
	
	// Trim the list
	value.ListVal = value.ListVal[start : stop+1]
	
	// If list becomes empty, remove the key
	if len(value.ListVal) == 0 {
		delete(db.data, key)
		delete(db.expiration, key)
	}
	
	return true
}

func (s *Store) LInsert(key, where, pivot, element string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()
	
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return 0 // Key doesn't exist or wrong type
	}
	
	// Find pivot element
	for i, val := range value.ListVal {
		if val == pivot {
			if strings.ToUpper(where) == "BEFORE" {
				// Insert before pivot
				value.ListVal = append(value.ListVal[:i], append([]string{element}, value.ListVal[i:]...)...)
			} else if strings.ToUpper(where) == "AFTER" {
				// Insert after pivot
				if i+1 >= len(value.ListVal) {
					value.ListVal = append(value.ListVal, element)
				} else {
					value.ListVal = append(value.ListVal[:i+1], append([]string{element}, value.ListVal[i+1:]...)...)
				}
			} else {
				return -1 // Invalid where parameter
			}
			return len(value.ListVal)
		}
	}
	
	return -1 // Pivot not found
}

// Basic implementation of blocking operations (non-blocking for now)
// TODO: Implement true blocking with client connection state management
func (s *Store) BLPop(keys []string, timeout int) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	// Try each key in order
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == ListType && len(value.ListVal) > 0 {
			result := value.ListVal[0]
			value.ListVal = value.ListVal[1:]
			
			if len(value.ListVal) == 0 {
				delete(db.data, key)
				delete(db.expiration, key)
			}
			
			return key, result, true
		}
	}
	
	// No data available (in a real implementation, this would block)
	return "", "", false
}

func (s *Store) BRPop(keys []string, timeout int) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()
	
	// Try each key in order
	for _, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if exists && value.Type == ListType && len(value.ListVal) > 0 {
			length := len(value.ListVal)
			result := value.ListVal[length-1]
			value.ListVal = value.ListVal[:length-1]
			
			if len(value.ListVal) == 0 {
				delete(db.data, key)
				delete(db.expiration, key)
			}
			
			return key, result, true
		}
	}
	
	// No data available (in a real implementation, this would block)
	return "", "", false
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

func (s *Store) RandomKey() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db := s.getCurrentDB()
	
	if len(db.data) == 0 {
		return ""
	}
	
	// Get a random key
	keys := make([]string, 0, len(db.data))
	for key := range db.data {
		s.cleanupExpired(key)
		if _, exists := db.data[key]; exists {
			keys = append(keys, key)
		}
	}
	
	if len(keys) == 0 {
		return ""
	}
	
	return keys[rand.Intn(len(keys))]
}

// ZSet helper functions
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
		// Update existing member - remove from sorted list first
		for i, m := range zs.Sorted {
			if m.Member == member {
				zs.Sorted = append(zs.Sorted[:i], zs.Sorted[i+1:]...)
				break
			}
		}
	}
	
	// Insert in correct position to maintain sort order
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
	
	return !exists // Return true if it's a new member
}

func (zs *ZSet) remove(member string) bool {
	_, exists := zs.Members[member]
	if !exists {
		return false
	}
	
	delete(zs.Members, member)
	
	// Remove from sorted list
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
	return -1 // Not found
}

func (zs *ZSet) getByRank(start, stop int, reverse bool) []ZSetMember {
	length := len(zs.Sorted)
	if length == 0 {
		return []ZSetMember{}
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

// Sorted Set operations
func (s *Store) ZAdd(key string, scoreMembers []string) int {
	if len(scoreMembers)%2 != 0 {
		return -1 // Invalid arguments
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
		return -1 // Wrong type error
	}
	
	added := 0
	for i := 0; i < len(scoreMembers); i += 2 {
		score, err := strconv.ParseFloat(scoreMembers[i], 64)
		if err != nil {
			continue // Skip invalid scores
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
		return 0, false // Wrong type error
	}
	
	currentScore, exists := value.ZSetVal.Members[member]
	newScore := currentScore + increment
	
	value.ZSetVal.add(member, newScore)
	return newScore, true
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
