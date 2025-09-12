package store

import (
	"keyra/persistence"
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
	Type  DataType
	Value interface{}
}

func StringValue(s string) *RedisValue {
	return &RedisValue{Type: StringType, Value: s}
}

func ListValue(l []string) *RedisValue {
	return &RedisValue{Type: ListType, Value: l}
}

func HashValue(h map[string]string) *RedisValue {
	return &RedisValue{Type: HashType, Value: h}
}

func SetValue(s map[string]bool) *RedisValue {
	return &RedisValue{Type: SetType, Value: s}
}

func ZSetValue(zs *ZSet) *RedisValue {
	return &RedisValue{Type: ZSetType, Value: zs}
}

func (rv *RedisValue) String() string {
	if rv.Type != StringType {
		panic("value is not a string")
	}
	return rv.Value.(string)
}

func (rv *RedisValue) List() []string {
	if rv.Type != ListType {
		panic("value is not a list")
	}
	return rv.Value.([]string)
}

func (rv *RedisValue) Hash() map[string]string {
	if rv.Type != HashType {
		panic("value is not a hash")
	}
	return rv.Value.(map[string]string)
}

func (rv *RedisValue) Set() map[string]bool {
	if rv.Type != SetType {
		panic("value is not a set")
	}
	return rv.Value.(map[string]bool)
}

func (rv *RedisValue) ZSet() *ZSet {
	if rv.Type != ZSetType {
		panic("value is not a zset")
	}
	return rv.Value.(*ZSet)
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
	db.data[key] = StringValue(value)
	delete(db.expiration, key)
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
	return value.String(), true
}

func (s *Store) GetRaw(key string) (*RedisValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isExpired(key) {
		return nil, false
	}
	db := s.getCurrentDB()
	value, exists := db.data[key]
	return value, exists
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

func (s *Store) GetCurrentDB() int {
	return s.GetCurrentDBIndex()
}

func (s *Store) GetList(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	
	db := s.getCurrentDB()
	value, exists := db.data[key]
	if !exists || value.Type != ListType {
		return nil
	}
	
	list := value.List()
	result := make([]string, len(list))
	copy(result, list)
	return result
}

func (s *Store) GetSet(key string) map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	
	db := s.getCurrentDB()
	value, exists := db.data[key]
	if !exists || value.Type != SetType {
		return nil
	}
	
	set := value.Set()
	result := make(map[string]bool)
	for k, v := range set {
		result[k] = v
	}
	return result
}

func (s *Store) GetHash(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	
	db := s.getCurrentDB()
	value, exists := db.data[key]
	if !exists || value.Type != HashType {
		return nil
	}
	
	hash := value.Hash()
	result := make(map[string]string)
	for k, v := range hash {
		result[k] = v
	}
	return result
}

func (s *Store) GetZSet(key string) *ZSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	
	db := s.getCurrentDB()
	value, exists := db.data[key]
	if !exists || value.Type != ZSetType {
		return nil
	}
	
	return value.ZSet()
}

func (s *Store) GetTTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	db := s.getCurrentDB()
	if expTime, exists := db.expiration[key]; exists {
		remaining := time.Until(expTime)
		if remaining > 0 {
			return remaining
		}
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
			dataCopy[k] = v.String()
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
		db.data[k] = StringValue(v)
	}
	
	db.expiration = expiration
	return nil
}
