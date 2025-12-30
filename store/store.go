package store

import (
	"encoding/json"
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
	JSONType
	StreamType
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
	case JSONType:
		return "ReJSON-RL"
	case StreamType:
		return "stream"
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

func JSONValue(j interface{}) *RedisValue {
	return &RedisValue{Type: JSONType, Value: j}
}

func StreamValueFromStream(s *Stream) *RedisValue {
	return &RedisValue{Type: StreamType, Value: s}
}

// Stream represents a Redis Stream
type Stream struct {
	Entries      []StreamEntry
	LastID       string
	Groups       map[string]*ConsumerGroup
	MaxLen       int64 // 0 means unlimited
	FirstID      string
}

// StreamEntry represents a single entry in a stream
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// ConsumerGroup represents a consumer group
type ConsumerGroup struct {
	Name            string
	LastDeliveredID string
	Pending         map[string]*PendingEntry // entry ID -> pending info
	Consumers       map[string]*Consumer
}

// Consumer represents a consumer in a group
type Consumer struct {
	Name        string
	Pending     int64
	LastSeenTime time.Time
}

// PendingEntry represents a pending entry
type PendingEntry struct {
	EntryID       string
	ConsumerName  string
	DeliveryTime  time.Time
	DeliveryCount int64
}

func NewStream() *Stream {
	return &Stream{
		Entries: make([]StreamEntry, 0),
		Groups:  make(map[string]*ConsumerGroup),
	}
}

func (rv *RedisValue) Stream() *Stream {
	if rv.Type != StreamType {
		panic("value is not a stream")
	}
	return rv.Value.(*Stream)
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

func (rv *RedisValue) JSON() interface{} {
	if rv.Type != JSONType {
		panic("value is not JSON")
	}
	return rv.Value
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
	
	var databases [16]persistence.DatabaseSnapshot
	
	for dbIdx := 0; dbIdx < 16; dbIdx++ {
		db := s.databases[dbIdx]
		databases[dbIdx] = persistence.DatabaseSnapshot{
			Data:       make(map[string]persistence.SerializedValue),
			Expiration: make(map[string]time.Time),
		}
		
		for k, v := range db.data {
			sv := persistence.SerializedValue{
				Type: persistence.DataType(v.Type),
			}
			
			switch v.Type {
			case StringType:
				sv.StringValue = v.String()
			case ListType:
				// Make a copy of the list
				list := v.List()
				sv.ListValue = make([]string, len(list))
				copy(sv.ListValue, list)
			case HashType:
				// Make a copy of the hash
				hash := v.Hash()
				sv.HashValue = make(map[string]string)
				for hk, hv := range hash {
					sv.HashValue[hk] = hv
				}
			case SetType:
				// Make a copy of the set
				set := v.Set()
				sv.SetValue = make(map[string]bool)
				for sk, sval := range set {
					sv.SetValue[sk] = sval
				}
			case ZSetType:
				// Make a copy of the zset
				zset := v.ZSet()
				sv.ZSetValue = &persistence.ZSetData{
					Members: make(map[string]float64),
					Sorted:  make([]persistence.ZSetMember, len(zset.Sorted)),
				}
				for mk, mv := range zset.Members {
					sv.ZSetValue.Members[mk] = mv
				}
				for i, m := range zset.Sorted {
					sv.ZSetValue.Sorted[i] = persistence.ZSetMember{
						Member: m.Member,
						Score:  m.Score,
					}
				}
			case JSONType:
				// Serialize JSON value to bytes
				jsonBytes, err := json.Marshal(v.JSON())
				if err == nil {
					sv.JSONValue = jsonBytes
				}
			case StreamType:
				// Serialize stream
				stream := v.Stream()
				sv.StreamValue = &persistence.StreamData{
					Entries: make([]persistence.StreamEntry, len(stream.Entries)),
					LastID:  stream.LastID,
					FirstID: stream.FirstID,
				}
				for i, e := range stream.Entries {
					sv.StreamValue.Entries[i] = persistence.StreamEntry{
						ID:     e.ID,
						Fields: make(map[string]string),
					}
					for fk, fv := range e.Fields {
						sv.StreamValue.Entries[i].Fields[fk] = fv
					}
				}
			}
			
			databases[dbIdx].Data[k] = sv
		}
		
		for k, v := range db.expiration {
			databases[dbIdx].Expiration[k] = v
		}
	}
	
	return s.persistence.SaveDatabases(databases)
}

func (s *Store) Load() error {
	if s.persistence == nil {
		return nil
	}
	
	databases, err := s.persistence.LoadDatabases()
	if err != nil {
		return err
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	
	for dbIdx := 0; dbIdx < 16; dbIdx++ {
		dbSnapshot := databases[dbIdx]
		db := s.databases[dbIdx]
		
		for k, sv := range dbSnapshot.Data {
			switch persistence.DataType(sv.Type) {
			case persistence.StringType:
				db.data[k] = StringValue(sv.StringValue)
			case persistence.ListType:
				// Make a copy of the list
				list := make([]string, len(sv.ListValue))
				copy(list, sv.ListValue)
				db.data[k] = ListValue(list)
			case persistence.HashType:
				// Make a copy of the hash
				hash := make(map[string]string)
				for hk, hv := range sv.HashValue {
					hash[hk] = hv
				}
				db.data[k] = HashValue(hash)
			case persistence.SetType:
				// Make a copy of the set
				set := make(map[string]bool)
				for sk, sval := range sv.SetValue {
					set[sk] = sval
				}
				db.data[k] = SetValue(set)
			case persistence.ZSetType:
				if sv.ZSetValue != nil {
					zset := &ZSet{
						Members: make(map[string]float64),
						Sorted:  make([]ZSetMember, len(sv.ZSetValue.Sorted)),
					}
					for mk, mv := range sv.ZSetValue.Members {
						zset.Members[mk] = mv
					}
					for i, m := range sv.ZSetValue.Sorted {
						zset.Sorted[i] = ZSetMember{
							Member: m.Member,
							Score:  m.Score,
						}
					}
					db.data[k] = ZSetValue(zset)
				}
			case persistence.JSONType:
				if sv.JSONValue != nil {
					var jsonData interface{}
					if err := json.Unmarshal(sv.JSONValue, &jsonData); err == nil {
						db.data[k] = JSONValue(jsonData)
					}
				}
			case persistence.StreamType:
				if sv.StreamValue != nil {
					stream := &Stream{
						Entries: make([]StreamEntry, len(sv.StreamValue.Entries)),
						LastID:  sv.StreamValue.LastID,
						FirstID: sv.StreamValue.FirstID,
						Groups:  make(map[string]*ConsumerGroup),
					}
					for i, e := range sv.StreamValue.Entries {
						stream.Entries[i] = StreamEntry{
							ID:     e.ID,
							Fields: make(map[string]string),
						}
						for fk, fv := range e.Fields {
							stream.Entries[i].Fields[fk] = fv
						}
					}
					db.data[k] = StreamValueFromStream(stream)
				}
			}
		}
		
		db.expiration = dbSnapshot.Expiration
	}
	
	return nil
}
