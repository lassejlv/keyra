package persistence

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"time"
)

// DataType mirrors store.DataType for persistence
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

// ZSetMember mirrors store.ZSetMember for persistence
type ZSetMember struct {
	Member string
	Score  float64
}

// ZSetData holds serializable ZSet data
type ZSetData struct {
	Members map[string]float64
	Sorted  []ZSetMember
}

// StreamEntry for persistence
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// StreamData holds serializable Stream data
type StreamData struct {
	Entries []StreamEntry
	LastID  string
	FirstID string
}

// SerializedValue represents a serializable version of RedisValue
type SerializedValue struct {
	Type         DataType
	StringValue  string
	ListValue    []string
	HashValue    map[string]string
	SetValue     map[string]bool
	ZSetValue    *ZSetData
	JSONValue    []byte // JSON stored as raw bytes
	StreamValue  *StreamData
}

// DatabaseSnapshot represents a single database's state
type DatabaseSnapshot struct {
	Data       map[string]SerializedValue
	Expiration map[string]time.Time
}

// DataSnapshot represents the full state of all databases
type DataSnapshot struct {
	Databases [16]DatabaseSnapshot
	Timestamp time.Time
}

type Persistence struct {
	filename string
}

func init() {
	// Register types for gob encoding
	gob.Register(SerializedValue{})
	gob.Register(ZSetData{})
	gob.Register(ZSetMember{})
	gob.Register(DatabaseSnapshot{})
	gob.Register(DataSnapshot{})
	gob.Register(StreamData{})
	gob.Register(StreamEntry{})
}

func New(filename string) *Persistence {
	if dir := filepath.Dir(filename); dir != "." {
		os.MkdirAll(dir, 0755)
	}
	return &Persistence{
		filename: filename,
	}
}

func GetStoragePath() string {
	if path := os.Getenv("REDIS_STORAGE_PATH"); path != "" {
		return path
	}
	if dataDir := os.Getenv("REDIS_DATA_DIR"); dataDir != "" {
		return filepath.Join(dataDir, "redis_data.rdb")
	}
	return "redis_data.rdb"
}

func (p *Persistence) SaveDatabases(databases [16]DatabaseSnapshot) error {
	snapshot := DataSnapshot{
		Databases: databases,
		Timestamp: time.Now(),
	}

	file, err := os.Create(p.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(snapshot)
}

func (p *Persistence) LoadDatabases() ([16]DatabaseSnapshot, error) {
	var databases [16]DatabaseSnapshot
	
	// Initialize empty databases
	for i := 0; i < 16; i++ {
		databases[i] = DatabaseSnapshot{
			Data:       make(map[string]SerializedValue),
			Expiration: make(map[string]time.Time),
		}
	}

	file, err := os.Open(p.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return databases, nil
		}
		return databases, err
	}
	defer file.Close()

	var snapshot DataSnapshot
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&snapshot)
	if err != nil {
		return databases, err
	}

	now := time.Now()

	// Filter out expired keys
	for dbIdx := 0; dbIdx < 16; dbIdx++ {
		dbSnapshot := snapshot.Databases[dbIdx]
		if dbSnapshot.Data == nil {
			continue
		}
		
		for key, value := range dbSnapshot.Data {
			if expTime, hasExpiration := dbSnapshot.Expiration[key]; hasExpiration {
				if now.After(expTime) {
					continue // Skip expired keys
				}
				databases[dbIdx].Expiration[key] = expTime
			}
			databases[dbIdx].Data[key] = value
		}
	}

	return databases, nil
}

// Legacy methods for backward compatibility
func (p *Persistence) Save(data map[string]string, expiration map[string]time.Time) error {
	var databases [16]DatabaseSnapshot
	for i := 0; i < 16; i++ {
		databases[i] = DatabaseSnapshot{
			Data:       make(map[string]SerializedValue),
			Expiration: make(map[string]time.Time),
		}
	}
	
	// Convert string-only data to new format in database 0
	for k, v := range data {
		databases[0].Data[k] = SerializedValue{
			Type:        StringType,
			StringValue: v,
		}
	}
	databases[0].Expiration = expiration
	
	return p.SaveDatabases(databases)
}

func (p *Persistence) Load() (map[string]string, map[string]time.Time, error) {
	databases, err := p.LoadDatabases()
	if err != nil {
		return nil, nil, err
	}
	
	// Convert back to string-only format from database 0
	data := make(map[string]string)
	for k, v := range databases[0].Data {
		if v.Type == StringType {
			data[k] = v.StringValue
		}
	}
	
	return data, databases[0].Expiration, nil
}

func (p *Persistence) Exists() bool {
	_, err := os.Stat(p.filename)
	return !os.IsNotExist(err)
}
