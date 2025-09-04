package persistence

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"time"
)

type DataSnapshot struct {
	Data       map[string]string
	Expiration map[string]time.Time
	Timestamp  time.Time
}

type Persistence struct {
	filename string
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

func (p *Persistence) Save(data map[string]string, expiration map[string]time.Time) error {
	snapshot := DataSnapshot{
		Data:       data,
		Expiration: expiration,
		Timestamp:  time.Now(),
	}

	file, err := os.Create(p.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(snapshot)
}

func (p *Persistence) Load() (map[string]string, map[string]time.Time, error) {
	file, err := os.Open(p.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]string), make(map[string]time.Time), nil
		}
		return nil, nil, err
	}
	defer file.Close()

	var snapshot DataSnapshot
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&snapshot)
	if err != nil {
		return nil, nil, err
	}

	data := make(map[string]string)
	expiration := make(map[string]time.Time)
	now := time.Now()

	for key, value := range snapshot.Data {
		if expTime, hasExpiration := snapshot.Expiration[key]; hasExpiration {
			if now.After(expTime) {
				continue
			}
			expiration[key] = expTime
		}
		data[key] = value
	}

	return data, expiration, nil
}

func (p *Persistence) Exists() bool {
	_, err := os.Stat(p.filename)
	return !os.IsNotExist(err)
}
