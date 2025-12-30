package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// XAdd adds an entry to a stream
func (s *Store) XAdd(key string, id string, fields map[string]string, maxLen int64, approximate bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	var stream *Stream
	value, exists := db.data[key]
	if !exists {
		stream = NewStream()
		db.data[key] = StreamValueFromStream(stream)
	} else if value.Type != StreamType {
		return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	} else {
		stream = value.Stream()
	}

	// Generate or validate ID
	entryID, err := s.generateStreamID(stream, id)
	if err != nil {
		return "", err
	}

	// Create entry
	entry := StreamEntry{
		ID:     entryID,
		Fields: fields,
	}

	// Add to stream
	stream.Entries = append(stream.Entries, entry)
	stream.LastID = entryID
	if stream.FirstID == "" {
		stream.FirstID = entryID
	}

	// Apply MAXLEN if specified
	if maxLen > 0 {
		if approximate {
			// Approximate trimming - only trim if significantly over
			if int64(len(stream.Entries)) > maxLen+100 {
				trimCount := int64(len(stream.Entries)) - maxLen
				stream.Entries = stream.Entries[trimCount:]
				if len(stream.Entries) > 0 {
					stream.FirstID = stream.Entries[0].ID
				}
			}
		} else {
			// Exact trimming
			if int64(len(stream.Entries)) > maxLen {
				trimCount := int64(len(stream.Entries)) - maxLen
				stream.Entries = stream.Entries[trimCount:]
				if len(stream.Entries) > 0 {
					stream.FirstID = stream.Entries[0].ID
				}
			}
		}
	}

	return entryID, nil
}

// generateStreamID generates or validates a stream entry ID
func (s *Store) generateStreamID(stream *Stream, id string) (string, error) {
	now := time.Now().UnixMilli()

	if id == "*" {
		// Auto-generate ID
		seq := int64(0)
		if stream.LastID != "" {
			lastTime, lastSeq := parseStreamID(stream.LastID)
			if now == lastTime {
				seq = lastSeq + 1
			} else if now < lastTime {
				// Clock went backwards, use last time + 1
				now = lastTime
				seq = lastSeq + 1
			}
		}
		return fmt.Sprintf("%d-%d", now, seq), nil
	}

	// Partial auto-generate (e.g., "12345-*")
	if strings.HasSuffix(id, "-*") {
		timeStr := strings.TrimSuffix(id, "-*")
		specTime, err := strconv.ParseInt(timeStr, 10, 64)
		if err != nil {
			return "", fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}

		seq := int64(0)
		if stream.LastID != "" {
			lastTime, lastSeq := parseStreamID(stream.LastID)
			if specTime == lastTime {
				seq = lastSeq + 1
			} else if specTime < lastTime {
				return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}
		}
		return fmt.Sprintf("%d-%d", specTime, seq), nil
	}

	// Explicit ID - validate it's greater than last
	if stream.LastID != "" {
		if compareStreamIDs(id, stream.LastID) <= 0 {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	// Validate ID format
	if !isValidStreamID(id) {
		return "", fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	return id, nil
}

// XLen returns the length of a stream
func (s *Store) XLen(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != StreamType {
		return 0
	}

	return int64(len(value.Stream().Entries))
}

// XRange returns entries from a stream in a range
func (s *Store) XRange(key, start, end string, count int64) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != StreamType {
		return nil
	}

	stream := value.Stream()
	return s.rangeEntries(stream, start, end, count, false)
}

// XRevRange returns entries from a stream in reverse order
func (s *Store) XRevRange(key, end, start string, count int64) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != StreamType {
		return nil
	}

	stream := value.Stream()
	return s.rangeEntries(stream, start, end, count, true)
}

func (s *Store) rangeEntries(stream *Stream, start, end string, count int64, reverse bool) []StreamEntry {
	// Normalize special IDs
	if start == "-" {
		start = "0-0"
	}
	if end == "+" {
		end = "18446744073709551615-18446744073709551615"
	}

	var result []StreamEntry

	if reverse {
		for i := len(stream.Entries) - 1; i >= 0; i-- {
			entry := stream.Entries[i]
			if compareStreamIDs(entry.ID, start) >= 0 && compareStreamIDs(entry.ID, end) <= 0 {
				result = append(result, entry)
				if count > 0 && int64(len(result)) >= count {
					break
				}
			}
		}
	} else {
		for _, entry := range stream.Entries {
			if compareStreamIDs(entry.ID, start) >= 0 && compareStreamIDs(entry.ID, end) <= 0 {
				result = append(result, entry)
				if count > 0 && int64(len(result)) >= count {
					break
				}
			}
		}
	}

	return result
}

// XRead reads from one or more streams
func (s *Store) XRead(keys []string, ids []string, count int64) map[string][]StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db := s.getCurrentDB()

	result := make(map[string][]StreamEntry)

	for i, key := range keys {
		s.cleanupExpired(key)
		value, exists := db.data[key]
		if !exists || value.Type != StreamType {
			continue
		}

		stream := value.Stream()
		startID := ids[i]

		// Handle special "$" ID - means only new entries
		if startID == "$" {
			startID = stream.LastID
			if startID == "" {
				startID = "0-0"
			}
		}

		var entries []StreamEntry
		for _, entry := range stream.Entries {
			if compareStreamIDs(entry.ID, startID) > 0 {
				entries = append(entries, entry)
				if count > 0 && int64(len(entries)) >= count {
					break
				}
			}
		}

		if len(entries) > 0 {
			result[key] = entries
		}
	}

	return result
}

// XTrim trims a stream to a maximum length
func (s *Store) XTrim(key string, maxLen int64, approximate bool) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != StreamType {
		return 0
	}

	stream := value.Stream()
	originalLen := int64(len(stream.Entries))

	if maxLen >= originalLen {
		return 0
	}

	trimCount := originalLen - maxLen
	if approximate && trimCount < 100 {
		return 0
	}

	stream.Entries = stream.Entries[trimCount:]
	if len(stream.Entries) > 0 {
		stream.FirstID = stream.Entries[0].ID
	} else {
		stream.FirstID = ""
	}

	return trimCount
}

// XDel deletes entries from a stream
func (s *Store) XDel(key string, ids []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != StreamType {
		return 0
	}

	stream := value.Stream()
	idSet := make(map[string]bool)
	for _, id := range ids {
		idSet[id] = true
	}

	var newEntries []StreamEntry
	deleted := int64(0)

	for _, entry := range stream.Entries {
		if idSet[entry.ID] {
			deleted++
		} else {
			newEntries = append(newEntries, entry)
		}
	}

	stream.Entries = newEntries
	if len(stream.Entries) > 0 {
		stream.FirstID = stream.Entries[0].ID
		stream.LastID = stream.Entries[len(stream.Entries)-1].ID
	} else {
		stream.FirstID = ""
		stream.LastID = ""
	}

	return deleted
}

// XInfo returns information about a stream
func (s *Store) XInfoStream(key string) (map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanupExpired(key)
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != StreamType {
		return nil, false
	}

	stream := value.Stream()
	info := map[string]interface{}{
		"length":           int64(len(stream.Entries)),
		"radix-tree-keys":  int64(1),
		"radix-tree-nodes": int64(2),
		"last-generated-id": stream.LastID,
		"groups":           int64(len(stream.Groups)),
		"first-entry":      nil,
		"last-entry":       nil,
	}

	if len(stream.Entries) > 0 {
		info["first-entry"] = stream.Entries[0]
		info["last-entry"] = stream.Entries[len(stream.Entries)-1]
	}

	return info, true
}

// XGroupCreate creates a consumer group
func (s *Store) XGroupCreate(key, group, id string, mkstream bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists {
		if mkstream {
			stream := NewStream()
			db.data[key] = StreamValueFromStream(stream)
			value = db.data[key]
		} else {
			return fmt.Errorf("ERR The XGROUP subcommand requires the key to exist")
		}
	} else if value.Type != StreamType {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := value.Stream()

	if _, exists := stream.Groups[group]; exists {
		return fmt.Errorf("BUSYGROUP Consumer Group name already exists")
	}

	lastID := id
	if id == "$" {
		lastID = stream.LastID
		if lastID == "" {
			lastID = "0-0"
		}
	} else if id == "0" {
		lastID = "0-0"
	}

	stream.Groups[group] = &ConsumerGroup{
		Name:            group,
		LastDeliveredID: lastID,
		Pending:         make(map[string]*PendingEntry),
		Consumers:       make(map[string]*Consumer),
	}

	return nil
}

// XGroupDestroy destroys a consumer group
func (s *Store) XGroupDestroy(key, group string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.getCurrentDB()

	value, exists := db.data[key]
	if !exists || value.Type != StreamType {
		return false, nil
	}

	stream := value.Stream()
	if _, exists := stream.Groups[group]; !exists {
		return false, nil
	}

	delete(stream.Groups, group)
	return true, nil
}

// Helper functions

func parseStreamID(id string) (int64, int64) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0
	}
	time, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	return time, seq
}

func compareStreamIDs(a, b string) int {
	aTime, aSeq := parseStreamID(a)
	bTime, bSeq := parseStreamID(b)

	if aTime < bTime {
		return -1
	}
	if aTime > bTime {
		return 1
	}
	if aSeq < bSeq {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	return 0
}

func isValidStreamID(id string) bool {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return false
	}
	_, err1 := strconv.ParseInt(parts[0], 10, 64)
	_, err2 := strconv.ParseInt(parts[1], 10, 64)
	return err1 == nil && err2 == nil
}

