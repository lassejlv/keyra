package server

import (
	"fmt"
	"strconv"
	"strings"

	"keyra/protocol"
	"keyra/store"
)

// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] [LIMIT count] *|id field value [field value ...]
func (s *Server) handleXAdd(args []string) string {
	if len(args) < 4 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xadd' command")
	}

	key := args[0]
	idx := 1

	var maxLen int64 = 0
	approximate := false

	// Parse options
	for idx < len(args) {
		arg := strings.ToUpper(args[idx])
		switch arg {
		case "NOMKSTREAM":
			idx++
			// Check if stream exists
			if !s.store.Exists(key) {
				return protocol.EncodeNull()
			}
		case "MAXLEN":
			idx++
			if idx >= len(args) {
				return protocol.EncodeError("ERR syntax error")
			}
			// Check for ~ or =
			nextArg := args[idx]
			if nextArg == "~" {
				approximate = true
				idx++
				if idx >= len(args) {
					return protocol.EncodeError("ERR syntax error")
				}
				nextArg = args[idx]
			} else if nextArg == "=" {
				idx++
				if idx >= len(args) {
					return protocol.EncodeError("ERR syntax error")
				}
				nextArg = args[idx]
			}
			var err error
			maxLen, err = strconv.ParseInt(nextArg, 10, 64)
			if err != nil {
				return protocol.EncodeError("ERR value is not an integer or out of range")
			}
			idx++
		case "MINID":
			// Skip MINID for now
			idx++
			if idx < len(args) && (args[idx] == "~" || args[idx] == "=") {
				idx++
			}
			if idx < len(args) {
				idx++
			}
		case "LIMIT":
			// Skip LIMIT for now
			idx += 2
		default:
			// This should be the ID
			goto parseID
		}
	}

parseID:
	if idx >= len(args) {
		return protocol.EncodeError("ERR wrong number of arguments for 'xadd' command")
	}

	id := args[idx]
	idx++

	// Parse field-value pairs
	if (len(args)-idx)%2 != 0 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xadd' command")
	}

	fields := make(map[string]string)
	for idx < len(args) {
		field := args[idx]
		value := args[idx+1]
		fields[field] = value
		idx += 2
	}

	if len(fields) == 0 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xadd' command")
	}

	entryID, err := s.store.XAdd(key, id, fields, maxLen, approximate)
	if err != nil {
		return protocol.EncodeError(err.Error())
	}

	return protocol.EncodeBulkString(entryID)
}

// XLEN key
func (s *Server) handleXLen(args []string) string {
	if len(args) != 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xlen' command")
	}

	length := s.store.XLen(args[0])
	return protocol.EncodeInteger(int(length))
}

// XRANGE key start end [COUNT count]
func (s *Server) handleXRange(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xrange' command")
	}

	key := args[0]
	start := args[1]
	end := args[2]
	var count int64 = 0

	if len(args) > 3 {
		if len(args) != 5 || strings.ToUpper(args[3]) != "COUNT" {
			return protocol.EncodeError("ERR syntax error")
		}
		var err error
		count, err = strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return protocol.EncodeError("ERR value is not an integer or out of range")
		}
	}

	entries := s.store.XRange(key, start, end, count)
	return encodeStreamEntries(entries)
}

// XREVRANGE key end start [COUNT count]
func (s *Server) handleXRevRange(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xrevrange' command")
	}

	key := args[0]
	end := args[1]
	start := args[2]
	var count int64 = 0

	if len(args) > 3 {
		if len(args) != 5 || strings.ToUpper(args[3]) != "COUNT" {
			return protocol.EncodeError("ERR syntax error")
		}
		var err error
		count, err = strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return protocol.EncodeError("ERR value is not an integer or out of range")
		}
	}

	entries := s.store.XRevRange(key, end, start, count)
	return encodeStreamEntries(entries)
}

// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
func (s *Server) handleXRead(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xread' command")
	}

	var count int64 = 0
	idx := 0

	// Parse options
	for idx < len(args) {
		arg := strings.ToUpper(args[idx])
		switch arg {
		case "COUNT":
			idx++
			if idx >= len(args) {
				return protocol.EncodeError("ERR syntax error")
			}
			var err error
			count, err = strconv.ParseInt(args[idx], 10, 64)
			if err != nil {
				return protocol.EncodeError("ERR value is not an integer or out of range")
			}
			idx++
		case "BLOCK":
			// Skip BLOCK for now (would require async handling)
			idx += 2
		case "STREAMS":
			idx++
			goto parseStreams
		default:
			return protocol.EncodeError("ERR syntax error")
		}
	}

	return protocol.EncodeError("ERR syntax error, STREAMS is required")

parseStreams:
	remaining := args[idx:]
	if len(remaining) == 0 || len(remaining)%2 != 0 {
		return protocol.EncodeError("ERR Unbalanced 'xread' list of streams: for each stream key an ID must be specified")
	}

	numStreams := len(remaining) / 2
	keys := remaining[:numStreams]
	ids := remaining[numStreams:]

	result := s.store.XRead(keys, ids, count)
	if len(result) == 0 {
		return protocol.EncodeNull()
	}

	return encodeXReadResult(keys, result)
}

// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
func (s *Server) handleXTrim(args []string) string {
	if len(args) < 3 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xtrim' command")
	}

	key := args[0]
	strategy := strings.ToUpper(args[1])
	
	if strategy != "MAXLEN" && strategy != "MINID" {
		return protocol.EncodeError("ERR syntax error")
	}

	idx := 2
	approximate := false

	if args[idx] == "~" {
		approximate = true
		idx++
	} else if args[idx] == "=" {
		idx++
	}

	if idx >= len(args) {
		return protocol.EncodeError("ERR syntax error")
	}

	threshold, err := strconv.ParseInt(args[idx], 10, 64)
	if err != nil {
		return protocol.EncodeError("ERR value is not an integer or out of range")
	}

	if strategy == "MAXLEN" {
		deleted := s.store.XTrim(key, threshold, approximate)
		return protocol.EncodeInteger(int(deleted))
	}

	// MINID not fully implemented
	return protocol.EncodeInteger(0)
}

// XDEL key id [id ...]
func (s *Server) handleXDel(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xdel' command")
	}

	key := args[0]
	ids := args[1:]

	deleted := s.store.XDel(key, ids)
	return protocol.EncodeInteger(int(deleted))
}

// XINFO [CONSUMERS key groupname] [GROUPS key] [STREAM key] [HELP]
func (s *Server) handleXInfo(args []string) string {
	if len(args) < 1 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xinfo' command")
	}

	subcommand := strings.ToUpper(args[0])

	switch subcommand {
	case "STREAM":
		if len(args) < 2 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xinfo stream' command")
		}
		info, ok := s.store.XInfoStream(args[1])
		if !ok {
			return protocol.EncodeError("ERR no such key")
		}
		return encodeStreamInfo(info)
	case "GROUPS":
		if len(args) < 2 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xinfo groups' command")
		}
		// Return empty array for now
		return "*0\r\n"
	case "CONSUMERS":
		if len(args) < 3 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xinfo consumers' command")
		}
		// Return empty array for now
		return "*0\r\n"
	case "HELP":
		return encodeXInfoHelp()
	default:
		return protocol.EncodeError("ERR unknown subcommand '" + args[0] + "'. Try XINFO HELP.")
	}
}

// XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries_read]
// XGROUP DESTROY key groupname
// XGROUP CREATECONSUMER key groupname consumername
// XGROUP DELCONSUMER key groupname consumername
// XGROUP SETID key groupname id|$ [ENTRIESREAD entries_read]
func (s *Server) handleXGroup(args []string) string {
	if len(args) < 2 {
		return protocol.EncodeError("ERR wrong number of arguments for 'xgroup' command")
	}

	subcommand := strings.ToUpper(args[0])

	switch subcommand {
	case "CREATE":
		if len(args) < 4 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xgroup create' command")
		}
		key := args[1]
		group := args[2]
		id := args[3]
		mkstream := false

		for i := 4; i < len(args); i++ {
			if strings.ToUpper(args[i]) == "MKSTREAM" {
				mkstream = true
			}
		}

		err := s.store.XGroupCreate(key, group, id, mkstream)
		if err != nil {
			return protocol.EncodeError(err.Error())
		}
		return protocol.EncodeSimpleString("OK")

	case "DESTROY":
		if len(args) < 3 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xgroup destroy' command")
		}
		key := args[1]
		group := args[2]

		destroyed, err := s.store.XGroupDestroy(key, group)
		if err != nil {
			return protocol.EncodeError(err.Error())
		}
		if destroyed {
			return protocol.EncodeInteger(1)
		}
		return protocol.EncodeInteger(0)

	case "CREATECONSUMER":
		if len(args) < 4 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xgroup createconsumer' command")
		}
		// Simplified implementation
		return protocol.EncodeInteger(1)

	case "DELCONSUMER":
		if len(args) < 4 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xgroup delconsumer' command")
		}
		return protocol.EncodeInteger(0)

	case "SETID":
		if len(args) < 4 {
			return protocol.EncodeError("ERR wrong number of arguments for 'xgroup setid' command")
		}
		return protocol.EncodeSimpleString("OK")

	case "HELP":
		return encodeXGroupHelp()

	default:
		return protocol.EncodeError("ERR unknown subcommand '" + args[0] + "'. Try XGROUP HELP.")
	}
}

// Helper functions

func encodeStreamEntries(entries []store.StreamEntry) string {
	if len(entries) == 0 {
		return "*0\r\n"
	}

	var result strings.Builder
	result.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))

	for _, entry := range entries {
		// Each entry is [id, [field, value, ...]]
		result.WriteString("*2\r\n")
		result.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))

		// Fields array
		fieldCount := len(entry.Fields) * 2
		result.WriteString(fmt.Sprintf("*%d\r\n", fieldCount))
		for field, value := range entry.Fields {
			result.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(field), field))
			result.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
		}
	}

	return result.String()
}

func encodeXReadResult(keys []string, result map[string][]store.StreamEntry) string {
	// Count non-empty results
	count := 0
	for _, key := range keys {
		if _, ok := result[key]; ok {
			count++
		}
	}

	var resp strings.Builder
	resp.WriteString(fmt.Sprintf("*%d\r\n", count))

	for _, key := range keys {
		entries, ok := result[key]
		if !ok {
			continue
		}

		// Each stream result is [key, entries]
		resp.WriteString("*2\r\n")
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))
		resp.WriteString(encodeStreamEntries(entries))
	}

	return resp.String()
}

func encodeStreamInfo(info map[string]interface{}) string {
	// Simplified info encoding
	var result strings.Builder
	result.WriteString("*14\r\n")

	// length
	result.WriteString("$6\r\nlength\r\n")
	length := info["length"].(int64)
	result.WriteString(fmt.Sprintf(":%d\r\n", length))

	// radix-tree-keys
	result.WriteString("$15\r\nradix-tree-keys\r\n")
	result.WriteString(":1\r\n")

	// radix-tree-nodes
	result.WriteString("$16\r\nradix-tree-nodes\r\n")
	result.WriteString(":2\r\n")

	// last-generated-id
	result.WriteString("$17\r\nlast-generated-id\r\n")
	lastID := info["last-generated-id"].(string)
	if lastID == "" {
		lastID = "0-0"
	}
	result.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(lastID), lastID))

	// groups
	result.WriteString("$6\r\ngroups\r\n")
	groups := info["groups"].(int64)
	result.WriteString(fmt.Sprintf(":%d\r\n", groups))

	// first-entry
	result.WriteString("$11\r\nfirst-entry\r\n")
	if firstEntry := info["first-entry"]; firstEntry != nil {
		entry := firstEntry.(store.StreamEntry)
		result.WriteString(encodeStreamEntries([]store.StreamEntry{entry}))
	} else {
		result.WriteString("$-1\r\n")
	}

	// last-entry
	result.WriteString("$10\r\nlast-entry\r\n")
	if lastEntry := info["last-entry"]; lastEntry != nil {
		entry := lastEntry.(store.StreamEntry)
		result.WriteString(encodeStreamEntries([]store.StreamEntry{entry}))
	} else {
		result.WriteString("$-1\r\n")
	}

	return result.String()
}

func encodeXInfoHelp() string {
	help := []string{
		"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CONSUMERS <key> <groupname>",
		"    Show consumers of <groupname>.",
		"GROUPS <key>",
		"    Show the stream consumer groups.",
		"STREAM <key> [FULL [COUNT <count>]",
		"    Show information about the stream.",
		"HELP",
		"    Print this help.",
	}
	return protocol.EncodeStringArray(help)
}

func encodeXGroupHelp() string {
	help := []string{
		"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CREATE <key> <groupname> <id|$> [MKSTREAM] [ENTRIESREAD <entries_read>]",
		"    Create a new consumer group.",
		"DESTROY <key> <groupname>",
		"    Destroy a consumer group.",
		"CREATECONSUMER <key> <groupname> <consumername>",
		"    Create a consumer in a consumer group.",
		"DELCONSUMER <key> <groupname> <consumername>",
		"    Delete a consumer from a consumer group.",
		"SETID <key> <groupname> <id|$> [ENTRIESREAD <entries_read>]",
		"    Set the current group ID.",
		"HELP",
		"    Print this help.",
	}
	return protocol.EncodeStringArray(help)
}

