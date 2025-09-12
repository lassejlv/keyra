package server

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
)

type PubSubMessage struct {
	Type    string // "message", "pmessage", "subscribe", "unsubscribe", "psubscribe", "punsubscribe"
	Channel string
	Pattern string // For pattern messages
	Data    string
	Count   int // Current subscription count
}

type Subscriber struct {
	conn         net.Conn
	connKey      string
	channels     map[string]bool
	patterns     map[string]*regexp.Regexp
	messageChan  chan PubSubMessage
	quit         chan bool
	mu           sync.RWMutex
}

type PubSubSystem struct {
	mu              sync.RWMutex
	subscribers     map[string]*Subscriber // connKey -> Subscriber
	channelSubs     map[string]map[string]*Subscriber // channel -> connKey -> Subscriber
	patternSubs     map[string]map[string]*Subscriber // pattern -> connKey -> Subscriber
	messageCount    int64
	subscribeCount  int64
}

func NewPubSubSystem() *PubSubSystem {
	return &PubSubSystem{
		subscribers: make(map[string]*Subscriber),
		channelSubs: make(map[string]map[string]*Subscriber),
		patternSubs: make(map[string]map[string]*Subscriber),
	}
}

func (ps *PubSubSystem) GetSubscriber(connKey string, conn net.Conn) *Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	if sub, exists := ps.subscribers[connKey]; exists {
		return sub
	}
	
	sub := &Subscriber{
		conn:        conn,
		connKey:     connKey,
		channels:    make(map[string]bool),
		patterns:    make(map[string]*regexp.Regexp),
		messageChan: make(chan PubSubMessage, 1000),
		quit:        make(chan bool, 1),
	}
	
	ps.subscribers[connKey] = sub
	go sub.messageLoop()
	
	return sub
}

func (ps *PubSubSystem) RemoveSubscriber(connKey string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	sub, exists := ps.subscribers[connKey]
	if !exists {
		return
	}
	
	// Remove from all channel subscriptions
	for channel := range sub.channels {
		if subs, exists := ps.channelSubs[channel]; exists {
			delete(subs, connKey)
			if len(subs) == 0 {
				delete(ps.channelSubs, channel)
			}
		}
	}
	
	// Remove from all pattern subscriptions
	for pattern := range sub.patterns {
		if subs, exists := ps.patternSubs[pattern]; exists {
			delete(subs, connKey)
			if len(subs) == 0 {
				delete(ps.patternSubs, pattern)
			}
		}
	}
	
	// Signal subscriber to quit and clean up
	close(sub.quit)
	delete(ps.subscribers, connKey)
}

func (ps *PubSubSystem) Subscribe(connKey string, conn net.Conn, channels []string) []PubSubMessage {
	sub := ps.GetSubscriber(connKey, conn)
	var responses []PubSubMessage
	
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	for _, channel := range channels {
		if !sub.channels[channel] {
			sub.channels[channel] = true
			
			if ps.channelSubs[channel] == nil {
				ps.channelSubs[channel] = make(map[string]*Subscriber)
			}
			ps.channelSubs[channel][connKey] = sub
			ps.subscribeCount++
		}
		
		responses = append(responses, PubSubMessage{
			Type:    "subscribe",
			Channel: channel,
			Count:   len(sub.channels) + len(sub.patterns),
		})
	}
	
	return responses
}

func (ps *PubSubSystem) Unsubscribe(connKey string, channels []string) []PubSubMessage {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	sub, exists := ps.subscribers[connKey]
	if !exists {
		return nil
	}
	
	var responses []PubSubMessage
	
	// If no channels specified, unsubscribe from all channels
	if len(channels) == 0 {
		for channel := range sub.channels {
			channels = append(channels, channel)
		}
	}
	
	for _, channel := range channels {
		if sub.channels[channel] {
			delete(sub.channels, channel)
			
			if subs, exists := ps.channelSubs[channel]; exists {
				delete(subs, connKey)
				if len(subs) == 0 {
					delete(ps.channelSubs, channel)
				}
			}
			ps.subscribeCount--
		}
		
		responses = append(responses, PubSubMessage{
			Type:    "unsubscribe",
			Channel: channel,
			Count:   len(sub.channels) + len(sub.patterns),
		})
	}
	
	return responses
}

func (ps *PubSubSystem) PSubscribe(connKey string, conn net.Conn, patterns []string) []PubSubMessage {
	sub := ps.GetSubscriber(connKey, conn)
	var responses []PubSubMessage
	
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	for _, pattern := range patterns {
		if _, exists := sub.patterns[pattern]; !exists {
			// Convert Redis pattern to Go regexp
			regexPattern := ps.redisPatternToRegex(pattern)
			if regex, err := regexp.Compile(regexPattern); err == nil {
				sub.patterns[pattern] = regex
				
				if ps.patternSubs[pattern] == nil {
					ps.patternSubs[pattern] = make(map[string]*Subscriber)
				}
				ps.patternSubs[pattern][connKey] = sub
				ps.subscribeCount++
			}
		}
		
		responses = append(responses, PubSubMessage{
			Type:    "psubscribe",
			Pattern: pattern,
			Count:   len(sub.channels) + len(sub.patterns),
		})
	}
	
	return responses
}

func (ps *PubSubSystem) PUnsubscribe(connKey string, patterns []string) []PubSubMessage {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	sub, exists := ps.subscribers[connKey]
	if !exists {
		return nil
	}
	
	var responses []PubSubMessage
	
	// If no patterns specified, unsubscribe from all patterns
	if len(patterns) == 0 {
		for pattern := range sub.patterns {
			patterns = append(patterns, pattern)
		}
	}
	
	for _, pattern := range patterns {
		if _, exists := sub.patterns[pattern]; exists {
			delete(sub.patterns, pattern)
			
			if subs, exists := ps.patternSubs[pattern]; exists {
				delete(subs, connKey)
				if len(subs) == 0 {
					delete(ps.patternSubs, pattern)
				}
			}
			ps.subscribeCount--
		}
		
		responses = append(responses, PubSubMessage{
			Type:    "punsubscribe",
			Pattern: pattern,
			Count:   len(sub.channels) + len(sub.patterns),
		})
	}
	
	return responses
}

func (ps *PubSubSystem) Publish(channel string, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	
	recipients := 0
	ps.messageCount++
	
	// Send to direct channel subscribers
	if subs, exists := ps.channelSubs[channel]; exists {
		for _, sub := range subs {
			select {
			case sub.messageChan <- PubSubMessage{
				Type:    "message",
				Channel: channel,
				Data:    message,
			}:
				recipients++
			default:
				// Channel is full, skip this subscriber
			}
		}
	}
	
	// Send to pattern subscribers
	for pattern, subs := range ps.patternSubs {
		for _, sub := range subs {
			if regex := sub.patterns[pattern]; regex != nil && regex.MatchString(channel) {
				select {
				case sub.messageChan <- PubSubMessage{
					Type:    "pmessage",
					Pattern: pattern,
					Channel: channel,
					Data:    message,
				}:
					recipients++
				default:
					// Channel is full, skip this subscriber
				}
			}
		}
	}
	
	return recipients
}

func (ps *PubSubSystem) GetChannels(pattern string) []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	
	var channels []string
	
	if pattern == "" || pattern == "*" {
		for channel := range ps.channelSubs {
			channels = append(channels, channel)
		}
	} else {
		regexPattern := ps.redisPatternToRegex(pattern)
		if regex, err := regexp.Compile(regexPattern); err == nil {
			for channel := range ps.channelSubs {
				if regex.MatchString(channel) {
					channels = append(channels, channel)
				}
			}
		}
	}
	
	return channels
}

func (ps *PubSubSystem) GetNumSub(channels []string) map[string]int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	
	result := make(map[string]int)
	
	for _, channel := range channels {
		if subs, exists := ps.channelSubs[channel]; exists {
			result[channel] = len(subs)
		} else {
			result[channel] = 0
		}
	}
	
	return result
}

func (ps *PubSubSystem) GetNumPat() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	
	return len(ps.patternSubs)
}

func (ps *PubSubSystem) redisPatternToRegex(pattern string) string {
	// Convert Redis glob pattern to Go regex
	// * matches any sequence of characters
	// ? matches any single character
	// [abc] matches any character in the set
	// [^abc] matches any character not in the set
	
	escaped := regexp.QuoteMeta(pattern)
	escaped = strings.ReplaceAll(escaped, `\*`, ".*")
	escaped = strings.ReplaceAll(escaped, `\?`, ".")
	escaped = strings.ReplaceAll(escaped, `\[`, "[")
	escaped = strings.ReplaceAll(escaped, `\]`, "]")
	escaped = strings.ReplaceAll(escaped, `\^`, "^")
	
	return "^" + escaped + "$"
}

func (sub *Subscriber) messageLoop() {
	for {
		select {
		case msg := <-sub.messageChan:
			sub.sendMessage(msg)
		case <-sub.quit:
			return
		}
	}
}

func (sub *Subscriber) sendMessage(msg PubSubMessage) {
	var response string
	
	switch msg.Type {
	case "message":
		response = fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(msg.Channel), msg.Channel, len(msg.Data), msg.Data)
	case "pmessage":
		response = fmt.Sprintf("*4\r\n$8\r\npmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(msg.Pattern), msg.Pattern, len(msg.Channel), msg.Channel, len(msg.Data), msg.Data)
	case "subscribe":
		response = fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(msg.Channel), msg.Channel, msg.Count)
	case "unsubscribe":
		response = fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(msg.Channel), msg.Channel, msg.Count)
	case "psubscribe":
		response = fmt.Sprintf("*3\r\n$10\r\npsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(msg.Pattern), msg.Pattern, msg.Count)
	case "punsubscribe":
		response = fmt.Sprintf("*3\r\n$12\r\npunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(msg.Pattern), msg.Pattern, msg.Count)
	}
	
	if response != "" {
		sub.conn.Write([]byte(response))
	}
}

func (sub *Subscriber) IsSubscribed() bool {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	return len(sub.channels) > 0 || len(sub.patterns) > 0
}

func (sub *Subscriber) GetSubscriptionCount() int {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	return len(sub.channels) + len(sub.patterns)
}
