package control

import (
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

// ReplyKey identifies one control operation in the scope required by the wire contract.
type ReplyKey struct {
	Type      protocol.Type
	Origin    string
	RequestID string
}

type cachedReply struct {
	response  protocol.Envelope
	expiresAt time.Time
}

// ReplyCache keeps idempotent control replies for a finite window and capacity.
// It is intended to be protected by its caller's mutex.
type ReplyCache struct {
	capacity int
	ttl      time.Duration
	now      func() time.Time
	entries  map[ReplyKey]cachedReply
	order    []ReplyKey
}

func NewReplyCache(capacity int, ttl time.Duration, now func() time.Time) *ReplyCache {
	if now == nil {
		now = time.Now
	}
	return &ReplyCache{capacity: capacity, ttl: ttl, now: now, entries: make(map[ReplyKey]cachedReply)}
}

func (c *ReplyCache) Get(key ReplyKey) (protocol.Envelope, bool) {
	entry, ok := c.entries[key]
	if !ok {
		return protocol.Envelope{}, false
	}
	if !c.now().Before(entry.expiresAt) {
		delete(c.entries, key)
		c.removeOrder(key)
		return protocol.Envelope{}, false
	}
	return entry.response, true
}

func (c *ReplyCache) Put(key ReplyKey, response protocol.Envelope) {
	if c.capacity <= 0 || c.ttl <= 0 {
		return
	}
	if _, exists := c.entries[key]; exists {
		return
	}
	for len(c.entries) >= c.capacity && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		if _, exists := c.entries[oldest]; exists {
			delete(c.entries, oldest)
		}
	}
	c.entries[key] = cachedReply{response: response, expiresAt: c.now().Add(c.ttl)}
	c.order = append(c.order, key)
}

func (c *ReplyCache) removeOrder(key ReplyKey) {
	for index, candidate := range c.order {
		if candidate == key {
			copy(c.order[index:], c.order[index+1:])
			c.order = c.order[:len(c.order)-1]
			return
		}
	}
}
