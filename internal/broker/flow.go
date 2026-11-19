package broker

import (
	"net"
	"time"

	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/protocol"
)

// Metrics records local processing and send attempts; it never claims receipt by a subscriber.
type Metrics struct {
	PublishedReceived     uint64
	PublishedAccepted     uint64
	RateLimited           uint64
	InputDropped          uint64
	OutputDropped         uint64
	NoSubscribers         uint64
	NoSubscriberRetries   uint64
	NoSubscriberTimeouts  uint64
	DeliverAttempts       uint64
	DeliveryWriteFailures uint64
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func newTokenBucket(now time.Time, burst int) tokenBucket {
	return tokenBucket{tokens: float64(burst), last: now}
}

func (b *tokenBucket) allow(now time.Time, rate, burst int) bool {
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * float64(rate)
		if b.tokens > float64(burst) {
			b.tokens = float64(burst)
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// messageQueue is bounded and intentionally drops the oldest data item when full.
// All access is protected by brokerState.mu.
type messageQueue struct {
	capacity int
	items    []protocol.Envelope
}

func newMessageQueue(capacity int) *messageQueue {
	return &messageQueue{capacity: capacity, items: make([]protocol.Envelope, 0, capacity)}
}

func (q *messageQueue) push(env protocol.Envelope) (dropped bool) {
	if len(q.items) == q.capacity {
		copy(q.items, q.items[1:])
		q.items[len(q.items)-1] = cloneEnvelope(env)
		return true
	}
	q.items = append(q.items, cloneEnvelope(env))
	return false
}

func (q *messageQueue) pop() (protocol.Envelope, bool) {
	if len(q.items) == 0 {
		return protocol.Envelope{}, false
	}
	item := q.items[0]
	copy(q.items, q.items[1:])
	q.items[len(q.items)-1] = protocol.Envelope{}
	q.items = q.items[:len(q.items)-1]
	return item, true
}

func (q *messageQueue) removeTopic(topic string) {
	kept := q.items[:0]
	for _, item := range q.items {
		if item.Topic != topic {
			kept = append(kept, item)
		}
	}
	for index := len(kept); index < len(q.items); index++ {
		q.items[index] = protocol.Envelope{}
	}
	q.items = kept
}

type pendingNotice struct {
	envelope protocol.Envelope
	address  *net.UDPAddr
	attempts int
	nextTry  time.Time
}

type outbound struct {
	address  *net.UDPAddr
	envelope protocol.Envelope
}

func cloneEnvelope(env protocol.Envelope) protocol.Envelope {
	clone := env
	clone.Payload = append([]byte(nil), env.Payload...)
	clone.Items = append([]byte(nil), env.Items...)
	return clone
}
