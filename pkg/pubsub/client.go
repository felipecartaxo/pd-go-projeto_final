package pubsub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

var (
	ErrUnavailable = errors.New("no broker is available")
	ErrClosed      = errors.New("pubsub client is closed")
)

type ProtocolError struct{ Code string }

func (e *ProtocolError) Error() string { return e.Code }

type Config struct {
	Brokers               [2]string
	ClientID              string
	PreferredBroker       int
	HeartbeatInterval     time.Duration
	HeartbeatTimeout      time.Duration
	ControlRetryInterval  time.Duration
	ControlMaxAttempts    int
	DeliveryQueueCapacity int
	EventQueueCapacity    int
}
type Message struct {
	Topic, MessageID string
	Payload          json.RawMessage
}
type EventType string

const (
	EventNoSubscribers EventType = "NoSubscribers"
	EventRateLimited   EventType = "RateLimited"
	EventBrokerChanged EventType = "BrokerChanged"
	EventUnavailable   EventType = "Unavailable"
	EventError         EventType = "Error"
)

type Event struct {
	Type                              EventType
	RequestID, MessageID, Topic, Code string
	Err                               error
}
type subscription struct{ ch chan Message }
type Client struct {
	cfg                  Config
	mu                   sync.Mutex
	conn                 *net.UDPConn
	active               int
	closed               bool
	pending              map[string]chan protocol.Envelope
	subs                 map[string]*subscription
	restoring            map[string]bool
	brokerChangedPending bool
	events               chan Event
	done                 chan struct{}
	wg                   sync.WaitGroup
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.ClientID == "" || cfg.Brokers[0] == "" || cfg.Brokers[1] == "" {
		return nil, fmt.Errorf("client_id and two broker addresses are required")
	}
	for _, address := range cfg.Brokers {
		if _, err := net.ResolveUDPAddr("udp", address); err != nil {
			return nil, err
		}
	}
	if cfg.PreferredBroker != 1 && cfg.PreferredBroker != 2 {
		cfg.PreferredBroker = 1
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 500 * time.Millisecond
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 1800 * time.Millisecond
	}
	if cfg.ControlRetryInterval <= 0 {
		cfg.ControlRetryInterval = 300 * time.Millisecond
	}
	if cfg.ControlMaxAttempts <= 0 {
		cfg.ControlMaxAttempts = 3
	}
	if cfg.DeliveryQueueCapacity <= 0 {
		cfg.DeliveryQueueCapacity = 64
	}
	if cfg.EventQueueCapacity <= 0 {
		cfg.EventQueueCapacity = 64
	}
	return &Client{
		cfg:       cfg,
		active:    -1,
		pending:   make(map[string]chan protocol.Envelope),
		subs:      make(map[string]*subscription),
		restoring: make(map[string]bool),
		events:    make(chan Event, cfg.EventQueueCapacity),
		done:      make(chan struct{}),
	}, nil
}

func (c *Client) Connect(ctx context.Context) error {
	order := []int{c.cfg.PreferredBroker - 1, 1 - (c.cfg.PreferredBroker - 1)}
	var last error
	for _, index := range order {
		if err := c.activate(ctx, index); err == nil {
			c.wg.Add(1)
			go c.heartbeatLoop()
			return nil
		} else {
			last = err
		}
	}
	if last != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, last)
	}
	return ErrUnavailable
}
func (c *Client) activate(ctx context.Context, index int) error {
	address, err := net.ResolveUDPAddr("udp", c.cfg.Brokers[index])
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, address)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		conn.Close()
		return ErrClosed
	}
	old := c.conn
	oldActive := c.active
	c.conn = conn
	c.active = index
	c.mu.Unlock()
	c.wg.Add(1)
	go c.readLoop(conn)
	if err := c.control(ctx, protocol.TypeRegister, ""); err != nil {
		conn.Close()
		c.mu.Lock()
		c.conn = old
		c.active = oldActive
		c.mu.Unlock()
		return err
	}
	if old != nil {
		old.Close()
	}
	return nil
}
func (c *Client) readLoop(conn *net.UDPConn) {
	defer c.wg.Done()
	buffer := make([]byte, protocol.MaxDatagramSize+1)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		env, err := protocol.Decode(buffer[:n])
		if err != nil {
			continue
		}
		c.dispatch(env)
	}
}
func (c *Client) dispatch(env protocol.Envelope) {
	c.mu.Lock()
	pending := c.pending[env.RequestID]
	c.mu.Unlock()
	if pending != nil && (env.Type == protocol.TypeOK || env.Type == protocol.TypeError || env.Type == protocol.TypeHeartbeat) {
		select {
		case pending <- env:
		default:
		}
		return
	}
	switch env.Type {
	case protocol.TypeDeliver:
		c.mu.Lock()
		sub := c.subs[env.Topic]
		c.mu.Unlock()
		if sub != nil {
			msg := Message{Topic: env.Topic, MessageID: env.MessageID, Payload: append(json.RawMessage(nil), env.Payload...)}
			select {
			case sub.ch <- msg:
			default:
			}
		}
	case protocol.TypeNoSubscribers:
		c.sendBestEffort(protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeACK, RequestID: env.RequestID, ClientID: c.cfg.ClientID})
		c.emit(Event{Type: EventNoSubscribers, RequestID: env.RequestID, MessageID: env.MessageID, Topic: env.Topic})
	case protocol.TypeRateLimited:
		c.emit(Event{Type: EventRateLimited, RequestID: env.RequestID, MessageID: env.MessageID, Topic: env.Topic, Code: string(protocol.CodeRateLimited)})
	case protocol.TypeError:
		c.emit(Event{Type: EventError, RequestID: env.RequestID, Code: string(env.Code), Err: &ProtocolError{Code: string(env.Code)}})
	}
}
func (c *Client) emit(event Event) {
	select {
	case c.events <- event:
	default:
	}
}
func (c *Client) Events() <-chan Event { return c.events }

// ActiveBroker returns the 1-based broker currently selected by this client.
// It returns 0 while no broker is active.
func (c *Client) ActiveBroker() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.conn == nil {
		return 0
	}
	return c.active + 1
}

func (c *Client) control(ctx context.Context, typ protocol.Type, topic string) error {
	env := protocol.Envelope{Version: protocol.Version1, Type: typ, RequestID: newID(), ClientID: c.cfg.ClientID, Topic: topic}
	if typ == protocol.TypeHeartbeat {
		reply := false
		env.Reply = &reply
	}
	ch := make(chan protocol.Envelope, 1)
	c.mu.Lock()
	if c.closed || c.conn == nil {
		c.mu.Unlock()
		return ErrUnavailable
	}
	c.pending[env.RequestID] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, env.RequestID); c.mu.Unlock() }()
	for attempt := 0; attempt < c.cfg.ControlMaxAttempts; attempt++ {
		if err := c.sendBestEffort(env); err != nil {
			return err
		}
		timer := time.NewTimer(c.cfg.ControlRetryInterval)
		select {
		case response := <-ch:
			timer.Stop()
			if response.Type == protocol.TypeError {
				return &ProtocolError{Code: string(response.Code)}
			}
			return nil
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrInconclusive
}
func (c *Client) sendBestEffort(env protocol.Envelope) error {
	data, err := protocol.Encode(env)
	if err != nil {
		return err
	}
	c.mu.Lock()
	conn := c.conn
	closed := c.closed
	c.mu.Unlock()
	if closed || conn == nil {
		return ErrClosed
	}
	_, err = conn.Write(data)
	return err
}
func (c *Client) CreateTopic(ctx context.Context, topic string) error {
	return c.control(ctx, protocol.TypeCreateTopic, topic)
}
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	return c.control(ctx, protocol.TypeDeleteTopic, topic)
}
func (c *Client) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	c.mu.Lock()
	if sub := c.subs[topic]; sub != nil {
		c.mu.Unlock()
		return sub.ch, nil
	}
	c.mu.Unlock()
	if err := c.control(ctx, protocol.TypeSubscribe, topic); err != nil {
		return nil, err
	}
	ch := make(chan Message, c.cfg.DeliveryQueueCapacity)
	c.mu.Lock()
	c.subs[topic] = &subscription{ch: ch}
	delete(c.restoring, topic)
	c.mu.Unlock()
	return ch, nil
}
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	if err := c.control(ctx, protocol.TypeUnsubscribe, topic); err != nil {
		return err
	}
	c.mu.Lock()
	if sub := c.subs[topic]; sub != nil {
		delete(c.subs, topic)
		close(sub.ch)
	}
	delete(c.restoring, topic)
	c.mu.Unlock()
	return nil
}
func (c *Client) Publish(ctx context.Context, topic string, payload json.RawMessage) (string, error) {
	if !json.Valid(payload) {
		return "", fmt.Errorf("payload is not JSON")
	}
	env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePublish, RequestID: newID(), ClientID: c.cfg.ClientID, Topic: topic, MessageID: newID(), Payload: payload}
	if _, err := protocol.Encode(env); err != nil {
		return "", err
	}
	if err := c.sendBestEffort(env); err != nil {
		return "", err
	}
	return env.MessageID, nil
}
func (c *Client) heartbeatLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HeartbeatTimeout)
			err := c.control(ctx, protocol.TypeHeartbeat, "")
			if err != nil {
				cancel()
				c.failover()
				continue
			}
			restoreErr := c.restoreSubscriptions(ctx)
			cancel()
			if restoreErr != nil {
				c.emit(Event{Type: EventUnavailable, Err: restoreErr})
				continue
			}
			c.mu.Lock()
			changed := c.brokerChangedPending
			c.brokerChangedPending = false
			c.mu.Unlock()
			if changed {
				c.emit(Event{Type: EventBrokerChanged})
			}
		}
	}
}
func (c *Client) failover() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	next := 1 - c.active
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HeartbeatTimeout)
	defer cancel()
	if err := c.activate(ctx, next); err != nil {
		c.emit(Event{Type: EventUnavailable, Err: err})
		return
	}
	c.mu.Lock()
	for topic := range c.subs {
		c.restoring[topic] = true
	}
	c.brokerChangedPending = true
	c.mu.Unlock()
	if err := c.restoreSubscriptions(ctx); err != nil {
		c.emit(Event{Type: EventUnavailable, Err: err})
		return
	}
	c.emit(Event{Type: EventBrokerChanged})
	c.mu.Lock()
	c.brokerChangedPending = false
	c.mu.Unlock()
}

func (c *Client) restoreSubscriptions(ctx context.Context) error {
	c.mu.Lock()
	topics := make([]string, 0, len(c.restoring))
	for topic := range c.restoring {
		topics = append(topics, topic)
	}
	c.mu.Unlock()

	var restoreErr error
	for _, topic := range topics {
		if err := c.control(ctx, protocol.TypeSubscribe, topic); err != nil {
			if restoreErr == nil {
				restoreErr = err
			}
			continue
		}
		c.mu.Lock()
		delete(c.restoring, topic)
		c.mu.Unlock()
	}
	return restoreErr
}
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	conn := c.conn
	c.conn = nil
	for _, sub := range c.subs {
		close(sub.ch)
	}
	c.subs = map[string]*subscription{}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	c.wg.Wait()
	close(c.events)
	return nil
}
func newID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
