package broker

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"time"

	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/protocol"
)

// Config contains the fixed-peer addresses and bounded process settings for one broker.
type Config struct {
	ID                   int
	ListenAddress        string
	PeerAddress          string
	SessionTTL           time.Duration
	HeartbeatInterval    time.Duration
	HeartbeatTimeout     time.Duration
	MaxDatagramBytes     int
	RateLimitPerSecond   int
	RateBurst            int
	InputQueueCapacity   int
	OutputQueueCapacity  int
	ControlRetryInterval time.Duration
	ControlMaxAttempts   int
}

// DefaultConfig supplies local-only defaults. They are implementation defaults, not
// requirements of the project specification.
func DefaultConfig(id int) Config {
	config := Config{
		ID:                   id,
		SessionTTL:           4 * time.Second,
		HeartbeatInterval:    500 * time.Millisecond,
		HeartbeatTimeout:     1800 * time.Millisecond,
		MaxDatagramBytes:     protocol.MaxDatagramSize,
		RateLimitPerSecond:   200,
		RateBurst:            100,
		InputQueueCapacity:   64,
		OutputQueueCapacity:  64,
		ControlRetryInterval: 300 * time.Millisecond,
		ControlMaxAttempts:   3,
	}
	switch id {
	case 1:
		config.ListenAddress = "127.0.0.1:9001"
		config.PeerAddress = "127.0.0.1:9002"
	case 2:
		config.ListenAddress = "127.0.0.1:9002"
		config.PeerAddress = "127.0.0.1:9001"
	}
	return config
}

// ParseConfig parses cmd/broker flags and selects defaults consistent with the chosen ID.
func ParseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)
	fs.SetOutput(nil)
	id := fs.Int("id", 1, "broker identifier: 1 or 2")
	listen := fs.String("listen", "", "UDP listen address")
	peer := fs.String("peer", "", "fixed UDP address of the other broker")
	sessionTTL := fs.Duration("session-ttl", 0, "session TTL")
	heartbeatInterval := fs.Duration("heartbeat-interval", 0, "heartbeat interval")
	heartbeatTimeout := fs.Duration("heartbeat-timeout", 0, "heartbeat timeout")
	maxDatagramBytes := fs.Int("max-datagram-bytes", 0, "maximum complete JSON envelope size")
	rateLimit := fs.Int("rate-limit-msgs-per-sec", 0, "per-publisher rate limit")
	rateBurst := fs.Int("rate-burst", 0, "per-publisher token burst")
	inputQueue := fs.Int("input-queue-capacity", 0, "per-publisher input queue capacity")
	outputQueue := fs.Int("output-queue-capacity", 0, "per-subscriber output queue capacity")
	retryInterval := fs.Duration("control-retry-interval", 0, "control retry interval")
	maxAttempts := fs.Int("control-max-attempts", 0, "total control send attempts")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	config := DefaultConfig(*id)
	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { visited[f.Name] = true })
	if visited["listen"] {
		config.ListenAddress = *listen
	}
	if visited["peer"] {
		config.PeerAddress = *peer
	}
	if visited["session-ttl"] {
		config.SessionTTL = *sessionTTL
	}
	if visited["heartbeat-interval"] {
		config.HeartbeatInterval = *heartbeatInterval
	}
	if visited["heartbeat-timeout"] {
		config.HeartbeatTimeout = *heartbeatTimeout
	}
	if visited["max-datagram-bytes"] {
		config.MaxDatagramBytes = *maxDatagramBytes
	}
	if visited["rate-limit-msgs-per-sec"] {
		config.RateLimitPerSecond = *rateLimit
	}
	if visited["rate-burst"] {
		config.RateBurst = *rateBurst
	}
	if visited["input-queue-capacity"] {
		config.InputQueueCapacity = *inputQueue
	}
	if visited["output-queue-capacity"] {
		config.OutputQueueCapacity = *outputQueue
	}
	if visited["control-retry-interval"] {
		config.ControlRetryInterval = *retryInterval
	}
	if visited["control-max-attempts"] {
		config.ControlMaxAttempts = *maxAttempts
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Validate rejects unsupported IDs, invalid UDP endpoints, and unbounded/invalid limits.
func (c Config) Validate() error {
	if c.ID != 1 && c.ID != 2 {
		return fmt.Errorf("broker id must be 1 or 2, got %d", c.ID)
	}
	if _, err := net.ResolveUDPAddr("udp", c.ListenAddress); err != nil {
		return fmt.Errorf("invalid listen address %q: %w", c.ListenAddress, err)
	}
	if _, err := net.ResolveUDPAddr("udp", c.PeerAddress); err != nil {
		return fmt.Errorf("invalid peer address %q: %w", c.PeerAddress, err)
	}
	if c.MaxDatagramBytes != protocol.MaxDatagramSize {
		return fmt.Errorf("max datagram bytes must be %d for protocol v1", protocol.MaxDatagramSize)
	}
	if c.SessionTTL <= 0 || c.HeartbeatInterval <= 0 || c.HeartbeatTimeout <= 0 || c.ControlRetryInterval <= 0 {
		return errors.New("timeouts and intervals must be positive")
	}
	if c.RateLimitPerSecond <= 0 || c.RateBurst <= 0 || c.InputQueueCapacity <= 0 || c.OutputQueueCapacity <= 0 || c.ControlMaxAttempts <= 0 {
		return errors.New("rates, capacities and control attempts must be positive")
	}
	return nil
}
