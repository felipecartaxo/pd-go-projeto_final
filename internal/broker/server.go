// Package broker contains the UDP process lifecycle. Message handling is added in later tasks.
package broker

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/control"
	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/protocol"
)

// Server owns one UDP listener and can be shut down through its context or Close.
type Server struct {
	config Config
	conn   *net.UDPConn
	state  *brokerState

	closeOnce sync.Once
	closeErr  error
}

// NewServer validates config and binds its UDP socket. It does not start protocol processing.
func NewServer(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	address, err := net.ResolveUDPAddr("udp", config.ListenAddress)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", address)
	if err != nil {
		return nil, err
	}
	return &Server{config: config, conn: conn, state: newBrokerState(config)}, nil
}

// Addr returns the local address assigned to the UDP socket.
func (s *Server) Addr() *net.UDPAddr {
	return s.conn.LocalAddr().(*net.UDPAddr)
}

// Serve validates, schedules, and sends local protocol traffic until context cancellation or Close.
// It has one receive/dispatch loop; no goroutine is created for a publication or delivery.
func (s *Server) Serve(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-stopped:
		}
	}()
	defer close(stopped)

	// One extra byte lets Decode reject oversized application datagrams instead of
	// silently accepting a truncated 1200-byte prefix.
	buffer := make([]byte, s.config.MaxDatagramBytes+1)
	for {
		if err := s.conn.SetReadDeadline(time.Now().Add(s.readInterval())); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		n, source, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() {
				s.expireSessions()
				s.writeOutbounds(s.collectOutbounds(s.state.now()))
				continue
			}
			return err
		}
		env, err := protocol.Decode(buffer[:n])
		if err != nil {
			continue
		}
		response, target, hasResponse := s.handle(env, source)
		if hasResponse {
			s.writeOutbounds([]outbound{{address: target, envelope: response}})
		}
		s.writeOutbounds(s.collectOutbounds(s.state.now()))
	}
}

// Close releases the UDP socket. It is safe to call repeatedly.
func (s *Server) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.conn.Close() })
	return s.closeErr
}

func (s *Server) sessionSweepInterval() time.Duration {
	interval := s.config.SessionTTL / 2
	if interval < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	return interval
}

func (s *Server) readInterval() time.Duration {
	interval := s.sessionSweepInterval()
	if s.config.ControlRetryInterval < interval {
		interval = s.config.ControlRetryInterval
	}
	// Queued data is drained promptly after a burst without spinning while idle.
	s.state.mu.Lock()
	hasData := len(s.state.publisherRR) > 0
	s.state.mu.Unlock()
	if hasData && interval > 5*time.Millisecond {
		return 5 * time.Millisecond
	}
	return interval
}

func (s *Server) expireSessions() {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.expireSessionsLocked(s.state.now())
}

// handle updates one client-originated request. Data delivery is later scheduled by
// collectOutbounds so no UDP I/O is performed while the state lock is held.
func (s *Server) handle(env protocol.Envelope, source *net.UDPAddr) (protocol.Envelope, *net.UDPAddr, bool) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	now := s.state.now()
	s.state.expireSessionsLocked(now)
	key := control.ReplyKey{Type: env.Type, Origin: source.String(), RequestID: env.RequestID}

	if env.Type == protocol.TypeRegister {
		// A cached registration remains valid only while that same binding exists.
		// Once its session expired, the retried REGISTER must recreate it.
		if response, ok := s.state.replies.Get(key); ok && s.state.sessionMatchesLocked(env.ClientID, source) {
			return response, s.state.replyAddressLocked(env.ClientID, source), true
		}
		s.state.registerLocked(env.ClientID, source, now)
		response := okReply(env)
		s.state.replies.Put(key, response)
		return response, s.state.replyAddressLocked(env.ClientID, source), true
	}

	if !s.state.sessionMatchesLocked(env.ClientID, source) {
		return errorReply(env, protocol.CodeNotRegistered), cloneUDPAddr(source), true
	}
	target := s.state.replyAddressLocked(env.ClientID, source)
	if env.Type == protocol.TypePublish {
		response, hasResponse := s.state.publishLocked(env, now)
		return response, target, hasResponse
	}
	if env.Type == protocol.TypeACK {
		s.state.acknowledgeNoticeLocked(env.ClientID, env.RequestID)
		return protocol.Envelope{}, nil, false
	}
	if response, ok := s.state.replies.Get(key); ok {
		return response, target, true
	}

	var response protocol.Envelope
	switch env.Type {
	case protocol.TypeHeartbeat:
		reply := true
		response = protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeHeartbeat, RequestID: env.RequestID, ClientID: env.ClientID, Reply: &reply}
		s.state.sessions[env.ClientID].lastSeen = now
	case protocol.TypeCreateTopic:
		response = s.state.createTopicLocked(s.config.ID, env)
	case protocol.TypeDeleteTopic:
		response = s.state.deleteTopicLocked(s.config.ID, env)
	case protocol.TypeSubscribe:
		response = s.state.subscribeLocked(env)
	case protocol.TypeUnsubscribe:
		response = s.state.unsubscribeLocked(env)
	default:
		return protocol.Envelope{}, nil, false
	}
	s.state.replies.Put(key, response)
	return response, target, true
}

func (s *Server) collectOutbounds(now time.Time) []outbound {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.expireSessionsLocked(now)
	return s.state.collectOutboundsLocked(now)
}

func (s *Server) writeOutbounds(outbounds []outbound) {
	for _, next := range outbounds {
		encoded, err := protocol.Encode(next.envelope)
		if err != nil {
			continue
		}
		// A successful UDP write is only a local send attempt, not subscriber receipt.
		if _, err := s.conn.WriteToUDP(encoded, next.address); err != nil && next.envelope.Type == protocol.TypeDeliver {
			s.state.mu.Lock()
			s.state.metrics.DeliveryWriteFailures++
			s.state.mu.Unlock()
		}
	}
}
