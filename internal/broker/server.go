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

// Serve validates and handles the local control plane until context cancellation or Close.
// Data-plane operations remain deliberately unimplemented.
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
		if err := s.conn.SetReadDeadline(time.Now().Add(s.sessionSweepInterval())); err != nil {
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
				continue
			}
			return err
		}
		env, err := protocol.Decode(buffer[:n])
		if err != nil {
			continue
		}
		response, target, ok := s.handle(env, source)
		if !ok {
			continue
		}
		encoded, err := protocol.Encode(response)
		if err != nil {
			continue
		}
		// A successful UDP write is only a local send attempt; it is not delivery.
		_, _ = s.conn.WriteToUDP(encoded, target)
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

func (s *Server) expireSessions() {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.expireSessionsLocked(s.state.now())
}

// handle applies only T004-T007 control-plane operations. It does not publish,
// enqueue, forward, or deliver data messages.
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
