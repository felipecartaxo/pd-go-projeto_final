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
				s.writeOutbounds(s.collectPeerOutbounds(s.state.now()))
				continue
			}
			return err
		}
		env, err := protocol.Decode(buffer[:n])
		if err != nil {
			continue
		}
		if s.isPeer(source) {
			s.writeOutbounds(s.handlePeer(env, source))
			s.writeOutbounds(s.collectOutbounds(s.state.now()))
			s.writeOutbounds(s.collectPeerOutbounds(s.state.now()))
			continue
		}
		response, target, hasResponse := s.handle(env, source)
		if hasResponse {
			s.writeOutbounds([]outbound{{address: target, envelope: response}})
		}
		s.writeOutbounds(s.collectOutbounds(s.state.now()))
		s.writeOutbounds(s.collectPeerOutbounds(s.state.now()))
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
	if (env.Type == protocol.TypeCreateTopic || env.Type == protocol.TypeDeleteTopic) && s.state.brokerID == 2 && s.state.peer.alive {
		action := "create"
		if env.Type == protocol.TypeDeleteTopic {
			action = "delete"
		}
		internalID := s.state.peer.nextID("topic-request")
		request := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeTopicRequest, RequestID: internalID, BrokerID: s.state.brokerID, Action: action, Topic: env.Topic}
		s.state.peer.clientRequests[internalID] = pendingClientRequest{envelope: env, address: target}
		s.state.peer.queueControlLocked(s.state, request)
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

func (s *Server) collectPeerOutbounds(now time.Time) []outbound {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	out := s.state.peer.heartbeatLocked(s.state, now)
	out = append(out, s.state.peer.collectLocked(s.state, now)...)
	return out
}

func (s *Server) isPeer(source *net.UDPAddr) bool {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return sameUDPAddr(s.state.peer.address, source)
}

func (s *Server) handlePeer(env protocol.Envelope, source *net.UDPAddr) []outbound {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	state := s.state
	peer := state.peer
	if !sameUDPAddr(peer.address, source) || (env.BrokerID != 1 && env.BrokerID != 2) || env.BrokerID == state.brokerID {
		return nil
	}
	now := state.now()
	wasAlive := peer.alive
	peer.alive = true
	peer.lastSeen = now
	if !wasAlive {
		peer.requestCatalogSyncLocked(state)
	}
	ack := func(request protocol.Envelope, status string, code protocol.Code) []outbound {
		response := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeACK, RequestID: request.RequestID, BrokerID: state.brokerID, Status: status, Code: code}
		return []outbound{{address: cloneUDPAddr(peer.address), envelope: response}}
	}
	switch env.Type {
	case protocol.TypePeerHeartbeat:
		if env.Reply != nil && *env.Reply {
			return nil
		}
		reply := true
		return []outbound{{address: cloneUDPAddr(peer.address), envelope: protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerHeartbeat, RequestID: env.RequestID, BrokerID: state.brokerID, Reply: &reply}}}
	case protocol.TypeACK:
		pending, exists := peer.pending[env.RequestID]
		if !exists {
			return nil
		}
		delete(peer.pending, env.RequestID)
		if client, waiting := peer.clientRequests[env.RequestID]; waiting {
			delete(peer.clientRequests, env.RequestID)
			if env.Status == "OK" {
				return []outbound{{address: client.address, envelope: okReply(client.envelope)}}
			}
			return []outbound{{address: client.address, envelope: errorReply(client.envelope, env.Code)}}
		}
		_ = pending
		return nil
	case protocol.TypeInterestUpdate:
		peer.applyInterestLocked(env)
		return ack(env, "OK", "")
	case protocol.TypeForwardPublication:
		// A forwarded publication is delivered only to local subscribers and never forwarded again.
		for subscriberID := range state.subscriptions[env.Topic] {
			if subscriber, ok := state.sessions[subscriberID]; ok {
				deliver := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeDeliver, ClientID: subscriberID, Topic: env.Topic, MessageID: env.MessageID, Payload: env.Payload}
				if subscriber.output.push(deliver) {
					state.metrics.OutputDropped++
				}
			}
		}
		return nil
	case protocol.TypeTopicRequest:
		if state.brokerID != 1 {
			return ack(env, "ERROR", protocol.CodeCoordinatorUnavailable)
		}
		request := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeCreateTopic, RequestID: env.RequestID, ClientID: "peer", Topic: env.Topic}
		if env.Action == "delete" {
			request.Type = protocol.TypeDeleteTopic
		}
		response := state.createTopicLocked(1, request)
		if response.Type == protocol.TypeError {
			return ack(env, "ERROR", response.Code)
		}
		return ack(env, "OK", "")
	case protocol.TypeTopicUpdate:
		if code := peer.applyTopicUpdateLocked(state, env); code != "" {
			return ack(env, "ERROR", code)
		}
		return ack(env, "OK", "")
	case protocol.TypePeerSync:
		if env.Action == "request" && env.Scope == "catalog" {
			return peer.makeCatalogChunksLocked(state, env)
		}
		if env.Action == "request" && env.Scope == "interest" {
			return peer.makeInterestChunksLocked(state, env)
		}
		if env.Action == "chunk" {
			_, code := peer.receiveSnapshotChunkLocked(state, env, now)
			if code != "" {
				return ack(env, "ERROR", code)
			}
			return ack(env, "OK", "")
		}
	}
	return nil
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
