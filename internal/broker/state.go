package broker

import (
	"net"
	"sync"
	"time"

	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/control"
	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/protocol"
)

type session struct {
	address       *net.UDPAddr
	lastSeen      time.Time
	subscriptions map[string]struct{}
}

type brokerState struct {
	mu            sync.Mutex
	now           func() time.Time
	sessionTTL    time.Duration
	sessions      map[string]*session
	topics        map[string]struct{}
	subscriptions map[string]map[string]struct{}
	replies       *control.ReplyCache
}

func newBrokerState(config Config) *brokerState {
	now := config.now
	if now == nil {
		now = time.Now
	}
	return &brokerState{
		now:           now,
		sessionTTL:    config.SessionTTL,
		sessions:      make(map[string]*session),
		topics:        make(map[string]struct{}),
		subscriptions: make(map[string]map[string]struct{}),
		replies:       control.NewReplyCache(config.ControlCacheCapacity, config.ControlCacheTTL, now),
	}
}

func (s *brokerState) registerLocked(clientID string, address *net.UDPAddr, now time.Time) {
	current, ok := s.sessions[clientID]
	if !ok {
		current = &session{subscriptions: make(map[string]struct{})}
		s.sessions[clientID] = current
	}
	current.address = cloneUDPAddr(address)
	current.lastSeen = now
}

func (s *brokerState) sessionMatchesLocked(clientID string, address *net.UDPAddr) bool {
	current, ok := s.sessions[clientID]
	return ok && sameUDPAddr(current.address, address)
}

func (s *brokerState) replyAddressLocked(clientID string, fallback *net.UDPAddr) *net.UDPAddr {
	if current, ok := s.sessions[clientID]; ok {
		return cloneUDPAddr(current.address)
	}
	return cloneUDPAddr(fallback)
}

func (s *brokerState) expireSessionsLocked(now time.Time) {
	for clientID, current := range s.sessions {
		if now.Sub(current.lastSeen) >= s.sessionTTL {
			s.removeSessionLocked(clientID)
		}
	}
}

func (s *brokerState) removeSessionLocked(clientID string) {
	current, ok := s.sessions[clientID]
	if !ok {
		return
	}
	for topic := range current.subscriptions {
		s.removeSubscriptionLocked(clientID, topic)
	}
	delete(s.sessions, clientID)
}

func (s *brokerState) createTopicLocked(brokerID int, env protocol.Envelope) protocol.Envelope {
	if brokerID != 1 {
		return errorReply(env, protocol.CodeCoordinatorUnavailable)
	}
	if _, exists := s.topics[env.Topic]; exists {
		return errorReply(env, protocol.CodeTopicAlreadyExists)
	}
	s.topics[env.Topic] = struct{}{}
	return okReply(env)
}

func (s *brokerState) deleteTopicLocked(brokerID int, env protocol.Envelope) protocol.Envelope {
	if brokerID != 1 {
		return errorReply(env, protocol.CodeCoordinatorUnavailable)
	}
	if _, exists := s.topics[env.Topic]; !exists {
		return errorReply(env, protocol.CodeTopicNotFound)
	}
	for clientID := range s.subscriptions[env.Topic] {
		s.removeSubscriptionLocked(clientID, env.Topic)
	}
	delete(s.subscriptions, env.Topic)
	delete(s.topics, env.Topic)
	return okReply(env)
}

func (s *brokerState) subscribeLocked(env protocol.Envelope) protocol.Envelope {
	if _, exists := s.topics[env.Topic]; !exists {
		return errorReply(env, protocol.CodeTopicNotFound)
	}
	current := s.sessions[env.ClientID]
	if _, exists := current.subscriptions[env.Topic]; exists {
		return okReply(env)
	}
	current.subscriptions[env.Topic] = struct{}{}
	clients := s.subscriptions[env.Topic]
	if clients == nil {
		clients = make(map[string]struct{})
		s.subscriptions[env.Topic] = clients
	}
	clients[env.ClientID] = struct{}{}
	return okReply(env)
}

func (s *brokerState) unsubscribeLocked(env protocol.Envelope) protocol.Envelope {
	s.removeSubscriptionLocked(env.ClientID, env.Topic)
	return okReply(env)
}

func (s *brokerState) removeSubscriptionLocked(clientID, topic string) {
	if current, ok := s.sessions[clientID]; ok {
		delete(current.subscriptions, topic)
	}
	if clients, ok := s.subscriptions[topic]; ok {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(s.subscriptions, topic)
		}
	}
}

func (s *Server) subscriptionCount(topic string) int {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return len(s.state.subscriptions[topic])
}

func okReply(request protocol.Envelope) protocol.Envelope {
	return protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeOK, RequestID: request.RequestID, ClientID: request.ClientID}
}

func errorReply(request protocol.Envelope, code protocol.Code) protocol.Envelope {
	return protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeError, RequestID: request.RequestID, ClientID: request.ClientID, Code: code}
}

func sameUDPAddr(left, right *net.UDPAddr) bool {
	return left != nil && right != nil && left.Port == right.Port && left.Zone == right.Zone && left.IP.Equal(right.IP)
}

func cloneUDPAddr(address *net.UDPAddr) *net.UDPAddr {
	if address == nil {
		return nil
	}
	copy := *address
	copy.IP = append(net.IP(nil), address.IP...)
	return &copy
}
