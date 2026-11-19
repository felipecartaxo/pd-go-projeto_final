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
	limiter       tokenBucket
	output        *messageQueue
}

type brokerState struct {
	mu            sync.Mutex
	now           func() time.Time
	sessionTTL    time.Duration
	sessions      map[string]*session
	topics        map[string]struct{}
	subscriptions map[string]map[string]struct{}
	replies       *control.ReplyCache
	input         map[string]*messageQueue
	publisherRR   []string
	pending       map[string]*pendingNotice
	metrics       Metrics
	rate          int
	burst         int
	inputCapacity int
	outputOrder   []string
	outputCap     int
	retryInterval time.Duration
	maxAttempts   int
	pendingCap    int
	brokerID      int
	peer          *peerState
}

func newBrokerState(config Config) *brokerState {
	now := config.now
	if now == nil {
		now = time.Now
	}
	state := &brokerState{
		now:           now,
		sessionTTL:    config.SessionTTL,
		sessions:      make(map[string]*session),
		topics:        make(map[string]struct{}),
		subscriptions: make(map[string]map[string]struct{}),
		replies:       control.NewReplyCache(config.ControlCacheCapacity, config.ControlCacheTTL, now),
		input:         make(map[string]*messageQueue),
		pending:       make(map[string]*pendingNotice),
		rate:          config.RateLimitPerSecond,
		burst:         config.RateBurst,
		inputCapacity: config.InputQueueCapacity,
		outputCap:     config.OutputQueueCapacity,
		retryInterval: config.ControlRetryInterval,
		maxAttempts:   config.ControlMaxAttempts,
		pendingCap:    config.ControlCacheCapacity,
		brokerID:      config.ID,
	}
	state.peer = newPeerState(config, now)
	return state
}

func (s *brokerState) registerLocked(clientID string, address *net.UDPAddr, now time.Time) {
	current, ok := s.sessions[clientID]
	if !ok {
		current = &session{subscriptions: make(map[string]struct{}), limiter: newTokenBucket(now, s.burst), output: newMessageQueue(s.outputCap)}
		s.sessions[clientID] = current
		s.outputOrder = append(s.outputOrder, clientID)
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
	delete(s.input, clientID)
	s.removePublisherLocked(clientID)
	s.removeOutputLocked(clientID)
	for key, pending := range s.pending {
		if pending.envelope.ClientID == clientID {
			delete(s.pending, key)
		}
	}
}

func (s *brokerState) createTopicLocked(brokerID int, env protocol.Envelope) protocol.Envelope {
	if brokerID != 1 {
		return errorReply(env, protocol.CodeCoordinatorUnavailable)
	}
	if _, exists := s.topics[env.Topic]; exists {
		return errorReply(env, protocol.CodeTopicAlreadyExists)
	}
	s.topics[env.Topic] = struct{}{}
	s.peer.catalogRevision++
	s.peer.queueTopicUpdateLocked(s, "create", env.Topic)
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
	s.removeTopicDataLocked(env.Topic)
	delete(s.subscriptions, env.Topic)
	delete(s.topics, env.Topic)
	s.peer.catalogRevision++
	s.peer.queueTopicUpdateLocked(s, "delete", env.Topic)
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
	wasEmpty := len(clients) == 0
	if clients == nil {
		clients = make(map[string]struct{})
		s.subscriptions[env.Topic] = clients
	}
	clients[env.ClientID] = struct{}{}
	if wasEmpty {
		s.peer.queueInterestLocked(s, env.Topic, true)
	}
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
			s.peer.queueInterestLocked(s, topic, false)
		}
	}
}

func (s *brokerState) publishLocked(env protocol.Envelope, now time.Time) (protocol.Envelope, bool) {
	s.metrics.PublishedReceived++
	if _, exists := s.topics[env.Topic]; !exists {
		return errorReply(env, protocol.CodeTopicNotFound), true
	}
	current := s.sessions[env.ClientID]
	if !current.limiter.allow(now, s.rate, s.burst) {
		s.metrics.RateLimited++
		return protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeRateLimited, RequestID: env.RequestID, ClientID: env.ClientID, Topic: env.Topic, MessageID: env.MessageID}, true
	}
	queue := s.input[env.ClientID]
	if queue == nil {
		queue = newMessageQueue(s.inputCapacity)
		s.input[env.ClientID] = queue
	}
	wasEmpty := len(queue.items) == 0
	if queue.push(env) {
		s.metrics.InputDropped++
	}
	if wasEmpty {
		s.publisherRR = append(s.publisherRR, env.ClientID)
	}
	s.metrics.PublishedAccepted++
	return protocol.Envelope{}, false
}

func (s *brokerState) dispatchOneLocked(now time.Time) []outbound {
	for len(s.publisherRR) > 0 {
		clientID := s.publisherRR[0]
		s.publisherRR = s.publisherRR[1:]
		queue := s.input[clientID]
		if queue == nil {
			continue
		}
		publication, ok := queue.pop()
		if !ok {
			delete(s.input, clientID)
			continue
		}
		if len(queue.items) > 0 {
			s.publisherRR = append(s.publisherRR, clientID)
		} else {
			delete(s.input, clientID)
		}

		clients := s.subscriptions[publication.Topic]
		remote := s.peer.alive && s.peer.remoteInterest[publication.Topic].present
		if len(clients) == 0 && !remote {
			s.metrics.NoSubscribers++
			return s.scheduleNoSubscribersLocked(publication, now)
		}
		for subscriberID := range clients {
			subscriber, active := s.sessions[subscriberID]
			if !active {
				continue
			}
			deliver := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeDeliver, ClientID: subscriberID, Topic: publication.Topic, MessageID: publication.MessageID, Payload: publication.Payload}
			if subscriber.output.push(deliver) {
				s.metrics.OutputDropped++
			}
		}
		return s.peer.forwardIfInterestedLocked(s, publication)
	}
	return nil
}

func (s *brokerState) scheduleNoSubscribersLocked(publication protocol.Envelope, now time.Time) []outbound {
	key := noticeKey(publication.ClientID, publication.RequestID)
	if _, exists := s.pending[key]; exists {
		return nil
	}
	if len(s.pending) >= s.pendingCap {
		return []outbound{{address: s.replyAddressLocked(publication.ClientID, nil), envelope: errorReply(publication, protocol.CodeControlBusy)}}
	}
	address := s.replyAddressLocked(publication.ClientID, nil)
	if address == nil {
		return nil
	}
	notice := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeNoSubscribers, RequestID: publication.RequestID, ClientID: publication.ClientID, Topic: publication.Topic, MessageID: publication.MessageID}
	s.pending[key] = &pendingNotice{envelope: notice, address: address, attempts: 1, nextTry: now.Add(s.retryInterval)}
	return []outbound{{address: cloneUDPAddr(address), envelope: notice}}
}

func (s *brokerState) collectOutboundsLocked(now time.Time) []outbound {
	budget := len(s.publisherRR)
	outbounds := make([]outbound, 0, budget+len(s.sessions))
	for range budget {
		outbounds = append(outbounds, s.dispatchOneLocked(now)...)
	}
	for key, pending := range s.pending {
		if pending.nextTry.After(now) {
			continue
		}
		if pending.attempts >= s.maxAttempts {
			delete(s.pending, key)
			s.metrics.NoSubscriberTimeouts++
			continue
		}
		pending.attempts++
		pending.nextTry = now.Add(s.retryInterval)
		s.metrics.NoSubscriberRetries++
		outbounds = append(outbounds, outbound{address: cloneUDPAddr(pending.address), envelope: cloneEnvelope(pending.envelope)})
	}
	for _, clientID := range s.outputOrder {
		current, ok := s.sessions[clientID]
		if !ok {
			continue
		}
		if deliver, ok := current.output.pop(); ok {
			s.metrics.DeliverAttempts++
			outbounds = append(outbounds, outbound{address: cloneUDPAddr(current.address), envelope: deliver})
		}
	}
	return outbounds
}

func (s *brokerState) acknowledgeNoticeLocked(clientID, requestID string) {
	delete(s.pending, noticeKey(clientID, requestID))
}

func (s *brokerState) removeTopicDataLocked(topic string) {
	for clientID, queue := range s.input {
		queue.removeTopic(topic)
		if len(queue.items) == 0 {
			delete(s.input, clientID)
			s.removePublisherLocked(clientID)
		}
	}
	for _, current := range s.sessions {
		current.output.removeTopic(topic)
	}
	for key, pending := range s.pending {
		if pending.envelope.Topic == topic {
			delete(s.pending, key)
		}
	}
}

func (s *brokerState) removePublisherLocked(clientID string) {
	for index := 0; index < len(s.publisherRR); index++ {
		if s.publisherRR[index] == clientID {
			copy(s.publisherRR[index:], s.publisherRR[index+1:])
			s.publisherRR = s.publisherRR[:len(s.publisherRR)-1]
			return
		}
	}
}

func (s *brokerState) removeOutputLocked(clientID string) {
	for index := 0; index < len(s.outputOrder); index++ {
		if s.outputOrder[index] == clientID {
			copy(s.outputOrder[index:], s.outputOrder[index+1:])
			s.outputOrder = s.outputOrder[:len(s.outputOrder)-1]
			return
		}
	}
}

func noticeKey(clientID, requestID string) string { return clientID + "\x00" + requestID }

func (s *Server) subscriptionCount(topic string) int {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return len(s.state.subscriptions[topic])
}

// Metrics returns a snapshot of local processing and send-attempt counters.
func (s *Server) Metrics() Metrics {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return s.state.metrics
}

func (s *Server) dispatchOne(now time.Time) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.expireSessionsLocked(now)
	s.state.dispatchOneLocked(now)
}

func (s *Server) popOutput(clientID string) (protocol.Envelope, bool) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	current, ok := s.state.sessions[clientID]
	if !ok {
		return protocol.Envelope{}, false
	}
	return current.output.pop()
}

func (s *Server) outputMessageIDs(clientID string) []string {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	current, ok := s.state.sessions[clientID]
	if !ok {
		return nil
	}
	ids := make([]string, len(current.output.items))
	for index, item := range current.output.items {
		ids[index] = item.MessageID
	}
	return ids
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
