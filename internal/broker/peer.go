package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

// peerState contains only bounded, volatile replication state for the fixed peer.
type peerState struct {
	address           *net.UDPAddr
	alive             bool
	lastSeen          time.Time
	localEpoch        uint64
	catalogEpoch      uint64
	catalogRevision   uint64
	remoteInterest    map[string]remoteInterest
	interestVersion   map[string]uint64
	pending           map[string]*peerPending
	clientRequests    map[string]pendingClientRequest
	snapshots         map[string]*pendingSnapshot
	sequence          uint64
	nextHeartbeat     time.Time
	syncRequested     bool
	interestRequested bool
}

type remoteInterest struct {
	present bool
	epoch   uint64
	version uint64
}

type peerPending struct {
	envelope protocol.Envelope
	attempts int
	nextTry  time.Time
}

type pendingClientRequest struct {
	envelope protocol.Envelope
	address  *net.UDPAddr
}

type pendingSnapshot struct {
	scope    string
	epoch    uint64
	revision uint64
	total    uint32
	parts    map[uint32]json.RawMessage
	bytes    int
	deadline time.Time
}

func newPeerState(config Config, now func() time.Time) *peerState {
	address, _ := net.ResolveUDPAddr("udp", config.PeerAddress)
	epoch := uint64(now().UnixNano())
	if epoch == 0 {
		epoch = 1
	}
	return &peerState{
		address:         address,
		localEpoch:      epoch,
		catalogEpoch:    epoch,
		remoteInterest:  make(map[string]remoteInterest),
		interestVersion: make(map[string]uint64),
		pending:         make(map[string]*peerPending),
		clientRequests:  make(map[string]pendingClientRequest),
		snapshots:       make(map[string]*pendingSnapshot),
	}
}

func (p *peerState) nextID(prefix string) string {
	p.sequence++
	return fmt.Sprintf("%s-%d-%d", prefix, p.localEpoch, p.sequence)
}

func (p *peerState) queueInterestLocked(state *brokerState, topic string, present bool) {
	p.interestVersion[topic]++
	env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeInterestUpdate, RequestID: p.nextID("interest"), BrokerID: state.brokerID, Topic: topic, Present: boolPtr(present), PeerEpoch: p.localEpoch, InterestVersion: p.interestVersion[topic]}
	p.queueControlLocked(state, env)
}

func (p *peerState) forwardIfInterestedLocked(state *brokerState, publication protocol.Envelope) []outbound {
	interest, ok := p.remoteInterest[publication.Topic]
	if !p.alive || !ok || !interest.present {
		return nil
	}
	env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeForwardPublication, BrokerID: state.brokerID, Topic: publication.Topic, MessageID: publication.MessageID, Payload: publication.Payload}
	return []outbound{{address: cloneUDPAddr(p.address), envelope: env}}
}

func (p *peerState) queueControlLocked(state *brokerState, env protocol.Envelope) {
	if len(p.pending) >= state.pendingCap {
		return
	}
	p.pending[env.RequestID] = &peerPending{envelope: cloneEnvelope(env), nextTry: state.now()}
}

func (p *peerState) collectLocked(state *brokerState, now time.Time) []outbound {
	out := make([]outbound, 0)
	if !p.lastSeen.IsZero() && now.Sub(p.lastSeen) > state.retryInterval*3 {
		p.alive = false
	}
	for id, pending := range p.pending {
		if pending.nextTry.After(now) {
			continue
		}
		if pending.attempts >= state.maxAttempts {
			delete(p.pending, id)
			delete(p.clientRequests, id)
			continue
		}
		pending.attempts++
		pending.nextTry = now.Add(state.retryInterval)
		out = append(out, outbound{address: cloneUDPAddr(p.address), envelope: cloneEnvelope(pending.envelope)})
	}
	for id, snapshot := range p.snapshots {
		if now.After(snapshot.deadline) {
			delete(p.snapshots, id)
		}
	}
	return out
}

func (p *peerState) heartbeatLocked(state *brokerState, now time.Time) []outbound {
	if now.Before(p.nextHeartbeat) {
		return nil
	}
	p.nextHeartbeat = now.Add(state.retryInterval)
	reply := false
	env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerHeartbeat, RequestID: p.nextID("peer-heartbeat"), BrokerID: state.brokerID, Reply: &reply}
	return []outbound{{address: cloneUDPAddr(p.address), envelope: env}}
}

func (p *peerState) queueTopicUpdateLocked(state *brokerState, action, topic string) {
	env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeTopicUpdate, RequestID: p.nextID("topic-update"), BrokerID: state.brokerID, Action: action, Topic: topic, CatalogEpoch: p.catalogEpoch, CatalogRevision: p.catalogRevision}
	p.queueControlLocked(state, env)
}

func (p *peerState) requestCatalogSyncLocked(state *brokerState) {
	if p.syncRequested {
		return
	}
	p.syncRequested = true
	env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerSync, RequestID: p.nextID("sync-request"), BrokerID: state.brokerID, Action: "request", Scope: "catalog"}
	p.queueControlLocked(state, env)
	if !p.interestRequested {
		p.interestRequested = true
		interest := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerSync, RequestID: p.nextID("interest-sync-request"), BrokerID: state.brokerID, Action: "request", Scope: "interest"}
		p.queueControlLocked(state, interest)
	}
}

func (p *peerState) applyTopicUpdateLocked(state *brokerState, env protocol.Envelope) protocol.Code {
	if env.CatalogEpoch == p.catalogEpoch && env.CatalogRevision <= p.catalogRevision {
		return ""
	}
	if env.CatalogEpoch != p.catalogEpoch {
		// B1 is the catalog authority. B2 must accept its new epoch even when
		// numeric epoch generation differs across processes.
		if p.catalogRevision != 0 && env.CatalogEpoch < p.catalogEpoch {
			return ""
		}
		if env.CatalogRevision != 1 {
			return protocol.CodeSyncRequired
		}
		p.catalogEpoch = env.CatalogEpoch
		p.catalogRevision = 0
		state.topics = make(map[string]struct{})
	}
	if env.CatalogRevision != p.catalogRevision+1 {
		return protocol.CodeSyncRequired
	}
	if env.Action == "create" {
		state.topics[env.Topic] = struct{}{}
	} else {
		state.removeTopicDataLocked(env.Topic)
		delete(state.subscriptions, env.Topic)
		delete(state.topics, env.Topic)
	}
	p.catalogRevision = env.CatalogRevision
	return ""
}

func (p *peerState) applyInterestLocked(env protocol.Envelope) bool {
	current, exists := p.remoteInterest[env.Topic]
	if exists && (env.PeerEpoch < current.epoch || (env.PeerEpoch == current.epoch && env.InterestVersion <= current.version)) {
		return false
	}
	p.remoteInterest[env.Topic] = remoteInterest{present: *env.Present, epoch: env.PeerEpoch, version: env.InterestVersion}
	return true
}

func (p *peerState) makeCatalogChunksLocked(state *brokerState, request protocol.Envelope) []outbound {
	topics := make([]string, 0, len(state.topics))
	for topic := range state.topics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	parts := make([][]string, 0, 1)
	current := make([]string, 0)
	for _, topic := range topics {
		candidate := append(append([]string(nil), current...), topic)
		items, _ := json.Marshal(candidate)
		env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerSync, RequestID: p.nextID("sync-part"), BrokerID: state.brokerID, Action: "chunk", SyncID: request.RequestID, Scope: "catalog", CatalogEpoch: p.catalogEpoch, CatalogRevision: p.catalogRevision, Part: uint32(len(parts)), Total: 1, Items: items}
		if _, err := protocol.Encode(env); err != nil && len(current) > 0 {
			parts = append(parts, current)
			current = []string{topic}
		} else {
			current = candidate
		}
	}
	if len(current) > 0 || len(parts) == 0 {
		parts = append(parts, current)
	}
	out := make([]outbound, 0, len(parts))
	for index, items := range parts {
		encodedItems, _ := json.Marshal(items)
		env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerSync, RequestID: p.nextID("sync-part"), BrokerID: state.brokerID, Action: "chunk", SyncID: request.RequestID, Scope: "catalog", CatalogEpoch: p.catalogEpoch, CatalogRevision: p.catalogRevision, Part: uint32(index), Total: uint32(len(parts)), Items: encodedItems}
		out = append(out, outbound{address: cloneUDPAddr(p.address), envelope: env})
		p.queueControlLocked(state, env)
	}
	return out
}

type interestSnapshotItem struct {
	Topic   string `json:"topic"`
	Present bool   `json:"present"`
	Epoch   uint64 `json:"epoch"`
	Version uint64 `json:"version"`
}

func (p *peerState) makeInterestChunksLocked(state *brokerState, request protocol.Envelope) []outbound {
	topics := make([]string, 0, len(p.interestVersion))
	for topic := range p.interestVersion {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	if len(topics) == 0 {
		topics = []string{""}
	}
	if len(topics) > state.pendingCap {
		return nil
	}
	out := make([]outbound, 0, len(topics))
	for index, topic := range topics {
		items := []interestSnapshotItem{}
		if topic != "" {
			items = append(items, interestSnapshotItem{Topic: topic, Present: len(state.subscriptions[topic]) > 0, Epoch: p.localEpoch, Version: p.interestVersion[topic]})
		}
		encoded, _ := json.Marshal(items)
		env := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerSync, RequestID: p.nextID("interest-sync-part"), BrokerID: state.brokerID, Action: "chunk", SyncID: request.RequestID, Scope: "interest", PeerEpoch: p.localEpoch, Part: uint32(index), Total: uint32(len(topics)), Items: encoded}
		out = append(out, outbound{address: cloneUDPAddr(p.address), envelope: env})
		p.queueControlLocked(state, env)
	}
	return out
}

func (p *peerState) receiveSnapshotChunkLocked(state *brokerState, env protocol.Envelope, now time.Time) (applied bool, code protocol.Code) {
	if env.Total == 0 || env.Total > uint32(state.pendingCap) || env.Part >= env.Total {
		return false, protocol.CodeSyncRequired
	}
	snapshot := p.snapshots[env.SyncID]
	if snapshot == nil {
		snapshot = &pendingSnapshot{scope: env.Scope, epoch: env.CatalogEpoch, revision: env.CatalogRevision, total: env.Total, parts: make(map[uint32]json.RawMessage), deadline: now.Add(state.retryInterval * time.Duration(state.maxAttempts))}
		p.snapshots[env.SyncID] = snapshot
	}
	if snapshot.scope != env.Scope || snapshot.epoch != env.CatalogEpoch || snapshot.revision != env.CatalogRevision || snapshot.total != env.Total {
		return false, protocol.CodeSyncRequired
	}
	if _, exists := snapshot.parts[env.Part]; !exists {
		copy := append(json.RawMessage(nil), env.Items...)
		snapshot.parts[env.Part] = copy
		snapshot.bytes += len(copy)
		if snapshot.bytes > state.pendingCap*protocol.MaxDatagramSize {
			delete(p.snapshots, env.SyncID)
			return false, protocol.CodeSyncRequired
		}
	}
	if uint32(len(snapshot.parts)) != snapshot.total {
		return false, ""
	}
	if snapshot.scope == "interest" {
		interests := make(map[string]remoteInterest)
		for part := uint32(0); part < snapshot.total; part++ {
			var items []interestSnapshotItem
			if err := json.Unmarshal(snapshot.parts[part], &items); err != nil {
				delete(p.snapshots, env.SyncID)
				return false, protocol.CodeSyncRequired
			}
			for _, item := range items {
				interests[item.Topic] = remoteInterest{present: item.Present, epoch: item.Epoch, version: item.Version}
			}
		}
		for topic, incoming := range interests {
			if current, exists := p.remoteInterest[topic]; !exists || incoming.epoch > current.epoch || (incoming.epoch == current.epoch && incoming.version >= current.version) {
				p.remoteInterest[topic] = incoming
			}
		}
		delete(p.snapshots, env.SyncID)
		p.interestRequested = false
		return true, ""
	}
	if snapshot.scope != "catalog" {
		delete(p.snapshots, env.SyncID)
		return false, protocol.CodeSyncRequired
	}
	topics := make(map[string]struct{})
	for part := uint32(0); part < snapshot.total; part++ {
		var names []string
		if err := json.Unmarshal(snapshot.parts[part], &names); err != nil {
			delete(p.snapshots, env.SyncID)
			return false, protocol.CodeSyncRequired
		}
		for _, name := range names {
			topics[name] = struct{}{}
		}
	}
	state.topics = topics
	p.catalogEpoch = snapshot.epoch
	p.catalogRevision = snapshot.revision
	delete(p.snapshots, env.SyncID)
	p.syncRequested = false
	return true, ""
}

func boolPtr(value bool) *bool { return &value }
