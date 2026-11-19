package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/protocol"
)

func TestInterestUpdateIgnoresDuplicateAndStaleVersions(t *testing.T) {
	now := time.Unix(100, 0)
	first, second := newPeerTestPair(t, &now)
	present := true
	update := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeInterestUpdate, RequestID: "interest-2", BrokerID: 2, Topic: "auction", Present: &present, PeerEpoch: 7, InterestVersion: 2}
	first.handlePeer(update, second.Addr())
	duplicate := update
	duplicate.RequestID = "interest-duplicate"
	first.handlePeer(duplicate, second.Addr())
	stale := update
	stale.RequestID = "interest-1"
	stale.InterestVersion = 1
	stalePresent := false
	stale.Present = &stalePresent
	first.handlePeer(stale, second.Addr())

	first.state.mu.Lock()
	interest := first.state.peer.remoteInterest["auction"]
	first.state.mu.Unlock()
	if !interest.present || interest.version != 2 || interest.epoch != 7 {
		t.Fatalf("remote interest = %#v, want present version 2 epoch 7", interest)
	}
}

func TestForwardPublicationDeliversOnlyAtDestination(t *testing.T) {
	now := time.Unix(100, 0)
	first, second := newPeerTestPair(t, &now)
	first.state.mu.Lock()
	first.state.peer.alive = true
	first.state.peer.remoteInterest["auction"] = remoteInterest{present: true, epoch: 1, version: 1}
	first.state.mu.Unlock()
	creator, publisher, subscriber := testAddr(10001), testAddr(10002), testAddr(10003)
	registerControl(t, first, creator, "creator")
	registerControl(t, first, publisher, "publisher")
	createTopic(t, first, creator, "creator", "auction")
	registerControl(t, second, subscriber, "subscriber")
	second.state.mu.Lock()
	second.state.topics["auction"] = struct{}{}
	second.state.mu.Unlock()
	subscribeTopic(t, second, subscriber, "subscriber", "auction")

	first.handle(publishEnvelope("publish", "publisher", "message-1", "auction"), publisher)
	outbounds := first.collectOutbounds(now)
	var forward protocol.Envelope
	for _, outbound := range outbounds {
		if outbound.envelope.Type == protocol.TypeForwardPublication {
			forward = outbound.envelope
		}
	}
	if forward.Type != protocol.TypeForwardPublication {
		t.Fatalf("outbounds = %#v, want FORWARD_PUBLICATION", outbounds)
	}
	second.handlePeer(forward, first.Addr())
	for _, outbound := range second.collectOutbounds(now) {
		if outbound.envelope.Type == protocol.TypeForwardPublication {
			t.Fatal("destination re-forwarded a FORWARD_PUBLICATION")
		}
		if outbound.envelope.Type == protocol.TypeDeliver && outbound.envelope.MessageID == "message-1" {
			return
		}
	}
	t.Fatal("destination did not enqueue local DELIVER")
}

func TestCatalogUpdateRejectsGapAndIgnoresDuplicate(t *testing.T) {
	now := time.Unix(100, 0)
	first, second := newPeerTestPair(t, &now)
	second.state.mu.Lock()
	epoch := second.state.peer.catalogEpoch + 1
	second.state.mu.Unlock()
	update := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeTopicUpdate, RequestID: "topic-1", BrokerID: 1, Action: "create", Topic: "auction", CatalogEpoch: epoch, CatalogRevision: 1}
	ack := second.handlePeer(update, first.Addr())
	if len(ack) != 1 || ack[0].envelope.Status != "OK" {
		t.Fatalf("first TOPIC_UPDATE ack = %#v", ack)
	}
	second.handlePeer(update, first.Addr())
	gap := update
	gap.RequestID = "topic-3"
	gap.Topic = "other"
	gap.CatalogRevision = 3
	ack = second.handlePeer(gap, first.Addr())
	if len(ack) != 1 || ack[0].envelope.Code != protocol.CodeSyncRequired {
		t.Fatalf("gapped TOPIC_UPDATE ack = %#v, want SYNC_REQUIRED", ack)
	}
	second.state.mu.Lock()
	_, auction := second.state.topics["auction"]
	_, other := second.state.topics["other"]
	second.state.mu.Unlock()
	if !auction || other {
		t.Fatalf("catalog after duplicate/gap = auction:%v other:%v", auction, other)
	}
}

func TestPeerSyncAppliesCatalogOnlyAfterAllReorderedChunks(t *testing.T) {
	now := time.Unix(100, 0)
	first, second := newPeerTestPair(t, &now)
	chunk0 := peerChunk(t, "chunk-0", 2, "snapshot", 0, 2, []string{"a"})
	chunk0.CatalogEpoch, chunk0.CatalogRevision = 11, 4
	chunk1 := peerChunk(t, "chunk-1", 2, "snapshot", 1, 2, []string{"b", "c"})
	chunk1.CatalogEpoch, chunk1.CatalogRevision = 11, 4
	// Reorder by feeding the final chunk first, and duplicate it before completion.
	first.handlePeer(chunk1, second.Addr())
	first.handlePeer(chunk1, second.Addr())
	first.state.mu.Lock()
	_, premature := first.state.topics["a"]
	first.state.mu.Unlock()
	if premature {
		t.Fatal("incomplete snapshot changed catalog")
	}
	first.handlePeer(chunk0, second.Addr())
	first.state.mu.Lock()
	gotEpoch, gotRevision := first.state.peer.catalogEpoch, first.state.peer.catalogRevision
	_, a := first.state.topics["a"]
	_, b := first.state.topics["b"]
	_, c := first.state.topics["c"]
	first.state.mu.Unlock()
	if !a || !b || !c || gotEpoch != 11 || gotRevision != 4 {
		t.Fatalf("snapshot catalog/epoch/revision = a:%v b:%v c:%v %d/%d", a, b, c, gotEpoch, gotRevision)
	}
}

func TestBrokerTwoRemainsCatalogReadOnlyAfterPeerFailure(t *testing.T) {
	now := time.Unix(100, 0)
	_, second := newPeerTestPair(t, &now)
	second.state.mu.Lock()
	second.state.peer.alive = true
	second.state.peer.lastSeen = now
	second.state.topics["known"] = struct{}{}
	second.state.mu.Unlock()
	now = now.Add(10 * time.Second)
	second.collectPeerOutbounds(now)
	creator := testAddr(10001)
	registerControl(t, second, creator, "creator")
	reply, _, _ := second.handle(controlEnvelope(protocol.TypeCreateTopic, "create-after-failure", "creator", "new"), creator)
	if reply.Type != protocol.TypeError || reply.Code != protocol.CodeCoordinatorUnavailable {
		t.Fatalf("B2 CREATE_TOPIC after peer failure = %#v", reply)
	}
}

func TestBrokerTwoForwardsCatalogRequestAndConvergesFromTopicUpdate(t *testing.T) {
	now := time.Unix(100, 0)
	first, second := newPeerTestPair(t, &now)
	second.state.mu.Lock()
	second.state.peer.alive = true
	second.state.mu.Unlock()
	creator := testAddr(10001)
	registerControl(t, second, creator, "creator")
	if response, _, replied := second.handle(controlEnvelope(protocol.TypeCreateTopic, "create-through-b2", "creator", "auction"), creator); replied || response.Type != "" {
		t.Fatalf("B2 CREATE_TOPIC replied %#v before B1 result", response)
	}
	var request protocol.Envelope
	for _, outbound := range second.collectPeerOutbounds(now) {
		if outbound.envelope.Type == protocol.TypeTopicRequest {
			request = outbound.envelope
		}
	}
	if request.Type != protocol.TypeTopicRequest {
		t.Fatal("B2 did not emit TOPIC_REQUEST")
	}
	acks := first.handlePeer(request, second.Addr())
	if len(acks) != 1 || acks[0].envelope.Type != protocol.TypeACK || acks[0].envelope.Status != "OK" {
		t.Fatalf("B1 TOPIC_REQUEST response = %#v", acks)
	}
	clientReply := second.handlePeer(acks[0].envelope, first.Addr())
	if len(clientReply) != 1 || clientReply[0].envelope.Type != protocol.TypeOK || clientReply[0].envelope.RequestID != "create-through-b2" {
		t.Fatalf("B2 client result = %#v", clientReply)
	}
	var update protocol.Envelope
	for _, outbound := range first.collectPeerOutbounds(now) {
		if outbound.envelope.Type == protocol.TypeTopicUpdate {
			update = outbound.envelope
		}
	}
	if update.Type != protocol.TypeTopicUpdate {
		t.Fatal("B1 did not emit TOPIC_UPDATE")
	}
	second.handlePeer(update, first.Addr())
	second.state.mu.Lock()
	_, exists := second.state.topics["auction"]
	second.state.mu.Unlock()
	if !exists {
		t.Fatal("B2 catalog did not converge after TOPIC_UPDATE")
	}
}

func newPeerTestPair(t *testing.T, now *time.Time) (*Server, *Server) {
	t.Helper()
	first := newDataTestServer(t, now, nil)
	config := DefaultConfig(2)
	config.ListenAddress = "127.0.0.1:0"
	config.PeerAddress = first.Addr().String()
	config.now = func() time.Time { return *now }
	second, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	first.state.mu.Lock()
	first.state.peer.address = cloneUDPAddr(second.Addr())
	first.state.mu.Unlock()
	return first, second
}

func peerChunk(t *testing.T, requestID string, brokerID int, syncID string, part, total uint32, topics []string) protocol.Envelope {
	t.Helper()
	items, err := json.Marshal(topics)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePeerSync, RequestID: requestID, BrokerID: brokerID, Action: "chunk", SyncID: syncID, Scope: "catalog", CatalogEpoch: 1, CatalogRevision: 1, Part: part, Total: total, Items: items}
}
