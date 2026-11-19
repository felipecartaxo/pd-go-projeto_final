package broker

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

func TestDuplicateControlAfterLostReplyExecutesCreateOnlyOnce(t *testing.T) {
	now := time.Unix(100, 0)
	server := newControlTestServer(t, 1, &now)
	client := testAddr(10001)
	registerControl(t, server, client, "creator")

	request := controlEnvelope(protocol.TypeCreateTopic, "create-1", "creator", "auction")
	first, _, ok := server.handle(request, client) // Simulates a reply lost on the network.
	if !ok || first.Type != protocol.TypeOK {
		t.Fatalf("first CREATE_TOPIC reply = %#v, handled = %v", first, ok)
	}
	retry, _, ok := server.handle(request, client)
	if !ok || !reflect.DeepEqual(retry, first) {
		t.Fatalf("duplicate CREATE_TOPIC reply = %#v, want cached %#v", retry, first)
	}

	other, _, _ := server.handle(controlEnvelope(protocol.TypeCreateTopic, "create-2", "creator", "auction"), client)
	if other.Type != protocol.TypeError || other.Code != protocol.CodeTopicAlreadyExists {
		t.Fatalf("distinct duplicate CREATE_TOPIC reply = %#v, want TOPIC_ALREADY_EXISTS", other)
	}
}

func TestServerServesLocalControlOverUDP(t *testing.T) {
	now := time.Unix(100, 0)
	server := newControlTestServer(t, 1, &now)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("Serve() did not stop")
		}
	}()

	client, err := net.DialUDP("udp", nil, server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := controlEnvelope(protocol.TypeRegister, "register-over-udp", "client-over-udp", "")
	encoded, err := protocol.Encode(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(encoded); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, protocol.MaxDatagramSize)
	n, err := client.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := protocol.Decode(buffer[:n])
	if err != nil {
		t.Fatal(err)
	}
	if reply.Type != protocol.TypeOK || reply.RequestID != request.RequestID || reply.ClientID != request.ClientID {
		t.Fatalf("REGISTER UDP reply = %#v", reply)
	}
}

func TestSessionsBindOriginHeartbeatAndExpireSubscriptions(t *testing.T) {
	now := time.Unix(100, 0)
	server := newControlTestServer(t, 1, &now)
	creator, subscriber, otherOrigin := testAddr(10001), testAddr(10002), testAddr(10003)
	registerControl(t, server, creator, "creator")
	registerControl(t, server, subscriber, "subscriber")
	create, _, _ := server.handle(controlEnvelope(protocol.TypeCreateTopic, "create", "creator", "auction"), creator)
	if create.Type != protocol.TypeOK {
		t.Fatalf("CREATE_TOPIC reply = %#v", create)
	}

	heartbeat, _, _ := server.handle(controlEnvelope(protocol.TypeHeartbeat, "heartbeat", "subscriber", ""), subscriber)
	if heartbeat.Type != protocol.TypeHeartbeat || heartbeat.Reply == nil || !*heartbeat.Reply {
		t.Fatalf("HEARTBEAT reply = %#v, want reply=true", heartbeat)
	}
	invalid, _, _ := server.handle(controlEnvelope(protocol.TypeHeartbeat, "bad-origin", "subscriber", ""), otherOrigin)
	if invalid.Type != protocol.TypeError || invalid.Code != protocol.CodeNotRegistered {
		t.Fatalf("HEARTBEAT from wrong origin = %#v, want NOT_REGISTERED", invalid)
	}

	subscribe, _, _ := server.handle(controlEnvelope(protocol.TypeSubscribe, "sub", "subscriber", "auction"), subscriber)
	if subscribe.Type != protocol.TypeOK || server.subscriptionCount("auction") != 1 {
		t.Fatalf("SUBSCRIBE reply/count = %#v/%d", subscribe, server.subscriptionCount("auction"))
	}
	now = now.Add(server.config.SessionTTL)
	server.expireSessions()
	if server.subscriptionCount("auction") != 0 {
		t.Fatalf("expired session left %d subscriptions", server.subscriptionCount("auction"))
	}
	expired, _, _ := server.handle(controlEnvelope(protocol.TypeSubscribe, "sub-after-expiry", "subscriber", "auction"), subscriber)
	if expired.Type != protocol.TypeError || expired.Code != protocol.CodeNotRegistered {
		t.Fatalf("SUBSCRIBE after expiry = %#v, want NOT_REGISTERED", expired)
	}
}

func TestCatalogAndSubscriptionsFollowLocalControlRules(t *testing.T) {
	now := time.Unix(100, 0)
	first := newControlTestServer(t, 1, &now)
	creator, subscriber := testAddr(10001), testAddr(10002)
	registerControl(t, first, creator, "creator")
	registerControl(t, first, subscriber, "subscriber")

	missing, _, _ := first.handle(controlEnvelope(protocol.TypeSubscribe, "missing", "subscriber", "absent"), subscriber)
	if missing.Type != protocol.TypeError || missing.Code != protocol.CodeTopicNotFound {
		t.Fatalf("SUBSCRIBE unknown topic = %#v, want TOPIC_NOT_FOUND", missing)
	}
	created, _, _ := first.handle(controlEnvelope(protocol.TypeCreateTopic, "create", "creator", "auction"), creator)
	if created.Type != protocol.TypeOK {
		t.Fatalf("CREATE_TOPIC reply = %#v", created)
	}
	for _, requestID := range []string{"subscribe", "subscribe-again"} {
		reply, _, _ := first.handle(controlEnvelope(protocol.TypeSubscribe, requestID, "subscriber", "auction"), subscriber)
		if reply.Type != protocol.TypeOK {
			t.Fatalf("SUBSCRIBE(%q) reply = %#v", requestID, reply)
		}
	}
	if first.subscriptionCount("auction") != 1 {
		t.Fatalf("duplicate subscription count = %d, want 1", first.subscriptionCount("auction"))
	}
	for _, requestID := range []string{"unsubscribe", "unsubscribe-again"} {
		reply, _, _ := first.handle(controlEnvelope(protocol.TypeUnsubscribe, requestID, "subscriber", "auction"), subscriber)
		if reply.Type != protocol.TypeOK {
			t.Fatalf("UNSUBSCRIBE(%q) reply = %#v", requestID, reply)
		}
	}
	if first.subscriptionCount("auction") != 0 {
		t.Fatalf("unsubscribed topic has %d clients", first.subscriptionCount("auction"))
	}
	deleted, _, _ := first.handle(controlEnvelope(protocol.TypeDeleteTopic, "delete", "creator", "auction"), creator)
	if deleted.Type != protocol.TypeOK {
		t.Fatalf("DELETE_TOPIC reply = %#v", deleted)
	}
	notFound, _, _ := first.handle(controlEnvelope(protocol.TypeDeleteTopic, "delete-again", "creator", "auction"), creator)
	if notFound.Type != protocol.TypeError || notFound.Code != protocol.CodeTopicNotFound {
		t.Fatalf("DELETE_TOPIC absent = %#v, want TOPIC_NOT_FOUND", notFound)
	}

	second := newControlTestServer(t, 2, &now)
	registerControl(t, second, creator, "creator")
	denied, _, _ := second.handle(controlEnvelope(protocol.TypeCreateTopic, "b2-create", "creator", "b2-topic"), creator)
	if denied.Type != protocol.TypeError || denied.Code != protocol.CodeCoordinatorUnavailable {
		t.Fatalf("B2 CREATE_TOPIC reply = %#v, want COORDINATOR_UNAVAILABLE", denied)
	}
}

func newControlTestServer(t *testing.T, id int, now *time.Time) *Server {
	t.Helper()
	config := DefaultConfig(id)
	config.ListenAddress = "127.0.0.1:0"
	config.PeerAddress = "127.0.0.1:9002"
	config.now = func() time.Time { return *now }
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func registerControl(t *testing.T, server *Server, addr *net.UDPAddr, clientID string) {
	t.Helper()
	reply, _, _ := server.handle(controlEnvelope(protocol.TypeRegister, "register-"+clientID, clientID, ""), addr)
	if reply.Type != protocol.TypeOK {
		t.Fatalf("REGISTER(%q) reply = %#v", clientID, reply)
	}
}

func controlEnvelope(typ protocol.Type, requestID, clientID, topic string) protocol.Envelope {
	return protocol.Envelope{Version: protocol.Version1, Type: typ, RequestID: requestID, ClientID: clientID, Topic: topic}
}

func testAddr(port int) *net.UDPAddr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
}
