package broker

import (
	"context"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

func TestRateLimitIsPerPublisher(t *testing.T) {
	now := time.Unix(100, 0)
	server := newDataTestServer(t, &now, func(config *Config) {
		config.RateLimitPerSecond = 1
		config.RateBurst = 1
	})
	creator, publisherA, publisherB := testAddr(10001), testAddr(10002), testAddr(10003)
	registerControl(t, server, creator, "creator")
	registerControl(t, server, publisherA, "publisher-a")
	registerControl(t, server, publisherB, "publisher-b")
	createTopic(t, server, creator, "creator", "auction")

	if reply, _, ok := server.handle(publishEnvelope("publish-a-1", "publisher-a", "a-1", "auction"), publisherA); ok || reply.Type != "" {
		t.Fatalf("first PUBLISH(A) replied %#v; successful data publish has no ACK", reply)
	}
	limited, _, ok := server.handle(publishEnvelope("publish-a-2", "publisher-a", "a-2", "auction"), publisherA)
	if !ok || limited.Type != protocol.TypeRateLimited {
		t.Fatalf("second PUBLISH(A) reply = %#v, handled = %v; want RATE_LIMITED", limited, ok)
	}
	if reply, _, ok := server.handle(publishEnvelope("publish-b-1", "publisher-b", "b-1", "auction"), publisherB); ok || reply.Type != "" {
		t.Fatalf("PUBLISH(B) replied %#v; independent publisher must remain accepted", reply)
	}
	metrics := server.Metrics()
	if metrics.PublishedAccepted != 2 || metrics.RateLimited != 1 {
		t.Fatalf("metrics = %#v, want 2 accepted and 1 rate-limited", metrics)
	}
}

func TestInputDropOldestAndFairDispatch(t *testing.T) {
	now := time.Unix(100, 0)
	server := newDataTestServer(t, &now, func(config *Config) {
		config.InputQueueCapacity = 2
		config.OutputQueueCapacity = 4
	})
	creator, publisherA, publisherB, subscriber := testAddr(10001), testAddr(10002), testAddr(10003), testAddr(10004)
	registerControl(t, server, creator, "creator")
	registerControl(t, server, publisherA, "publisher-a")
	registerControl(t, server, publisherB, "publisher-b")
	registerControl(t, server, subscriber, "subscriber")
	createTopic(t, server, creator, "creator", "auction")
	subscribeTopic(t, server, subscriber, "subscriber", "auction")

	for _, id := range []string{"a-1", "a-2", "a-3"} {
		server.handle(publishEnvelope("request-"+id, "publisher-a", id, "auction"), publisherA)
	}
	server.handle(publishEnvelope("request-b-1", "publisher-b", "b-1", "auction"), publisherB)

	var delivered []string
	for range 3 {
		for _, outbound := range server.collectOutbounds(now) {
			if outbound.envelope.Type == protocol.TypeDeliver {
				delivered = append(delivered, outbound.envelope.MessageID)
			}
		}
	}
	want := []string{"a-2", "b-1", "a-3"}
	if len(delivered) != len(want) {
		t.Fatalf("DELIVER ids = %v, want %v", delivered, want)
	}
	for index := range want {
		if delivered[index] != want[index] {
			t.Fatalf("DELIVER ids = %v, want %v", delivered, want)
		}
	}
	if metrics := server.Metrics(); metrics.InputDropped != 1 {
		t.Fatalf("metrics.InputDropped = %d, want 1", metrics.InputDropped)
	}
}

func TestSlowSubscriberOutputQueueDropsOldestWithoutBlockingOthers(t *testing.T) {
	now := time.Unix(100, 0)
	server := newDataTestServer(t, &now, func(config *Config) {
		config.OutputQueueCapacity = 2
	})
	creator, publisher, slow, healthy := testAddr(10001), testAddr(10002), testAddr(10003), testAddr(10004)
	registerControl(t, server, creator, "creator")
	registerControl(t, server, publisher, "publisher")
	registerControl(t, server, slow, "slow")
	registerControl(t, server, healthy, "healthy")
	createTopic(t, server, creator, "creator", "auction")
	subscribeTopic(t, server, slow, "slow", "auction")
	subscribeTopic(t, server, healthy, "healthy", "auction")

	var healthyDelivered []string
	for _, id := range []string{"m-1", "m-2", "m-3"} {
		server.handle(publishEnvelope("request-"+id, "publisher", id, "auction"), publisher)
		server.dispatchOne(now)
		if delivery, ok := server.popOutput("healthy"); ok {
			healthyDelivered = append(healthyDelivered, delivery.MessageID)
		}
	}

	slowQueue := server.outputMessageIDs("slow")
	if got, want := slowQueue, []string{"m-2", "m-3"}; !sameStrings(got, want) {
		t.Fatalf("slow queue = %v, want %v", got, want)
	}
	if got, want := healthyDelivered, []string{"m-1", "m-2", "m-3"}; !sameStrings(got, want) {
		t.Fatalf("healthy deliveries = %v, want %v", got, want)
	}
	if metrics := server.Metrics(); metrics.OutputDropped != 1 {
		t.Fatalf("metrics.OutputDropped = %d, want 1", metrics.OutputDropped)
	}
}

func TestPublishDeliversBestEffortWithoutACK(t *testing.T) {
	now := time.Unix(100, 0)
	server := newDataTestServer(t, &now, nil)
	creator, publisher, subscriber := testAddr(10001), testAddr(10002), testAddr(10003)
	registerControl(t, server, creator, "creator")
	registerControl(t, server, publisher, "publisher")
	registerControl(t, server, subscriber, "subscriber")
	createTopic(t, server, creator, "creator", "auction")
	subscribeTopic(t, server, subscriber, "subscriber", "auction")

	response, _, hasResponse := server.handle(publishEnvelope("publish", "publisher", "message-1", "auction"), publisher)
	if hasResponse || response.Type != "" {
		t.Fatalf("PUBLISH response = %#v; successful delivery cannot receive an OK ACK", response)
	}
	outbounds := server.collectOutbounds(now)
	if len(outbounds) != 1 || outbounds[0].envelope.Type != protocol.TypeDeliver {
		t.Fatalf("outbounds = %#v, want one DELIVER", outbounds)
	}
	deliver := outbounds[0].envelope
	if deliver.ClientID != "subscriber" || deliver.MessageID != "message-1" || string(deliver.Payload) != `{"bid":1}` {
		t.Fatalf("DELIVER = %#v", deliver)
	}
	if more := server.collectOutbounds(now); len(more) != 0 {
		t.Fatalf("DELIVER was retried without an ACK: %#v", more)
	}
}

func TestServerDeliversOverUDPWithoutPositivePublishACK(t *testing.T) {
	now := time.Unix(100, 0)
	server := newDataTestServer(t, &now, nil)
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

	creator := newUDPClient(t, server)
	publisher := newUDPClient(t, server)
	subscriber := newUDPClient(t, server)
	controlRoundTrip(t, creator, controlEnvelope(protocol.TypeRegister, "register-creator", "creator", ""))
	controlRoundTrip(t, publisher, controlEnvelope(protocol.TypeRegister, "register-publisher", "publisher", ""))
	controlRoundTrip(t, subscriber, controlEnvelope(protocol.TypeRegister, "register-subscriber", "subscriber", ""))
	controlRoundTrip(t, creator, controlEnvelope(protocol.TypeCreateTopic, "create", "creator", "auction"))
	controlRoundTrip(t, subscriber, controlEnvelope(protocol.TypeSubscribe, "subscribe", "subscriber", "auction"))

	publish := publishEnvelope("publish", "publisher", "message-1", "auction")
	encoded, err := protocol.Encode(publish)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Write(encoded); err != nil {
		t.Fatal(err)
	}
	if err := subscriber.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, protocol.MaxDatagramSize)
	n, err := subscriber.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	deliver, err := protocol.Decode(buffer[:n])
	if err != nil {
		t.Fatal(err)
	}
	if deliver.Type != protocol.TypeDeliver || deliver.MessageID != "message-1" {
		t.Fatalf("received %#v, want DELIVER(message-1)", deliver)
	}
	if err := publisher.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Read(buffer); err == nil {
		t.Fatal("PUBLISH unexpectedly received a positive reply")
	} else if networkErr, ok := err.(net.Error); !ok || !networkErr.Timeout() {
		t.Fatalf("PUBLISH read error = %v, want timeout", err)
	}
}

func TestPublishBurstDoesNotCreateGoroutinePerDatagram(t *testing.T) {
	now := time.Unix(100, 0)
	server := newDataTestServer(t, &now, func(config *Config) {
		config.RateBurst = 1000
		config.InputQueueCapacity = 4
	})
	creator, publisher := testAddr(10001), testAddr(10002)
	registerControl(t, server, creator, "creator")
	registerControl(t, server, publisher, "publisher")
	createTopic(t, server, creator, "creator", "auction")
	before := runtime.NumGoroutine()
	for index := 0; index < 500; index++ {
		server.handle(publishEnvelope("request-burst-"+strconv.Itoa(index), "publisher", "message-burst-"+strconv.Itoa(index), "auction"), publisher)
	}
	if after := runtime.NumGoroutine(); after > before+1 {
		t.Fatalf("goroutines before/after burst = %d/%d; publishing must not spawn a goroutine per datagram", before, after)
	}
}

func TestNoSubscribersRetriesUntilACKOrLimit(t *testing.T) {
	now := time.Unix(100, 0)
	server := newDataTestServer(t, &now, func(config *Config) {
		config.ControlRetryInterval = time.Second
		config.ControlMaxAttempts = 3
		config.SessionTTL = time.Minute
	})
	creator, publisher := testAddr(10001), testAddr(10002)
	registerControl(t, server, creator, "creator")
	registerControl(t, server, publisher, "publisher")
	createTopic(t, server, creator, "creator", "auction")

	server.handle(publishEnvelope("no-subs", "publisher", "message-1", "auction"), publisher)
	if outbounds := server.collectOutbounds(now); len(outbounds) != 1 || outbounds[0].envelope.Type != protocol.TypeNoSubscribers {
		t.Fatalf("initial NO_SUBSCRIBERS = %#v", outbounds)
	}
	now = now.Add(time.Second)
	if outbounds := server.collectOutbounds(now); len(outbounds) != 1 || outbounds[0].envelope.Type != protocol.TypeNoSubscribers {
		t.Fatalf("first NO_SUBSCRIBERS retry = %#v", outbounds)
	}
	ack := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeACK, RequestID: "no-subs", ClientID: "publisher"}
	if reply, _, ok := server.handle(ack, publisher); ok || reply.Type != "" {
		t.Fatalf("ACK reply = %#v; ACK does not itself require an ACK", reply)
	}
	now = now.Add(5 * time.Second)
	if outbounds := server.collectOutbounds(now); len(outbounds) != 0 {
		t.Fatalf("NO_SUBSCRIBERS continued after ACK: %#v", outbounds)
	}

	server.handle(publishEnvelope("no-ack", "publisher", "message-2", "auction"), publisher)
	for attempt := 0; attempt < 3; attempt++ {
		if outbounds := server.collectOutbounds(now); len(outbounds) != 1 || outbounds[0].envelope.Type != protocol.TypeNoSubscribers {
			t.Fatalf("NO_SUBSCRIBERS attempt %d = %#v", attempt+1, outbounds)
		}
		now = now.Add(time.Second)
	}
	if outbounds := server.collectOutbounds(now); len(outbounds) != 0 {
		t.Fatalf("NO_SUBSCRIBERS exceeded retry limit: %#v", outbounds)
	}
	if metrics := server.Metrics(); metrics.NoSubscriberTimeouts != 1 {
		t.Fatalf("metrics.NoSubscriberTimeouts = %d, want 1", metrics.NoSubscriberTimeouts)
	}
}

func newDataTestServer(t *testing.T, now *time.Time, change func(*Config)) *Server {
	t.Helper()
	config := DefaultConfig(1)
	config.ListenAddress = "127.0.0.1:0"
	config.PeerAddress = "127.0.0.1:9002"
	config.now = func() time.Time { return *now }
	if change != nil {
		change(&config)
	}
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func createTopic(t *testing.T, server *Server, source *net.UDPAddr, clientID, topic string) {
	t.Helper()
	reply, _, _ := server.handle(controlEnvelope(protocol.TypeCreateTopic, "create-"+topic, clientID, topic), source)
	if reply.Type != protocol.TypeOK {
		t.Fatalf("CREATE_TOPIC(%q) = %#v", topic, reply)
	}
}

func subscribeTopic(t *testing.T, server *Server, source *net.UDPAddr, clientID, topic string) {
	t.Helper()
	reply, _, _ := server.handle(controlEnvelope(protocol.TypeSubscribe, "subscribe-"+clientID+"-"+topic, clientID, topic), source)
	if reply.Type != protocol.TypeOK {
		t.Fatalf("SUBSCRIBE(%q) = %#v", topic, reply)
	}
}

func publishEnvelope(requestID, clientID, messageID, topic string) protocol.Envelope {
	return protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePublish, RequestID: requestID, ClientID: clientID, MessageID: messageID, Topic: topic, Payload: []byte(`{"bid":1}`)}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func newUDPClient(t *testing.T, server *Server) *net.UDPConn {
	t.Helper()
	conn, err := net.DialUDP("udp", nil, server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func controlRoundTrip(t *testing.T, conn *net.UDPConn, request protocol.Envelope) protocol.Envelope {
	t.Helper()
	encoded, err := protocol.Encode(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(encoded); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, protocol.MaxDatagramSize)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := protocol.Decode(buffer[:n])
	if err != nil {
		t.Fatal(err)
	}
	if reply.Type != protocol.TypeOK {
		t.Fatalf("%s reply = %#v, want OK", request.Type, reply)
	}
	return reply
}
