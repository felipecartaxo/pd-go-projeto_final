package pubsub

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

func TestClientSingleReaderRoutesConcurrentControlsAndDelivery(t *testing.T) {
	broker, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	var mu sync.Mutex
	var clientAddr *net.UDPAddr
	stop := make(chan struct{})
	defer close(stop)

	go func() {
		buffer := make([]byte, protocol.MaxDatagramSize)
		for {
			broker.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			n, addr, err := broker.ReadFromUDP(buffer)
			if err != nil {
				select {
				case <-stop:
					return
				default:
					continue
				}
			}
			env, err := protocol.Decode(buffer[:n])
			if err != nil {
				continue
			}
			mu.Lock()
			clientAddr = addr
			mu.Unlock()
			if env.Type == protocol.TypePublish || env.Type == protocol.TypeACK {
				continue
			}
			reply := protocol.Envelope{Version: 1, Type: protocol.TypeOK, RequestID: env.RequestID, ClientID: env.ClientID}
			data, _ := protocol.Encode(reply)
			_, _ = broker.WriteToUDP(data, addr)
		}
	}()

	c, err := NewClient(Config{
		Brokers:              [2]string{broker.LocalAddr().String(), broker.LocalAddr().String()},
		ClientID:             "client",
		ControlRetryInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, topic := range []string{"a", "b"} {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			if err := c.CreateTopic(ctx, topic); err != nil {
				t.Errorf("CreateTopic: %v", err)
			}
		}(topic)
	}
	wg.Wait()

	ch, err := c.Subscribe(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	addr := clientAddr
	mu.Unlock()
	deliver := protocol.Envelope{
		Version:   1,
		Type:      protocol.TypeDeliver,
		ClientID:  "client",
		Topic:     "a",
		MessageID: "m",
		Payload:   []byte(`{"x":1}`),
	}
	data, _ := protocol.Encode(deliver)
	if _, err := broker.WriteToUDP(data, addr); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-ch:
		if msg.MessageID != "m" {
			t.Fatalf("message=%#v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("DELIVER not routed")
	}
}

func TestClientFailoverRegistersAndRestoresSubscriptions(t *testing.T) {
	first, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	second, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()

	stop := make(chan struct{})
	defer close(stop)
	var mu sync.Mutex
	subscribedSecond := 0
	subscribeAttemptsSecond := 0
	var secondClientAddr *net.UDPAddr

	serve := func(conn *net.UDPConn, countSubscription bool, subscribeFailures int) {
		go func() {
			buffer := make([]byte, protocol.MaxDatagramSize+1)
			for {
				conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
				n, addr, err := conn.ReadFromUDP(buffer)
				if err != nil {
					select {
					case <-stop:
						return
					default:
						continue
					}
				}
				env, err := protocol.Decode(buffer[:n])
				if err != nil {
					continue
				}
				shouldFailSubscribe := false
				if env.Type == protocol.TypeSubscribe && countSubscription {
					mu.Lock()
					subscribeAttemptsSecond++
					secondClientAddr = addr
					currentAttempt := subscribeAttemptsSecond
					if currentAttempt > subscribeFailures {
						subscribedSecond++
					} else {
						shouldFailSubscribe = true
					}
					mu.Unlock()
				}
				if env.Type == protocol.TypePublish || env.Type == protocol.TypeACK {
					continue
				}
				responseType := protocol.TypeOK
				code := protocol.Code("")
				if shouldFailSubscribe {
					responseType = protocol.TypeError
					code = protocol.CodeControlBusy
				}
				response := protocol.Envelope{Version: 1, Type: responseType, RequestID: env.RequestID, ClientID: env.ClientID, Code: code}
				data, _ := protocol.Encode(response)
				_, _ = conn.WriteToUDP(data, addr)
			}
		}()
	}

	serve(first, false, 0)
	serve(second, true, 1)

	c, err := NewClient(Config{
		Brokers:               [2]string{first.LocalAddr().String(), second.LocalAddr().String()},
		ClientID:              "failover-client",
		PreferredBroker:       1,
		HeartbeatInterval:     20 * time.Millisecond,
		HeartbeatTimeout:      100 * time.Millisecond,
		ControlRetryInterval:  20 * time.Millisecond,
		ControlMaxAttempts:    2,
		DeliveryQueueCapacity: 8,
		EventQueueCapacity:    8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch, err := c.Subscribe(ctx, "falha/teste")
	if err != nil {
		t.Fatal(err)
	}
	if got := c.ActiveBroker(); got != 1 {
		t.Fatalf("active broker before failover = %d, want 1", got)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(time.Second)
	for {
		mu.Lock()
		count := subscribedSecond
		addr := secondClientAddr
		mu.Unlock()
		if count > 0 && addr != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("subscription was not restored on second broker")
		case <-time.After(10 * time.Millisecond):
		}
	}

	brokerChanged := false
	deadline = time.After(time.Second)
	for !brokerChanged {
		select {
		case event := <-c.Events():
			if event.Type == EventUnavailable {
				continue
			}
			if event.Type == EventBrokerChanged {
				brokerChanged = true
				continue
			}
		case <-deadline:
			t.Fatal("BrokerChanged event was not emitted after restore")
		}
	}
	if got := c.ActiveBroker(); got != 2 {
		t.Fatalf("active broker after failover = %d, want 2", got)
	}

	mu.Lock()
	attempts := subscribeAttemptsSecond
	mu.Unlock()
	if attempts < 2 {
		t.Fatalf("subscribe attempts = %d, want at least 2 for retry", attempts)
	}

	deliver := protocol.Envelope{
		Version:   1,
		Type:      protocol.TypeDeliver,
		ClientID:  "failover-client",
		Topic:     "falha/teste",
		MessageID: "after-failover",
		Payload:   []byte(`{"teste":"apos-falha"}`),
	}
	data, err := protocol.Encode(deliver)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	addr := secondClientAddr
	mu.Unlock()
	if _, err := second.WriteToUDP(data, addr); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-ch:
		if msg.MessageID != "after-failover" {
			t.Fatalf("message id = %q", msg.MessageID)
		}
	case <-time.After(time.Second):
		t.Fatal("DELIVER was not received after failover")
	}
}
