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
		b := make([]byte, protocol.MaxDatagramSize)
		for {
			broker.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			n, addr, e := broker.ReadFromUDP(b)
			if e != nil {
				select {
				case <-stop:
					return
				default:
					continue
				}
			}
			env, e := protocol.Decode(b[:n])
			if e != nil {
				continue
			}
			mu.Lock()
			clientAddr = addr
			mu.Unlock()
			if env.Type == protocol.TypePublish {
				continue
			}
			if env.Type == protocol.TypeACK {
				continue
			}
			reply := protocol.Envelope{Version: 1, Type: protocol.TypeOK, RequestID: env.RequestID, ClientID: env.ClientID}
			data, _ := protocol.Encode(reply)
			broker.WriteToUDP(data, addr)
		}
	}()
	c, err := NewClient(Config{Brokers: [2]string{broker.LocalAddr().String(), broker.LocalAddr().String()}, ClientID: "client", ControlRetryInterval: 20 * time.Millisecond})
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
	deliver := protocol.Envelope{Version: 1, Type: protocol.TypeDeliver, ClientID: "client", Topic: "a", MessageID: "m", Payload: []byte(`{"x":1}`)}
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
