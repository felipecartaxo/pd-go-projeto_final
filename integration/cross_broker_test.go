package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/broker"
	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/protocol"
)

func TestTwoUDPBrokersForwardOnlyForKnownRemoteInterest(t *testing.T) {
	firstAddr := freeUDPAddress(t)
	secondAddr := freeUDPAddress(t)
	firstConfig := broker.DefaultConfig(1)
	firstConfig.ListenAddress, firstConfig.PeerAddress = firstAddr, secondAddr
	firstConfig.ControlRetryInterval = 15 * time.Millisecond
	secondConfig := broker.DefaultConfig(2)
	secondConfig.ListenAddress, secondConfig.PeerAddress = secondAddr, firstAddr
	secondConfig.ControlRetryInterval = 15 * time.Millisecond
	first, err := broker.NewServer(firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := broker.NewServer(secondConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 2)
	go func() { done <- first.Serve(ctx) }()
	go func() { done <- second.Serve(ctx) }()
	defer func() {
		cancel()
		for range 2 {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Serve() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Error("broker did not stop")
			}
		}
	}()

	creator := udpClient(t, first.Addr())
	subscriber := udpClient(t, second.Addr())
	publisher := udpClient(t, first.Addr())
	control(t, creator, protocol.Envelope{Version: 1, Type: protocol.TypeRegister, RequestID: "register-creator", ClientID: "creator"})
	control(t, publisher, protocol.Envelope{Version: 1, Type: protocol.TypeRegister, RequestID: "register-publisher", ClientID: "publisher"})
	control(t, subscriber, protocol.Envelope{Version: 1, Type: protocol.TypeRegister, RequestID: "register-subscriber", ClientID: "subscriber"})
	control(t, creator, protocol.Envelope{Version: 1, Type: protocol.TypeCreateTopic, RequestID: "create", ClientID: "creator", Topic: "auction"})

	deadline := time.Now().Add(5 * time.Second)
	for attempt := 0; ; attempt++ {
		reply := controlReply(t, subscriber, protocol.Envelope{Version: 1, Type: protocol.TypeSubscribe, RequestID: fmt.Sprintf("subscribe-%d", attempt), ClientID: "subscriber", Topic: "auction"})
		if reply.Type == protocol.TypeOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("B2 did not converge catalog: %#v", reply)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := subscriber.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; ; attempt++ {
		publish := protocol.Envelope{Version: 1, Type: protocol.TypePublish, RequestID: fmt.Sprintf("publish-%d", attempt), ClientID: "publisher", Topic: "auction", MessageID: fmt.Sprintf("message-%d", attempt), Payload: []byte(`{"bid":1}`)}
		data, err := protocol.Encode(publish)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := publisher.Write(data); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, protocol.MaxDatagramSize)
		if err := subscriber.SetReadDeadline(time.Now().Add(120 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, err := subscriber.Read(buffer)
		if err == nil {
			deliver, decodeErr := protocol.Decode(buffer[:n])
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if deliver.Type == protocol.TypeDeliver && deliver.MessageID == publish.MessageID {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("B1 did not forward to B2 after interest convergence")
		}
	}
}

func freeUDPAddress(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	address := conn.LocalAddr().String()
	_ = conn.Close()
	return address
}
func udpClient(t *testing.T, address *net.UDPAddr) *net.UDPConn {
	t.Helper()
	conn, err := net.DialUDP("udp", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
func control(t *testing.T, conn *net.UDPConn, env protocol.Envelope) {
	t.Helper()
	reply := controlReply(t, conn, env)
	if reply.Type != protocol.TypeOK {
		t.Fatalf("%s reply = %#v", env.Type, reply)
	}
}
func controlReply(t *testing.T, conn *net.UDPConn, env protocol.Envelope) protocol.Envelope {
	t.Helper()
	data, err := protocol.Encode(env)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(data); err != nil {
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
	return reply
}
