package broker

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	valid := DefaultConfig(1)
	valid.ListenAddress = "127.0.0.1:0"
	valid.PeerAddress = "127.0.0.1:9002"
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	invalidID := valid
	invalidID.ID = 3
	if err := invalidID.Validate(); err == nil {
		t.Fatal("invalid broker ID was accepted")
	}
	invalidAddress := valid
	invalidAddress.ListenAddress = "not-an-address"
	if err := invalidAddress.Validate(); err == nil {
		t.Fatal("invalid listen address was accepted")
	}
}

func TestParseConfigUsesSelectedBrokerDefaultsAndExplicitFlags(t *testing.T) {
	config, err := ParseConfig([]string{"-id=2", "-rate-burst=17", "-heartbeat-timeout=2s"})
	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	if config.ListenAddress != "127.0.0.1:9002" || config.PeerAddress != "127.0.0.1:9001" {
		t.Fatalf("ParseConfig() addresses = %q, %q", config.ListenAddress, config.PeerAddress)
	}
	if config.RateBurst != 17 || config.HeartbeatTimeout != 2*time.Second {
		t.Fatalf("ParseConfig() limits = %#v", config)
	}
}

func TestTwoBrokersStartOnDifferentUDPLocalhostPortsAndStopGracefully(t *testing.T) {
	firstConfig := DefaultConfig(1)
	firstConfig.ListenAddress = "127.0.0.1:0"
	firstConfig.PeerAddress = "127.0.0.1:9002"
	first, err := NewServer(firstConfig)
	if err != nil {
		t.Fatalf("NewServer(B1) error = %v", err)
	}
	defer first.Close()

	secondConfig := DefaultConfig(2)
	secondConfig.ListenAddress = "127.0.0.1:0"
	secondConfig.PeerAddress = first.Addr().String()
	second, err := NewServer(secondConfig)
	if err != nil {
		t.Fatalf("NewServer(B2) error = %v", err)
	}
	defer second.Close()

	if first.Addr().Port == second.Addr().Port {
		t.Fatalf("brokers share UDP port %d", first.Addr().Port)
	}
	for _, server := range []*Server{first, second} {
		conn, err := net.DialUDP("udp", nil, server.Addr())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("ignored bootstrap datagram")); err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() { done <- first.Serve(ctx) }()
	go func() { done <- second.Serve(ctx) }()
	cancel()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Serve() did not stop after context cancellation")
		}
	}
}
