// Package broker contains the UDP process lifecycle. Message handling is added in later tasks.
package broker

import (
	"context"
	"errors"
	"net"
	"sync"
)

// Server owns one UDP listener and can be shut down through its context or Close.
type Server struct {
	config Config
	conn   *net.UDPConn

	closeOnce sync.Once
	closeErr  error
}

// NewServer validates config and binds its UDP socket. It does not start protocol processing.
func NewServer(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	address, err := net.ResolveUDPAddr("udp", config.ListenAddress)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", address)
	if err != nil {
		return nil, err
	}
	return &Server{config: config, conn: conn}, nil
}

// Addr returns the local address assigned to the UDP socket.
func (s *Server) Addr() *net.UDPAddr {
	return s.conn.LocalAddr().(*net.UDPAddr)
}

// Serve reads and discards bootstrap datagrams until context cancellation or Close.
// Full protocol dispatch belongs to subsequent broker tasks.
func (s *Server) Serve(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-stopped:
		}
	}()
	defer close(stopped)

	buffer := make([]byte, s.config.MaxDatagramBytes)
	for {
		_, _, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
	}
}

// Close releases the UDP socket. It is safe to call repeatedly.
func (s *Server) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.conn.Close() })
	return s.closeErr
}
