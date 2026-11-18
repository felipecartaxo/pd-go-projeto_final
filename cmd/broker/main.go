package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"

	"github.com/Neraky/pd-go-cenario-2-implementacao/internal/broker"
)

func main() {
	config, err := broker.ParseConfig(os.Args[1:])
	if err != nil {
		slog.Error("invalid broker configuration", "error", err)
		os.Exit(2)
	}
	server, err := broker.NewServer(config)
	if err != nil {
		slog.Error("could not start broker", "error", err)
		os.Exit(1)
	}
	defer server.Close()

	slog.Info("broker listening", "broker_id", config.ID, "listen", server.Addr(), "peer", config.PeerAddress)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := server.Serve(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("broker stopped with error", "error", err)
		os.Exit(1)
	}
	slog.Info("broker stopped", "broker_id", config.ID)
}
