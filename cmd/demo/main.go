package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/pkg/pubsub"
)

func main() {
	mode := flag.String("mode", "", "create|delete|pub|sub|unsubscribe")
	clientID := flag.String("client-id", "demo-client", "logical client ID")
	brokers := flag.String("brokers", "127.0.0.1:9001,127.0.0.1:9002", "B1,B2 addresses")
	prefer := flag.Int("prefer", 1, "preferred broker (1 or 2)")
	topic := flag.String("topic", "", "topic")
	payload := flag.String("payload", "{}", "JSON payload for pub")
	payloadFile := flag.String("payload-file", "", "read JSON payload from a UTF-8 file")
	flag.Parse()
	parts := strings.Split(*brokers, ",")
	if len(parts) != 2 || *topic == "" {
		log.Fatal("-brokers=B1,B2 and -topic are required")
	}
	c, err := pubsub.NewClient(pubsub.Config{Brokers: [2]string{parts[0], parts[1]}, ClientID: *clientID, PreferredBroker: *prefer})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	switch *mode {
	case "create":
		err = c.CreateTopic(ctx, *topic)
	case "delete":
		err = c.DeleteTopic(ctx, *topic)
	case "unsubscribe":
		err = c.Unsubscribe(ctx, *topic)
	case "pub":
		raw, readErr := resolvePayload(*payload, *payloadFile)
		if readErr != nil {
			log.Fatal(readErr)
		}
		_, err = c.Publish(ctx, *topic, raw)
	case "sub":
		ch, e := c.Subscribe(ctx, *topic)
		if e != nil {
			err = e
			break
		}
		for {
			select {
			case m := <-ch:
				fmt.Printf("%s %s %s\n", m.Topic, m.MessageID, m.Payload)
			case ev := <-c.Events():
				fmt.Printf("event %s %s\n", ev.Type, ev.Code)
			}
		}
	default:
		log.Fatal("invalid -mode")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func resolvePayload(payload, payloadFile string) (json.RawMessage, error) {
	if payloadFile == "" {
		return json.RawMessage(payload), nil
	}
	data, err := os.ReadFile(payloadFile)
	if err != nil {
		return nil, fmt.Errorf("read payload file: %w", err)
	}
	return json.RawMessage(data), nil
}
