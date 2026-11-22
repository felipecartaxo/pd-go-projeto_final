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
		if err == nil {
			fmt.Print(topicActionOutput("CREATE_TOPIC", *topic))
		}
	case "delete":
		err = c.DeleteTopic(ctx, *topic)
		if err == nil {
			fmt.Print(topicActionOutput("DELETE_TOPIC", *topic))
		}
	case "unsubscribe":
		err = c.Unsubscribe(ctx, *topic)
		if err == nil {
			fmt.Print(topicActionOutput("UNSUBSCRIBE", *topic))
		}
	case "pub":
		raw, readErr := resolvePayload(*payload, *payloadFile)
		if readErr != nil {
			log.Fatal(readErr)
		}
		var messageID string
		messageID, err = c.Publish(ctx, *topic, raw)
		if err == nil {
			fmt.Print(publishOutput(*topic, messageID))
			waitForPublishEvents(ctx, c, 1*time.Second)
		}
	case "sub":
		ch, e := c.Subscribe(ctx, *topic)
		if e != nil {
			err = e
			break
		}
		fmt.Printf("subscribed client_id=%s topic=%s broker=%d\n", *clientID, *topic, c.ActiveBroker())
		for {
			select {
			case m := <-ch:
				fmt.Printf("%s %s %s\n", m.Topic, m.MessageID, m.Payload)
			case ev := <-c.Events():
				if ev.Type == pubsub.EventBrokerChanged {
					fmt.Printf("FAILOVER_OK client_id=%s migrated_to_broker=%d topic=%s\n", *clientID, c.ActiveBroker(), *topic)
					continue
				}
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

func topicActionOutput(action, topic string) string {
	return fmt.Sprintf("%s OK topic=%s\n", action, topic)
}

func publishOutput(topic, messageID string) string {
	return fmt.Sprintf("PUBLISH sent message_id=%s topic=%s\n", messageID, topic)
}

func eventOutput(ev pubsub.Event) string {
	switch ev.Type {
	case pubsub.EventNoSubscribers:
		return fmt.Sprintf("NO_SUBSCRIBERS topic=%s message_id=%s\n", ev.Topic, ev.MessageID)
	case pubsub.EventRateLimited:
		return fmt.Sprintf("RATE_LIMITED topic=%s message_id=%s code=%s\n", ev.Topic, ev.MessageID, ev.Code)
	case pubsub.EventError:
		return fmt.Sprintf("ERROR code=%s topic=%s message_id=%s\n", ev.Code, ev.Topic, ev.MessageID)
	case pubsub.EventUnavailable:
		return fmt.Sprintf("UNAVAILABLE %v\n", ev.Err)
	default:
		return fmt.Sprintf("event %s code=%s topic=%s message_id=%s\n", ev.Type, ev.Code, ev.Topic, ev.MessageID)
	}
}

func waitForPublishEvents(ctx context.Context, c *pubsub.Client, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-c.Events():
			if !ok {
				return
			}
			fmt.Print(eventOutput(ev))
		case <-timer.C:
			return
		case <-ctx.Done():
			return
		}
	}
}
