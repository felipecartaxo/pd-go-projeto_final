package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/felipecartaxo/pd-go-projeto_final/pkg/pubsub"
)

func TestResolvePayload(t *testing.T) {
	t.Run("direct payload", func(t *testing.T) {
		raw, err := resolvePayload(`{"valor":120}`, "")
		if err != nil {
			t.Fatal(err)
		}
		if got := string(raw); got != `{"valor":120}` {
			t.Fatalf("payload = %q", got)
		}
	})

	t.Run("payload file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "payload.json")
		if err := os.WriteFile(path, []byte(`{"valor":120,"lote":3}`), 0o600); err != nil {
			t.Fatal(err)
		}
		raw, err := resolvePayload("", path)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(raw); got != `{"valor":120,"lote":3}` {
			t.Fatalf("payload = %q", got)
		}
	})
}

func TestTopicActionOutput(t *testing.T) {
	tests := []struct {
		action string
		topic  string
		want   string
	}{
		{action: "CREATE_TOPIC", topic: "leilao/lances", want: "CREATE_TOPIC OK topic=leilao/lances\n"},
		{action: "DELETE_TOPIC", topic: "leilao/lances", want: "DELETE_TOPIC OK topic=leilao/lances\n"},
		{action: "UNSUBSCRIBE", topic: "leilao/lances", want: "UNSUBSCRIBE OK topic=leilao/lances\n"},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			if got := topicActionOutput(tt.action, tt.topic); got != tt.want {
				t.Fatalf("topicActionOutput() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPublishOutput(t *testing.T) {
	got := publishOutput("leilao/lances", "msg-123")
	want := "PUBLISH sent message_id=msg-123 topic=leilao/lances\n"
	if got != want {
		t.Fatalf("publishOutput() = %q, want %q", got, want)
	}
}

func TestEventOutput(t *testing.T) {
	tests := []struct {
		name string
		ev   pubsub.Event
		want string
	}{
		{
			name: "no subscribers",
			ev: pubsub.Event{
				Type:      pubsub.EventNoSubscribers,
				Topic:     "leilao/lances",
				MessageID: "msg-123",
			},
			want: "NO_SUBSCRIBERS topic=leilao/lances message_id=msg-123\n",
		},
		{
			name: "rate limited",
			ev: pubsub.Event{
				Type:      pubsub.EventRateLimited,
				Topic:     "leilao/lances",
				MessageID: "msg-123",
				Code:      "RATE_LIMITED",
			},
			want: "RATE_LIMITED topic=leilao/lances message_id=msg-123 code=RATE_LIMITED\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eventOutput(tt.ev); got != tt.want {
				t.Fatalf("eventOutput() = %q, want %q", got, tt.want)
			}
		})
	}
}
