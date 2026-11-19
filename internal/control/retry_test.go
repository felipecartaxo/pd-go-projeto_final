package control

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

func TestRetryResendsTheSameControlRequestAfterLostReply(t *testing.T) {
	request := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeRegister, RequestID: "register-1", ClientID: "client-1"}
	var seen []protocol.Envelope

	response, err := Retry(context.Background(), Policy{MaxAttempts: 3}, request, func(_ context.Context, env protocol.Envelope) (protocol.Envelope, bool, error) {
		seen = append(seen, env)
		if len(seen) == 1 { // The broker executed it, but its reply was lost.
			return protocol.Envelope{}, false, nil
		}
		return protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeOK, RequestID: env.RequestID, ClientID: env.ClientID}, true, nil
	})
	if err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	if response.Type != protocol.TypeOK || len(seen) != 2 {
		t.Fatalf("Retry() response/attempts = %#v/%d, want OK/2", response, len(seen))
	}
	for _, got := range seen {
		if got.RequestID != request.RequestID || !reflect.DeepEqual(got, request) {
			t.Fatalf("Retry() resent %#v, want unchanged %#v", got, request)
		}
	}
}

func TestRetryReturnsErrInconclusiveAfterBoundedAttempts(t *testing.T) {
	request := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeRegister, RequestID: "register-1", ClientID: "client-1"}
	attempts := 0
	_, err := Retry(context.Background(), Policy{MaxAttempts: 3}, request, func(_ context.Context, _ protocol.Envelope) (protocol.Envelope, bool, error) {
		attempts++
		return protocol.Envelope{}, false, nil
	})
	if !errors.Is(err, ErrInconclusive) {
		t.Fatalf("Retry() error = %v, want ErrInconclusive", err)
	}
	if attempts != 3 {
		t.Fatalf("Retry() attempts = %d, want 3", attempts)
	}
}

func TestRetryRejectsDataPlaneMessages(t *testing.T) {
	request := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypePublish, RequestID: "publish-1", ClientID: "client-1", Topic: "auction", MessageID: "message-1", Payload: []byte(`{}`)}
	called := false
	_, err := Retry(context.Background(), Policy{MaxAttempts: 3}, request, func(_ context.Context, _ protocol.Envelope) (protocol.Envelope, bool, error) {
		called = true
		return protocol.Envelope{}, false, nil
	})
	if err == nil {
		t.Fatal("Retry(PUBLISH) unexpectedly succeeded")
	}
	if called {
		t.Fatal("Retry(PUBLISH) attempted a data-plane resend")
	}
}
