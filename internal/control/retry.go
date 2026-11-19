// Package control provides bounded, reusable building blocks for reliable
// control-plane exchanges. It deliberately has no data-plane retry support.
package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

// ErrInconclusive reports that every permitted control attempt elapsed without
// a response. It does not establish whether a remote broker applied the request.
var ErrInconclusive = errors.New("control result is inconclusive")

// Policy bounds a control exchange. MaxAttempts includes the first send.
type Policy struct {
	MaxAttempts int
	RetryAfter  time.Duration
}

// Retry sends an unchanged control request until a response is received, a
// terminal send error occurs, the context ends, or the bounded attempt count is
// exhausted. received=false represents a lost or timed-out reply for one send.
func Retry(ctx context.Context, policy Policy, request protocol.Envelope, send func(context.Context, protocol.Envelope) (response protocol.Envelope, received bool, err error)) (protocol.Envelope, error) {
	if policy.MaxAttempts <= 0 {
		return protocol.Envelope{}, fmt.Errorf("control max attempts must be positive")
	}
	if policy.RetryAfter < 0 {
		return protocol.Envelope{}, fmt.Errorf("control retry interval cannot be negative")
	}
	if err := protocol.Validate(request); err != nil {
		return protocol.Envelope{}, err
	}
	if !retryableType(request.Type) {
		return protocol.Envelope{}, fmt.Errorf("%s is not a retryable control message", request.Type)
	}
	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return protocol.Envelope{}, err
		}
		response, received, err := send(ctx, request)
		if err != nil {
			return protocol.Envelope{}, err
		}
		if received {
			return response, nil
		}
		if attempt+1 < policy.MaxAttempts && policy.RetryAfter > 0 {
			timer := time.NewTimer(policy.RetryAfter)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return protocol.Envelope{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return protocol.Envelope{}, fmt.Errorf("%w after %d attempts", ErrInconclusive, policy.MaxAttempts)
}

func retryableType(typ protocol.Type) bool {
	switch typ {
	case protocol.TypeRegister, protocol.TypeCreateTopic, protocol.TypeDeleteTopic,
		protocol.TypeSubscribe, protocol.TypeUnsubscribe, protocol.TypeNoSubscribers,
		protocol.TypeTopicRequest, protocol.TypeTopicUpdate, protocol.TypeInterestUpdate,
		protocol.TypePeerSync:
		return true
	default:
		return false
	}
}
