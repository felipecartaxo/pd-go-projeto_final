// Package protocol implements the versioned JSON envelope carried by one UDP datagram.
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	// Version1 is the only wire-protocol version currently accepted.
	Version1 = 1
	// MaxDatagramSize is the maximum number of bytes in a complete serialized JSON envelope.
	MaxDatagramSize = 1200
	maxIDBytes      = 64
	maxTopicBytes   = 64
)

// Type identifies an operation in protocol v1.
type Type string

const (
	TypeRegister           Type = "REGISTER"
	TypeCreateTopic        Type = "CREATE_TOPIC"
	TypeDeleteTopic        Type = "DELETE_TOPIC"
	TypeSubscribe          Type = "SUBSCRIBE"
	TypeUnsubscribe        Type = "UNSUBSCRIBE"
	TypePublish            Type = "PUBLISH"
	TypeDeliver            Type = "DELIVER"
	TypeHeartbeat          Type = "HEARTBEAT"
	TypeNoSubscribers      Type = "NO_SUBSCRIBERS"
	TypeRateLimited        Type = "RATE_LIMITED"
	TypeOK                 Type = "OK"
	TypeError              Type = "ERROR"
	TypeACK                Type = "ACK"
	TypeTopicRequest       Type = "TOPIC_REQUEST"
	TypeTopicUpdate        Type = "TOPIC_UPDATE"
	TypeInterestUpdate     Type = "INTEREST_UPDATE"
	TypeForwardPublication Type = "FORWARD_PUBLICATION"
	TypePeerSync           Type = "PEER_SYNC"
	TypePeerHeartbeat      Type = "PEER_HEARTBEAT"
)

// Code is a protocol error code.
type Code string

const (
	CodeInvalidMessage         Code = "INVALID_MESSAGE"
	CodeDatagramTooLarge       Code = "DATAGRAM_TOO_LARGE"
	CodeNotRegistered          Code = "NOT_REGISTERED"
	CodeTopicNotFound          Code = "TOPIC_NOT_FOUND"
	CodeTopicAlreadyExists     Code = "TOPIC_ALREADY_EXISTS"
	CodeCoordinatorUnavailable Code = "COORDINATOR_UNAVAILABLE"
	CodeRateLimited            Code = "RATE_LIMITED"
	CodeControlBusy            Code = "CONTROL_BUSY"
	CodeSyncRequired           Code = "SYNC_REQUIRED"
	CodeInternalError          Code = "INTERNAL_ERROR"
)

// Envelope is the complete JSON object serialized into one UDP datagram. Payload and
// Items retain raw JSON so callers do not lose their original JSON value semantics.
type Envelope struct {
	Version         int             `json:"v"`
	Type            Type            `json:"type"`
	RequestID       string          `json:"request_id,omitempty"`
	ClientID        string          `json:"client_id,omitempty"`
	Topic           string          `json:"topic,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	MessageID       string          `json:"message_id,omitempty"`
	BrokerID        int             `json:"broker_id,omitempty"`
	Code            Code            `json:"code,omitempty"`
	Reply           *bool           `json:"reply,omitempty"`
	Action          string          `json:"action,omitempty"`
	CatalogEpoch    uint64          `json:"catalog_epoch,omitempty"`
	CatalogRevision uint64          `json:"catalog_revision,omitempty"`
	InterestVersion uint64          `json:"interest_version,omitempty"`
	PeerEpoch       uint64          `json:"peer_epoch,omitempty"`
	Present         *bool           `json:"present,omitempty"`
	SyncID          string          `json:"sync_id,omitempty"`
	Scope           string          `json:"scope,omitempty"`
	Part            uint32          `json:"part,omitempty"`
	Total           uint32          `json:"total,omitempty"`
	Status          string          `json:"status,omitempty"`
	Items           json.RawMessage `json:"items,omitempty"`
}

// ProtocolError indicates that a datagram does not meet a wire-protocol rule.
type ProtocolError struct {
	Code Code
	Err  error
}

func (e *ProtocolError) Error() string {
	if e.Err == nil {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e *ProtocolError) Unwrap() error { return e.Err }

func protocolError(code Code, format string, args ...any) error {
	return &ProtocolError{Code: code, Err: fmt.Errorf(format, args...)}
}

// IsCode reports whether err is a protocol error with code.
func IsCode(err error, code Code) bool {
	var protocolErr *ProtocolError
	return errors.As(err, &protocolErr) && protocolErr.Code == code
}

// Encode validates env and serializes it into a single datagram-safe JSON envelope.
func Encode(env Envelope) ([]byte, error) {
	if err := Validate(env); err != nil {
		return nil, err
	}
	data, err := json.Marshal(env)
	if err != nil {
		return nil, protocolError(CodeInvalidMessage, "marshal envelope: %v", err)
	}
	if len(data) > MaxDatagramSize {
		return nil, protocolError(CodeDatagramTooLarge, "serialized envelope is %d bytes; maximum is %d", len(data), MaxDatagramSize)
	}
	return data, nil
}

// Decode verifies byte length and UTF-8 before parsing and validating a protocol v1 envelope.
func Decode(data []byte) (Envelope, error) {
	if len(data) > MaxDatagramSize {
		return Envelope{}, protocolError(CodeDatagramTooLarge, "datagram is %d bytes; maximum is %d", len(data), MaxDatagramSize)
	}
	if !utf8.Valid(data) {
		return Envelope{}, protocolError(CodeInvalidMessage, "datagram is not valid UTF-8")
	}
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, protocolError(CodeInvalidMessage, "decode JSON: %v", err)
	}
	if err := Validate(env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Validate checks fields that are mandatory for each protocol v1 message type.
func Validate(env Envelope) error {
	if env.Version != Version1 {
		return protocolError(CodeInvalidMessage, "unsupported protocol version %d", env.Version)
	}
	if !knownType(env.Type) {
		return protocolError(CodeInvalidMessage, "unknown message type %q", env.Type)
	}
	if err := validateOptionalFields(env); err != nil {
		return err
	}

	switch env.Type {
	case TypeRegister:
		return requireRequestAndClient(env)
	case TypeCreateTopic, TypeDeleteTopic, TypeSubscribe, TypeUnsubscribe:
		return requireRequestClientAndTopic(env)
	case TypePublish:
		if err := requireRequestClientAndTopic(env); err != nil {
			return err
		}
		return requireMessageAndPayload(env)
	case TypeDeliver:
		if err := requireClientAndTopic(env); err != nil {
			return err
		}
		return requireMessageAndPayload(env)
	case TypeHeartbeat:
		return requireRequestAndClient(env)
	case TypeNoSubscribers, TypeRateLimited:
		if err := requireRequestClientAndTopic(env); err != nil {
			return err
		}
		return requireMessageID(env)
	case TypeOK, TypeError:
		return requireRequestAndClient(env)
	case TypeACK:
		if err := requireRequestID(env); err != nil {
			return err
		}
		if env.ClientID == "" && env.BrokerID == 0 {
			return protocolError(CodeInvalidMessage, "ACK requires client_id or broker_id")
		}
		return nil
	case TypeTopicRequest:
		if err := requireRequestID(env); err != nil {
			return err
		}
		return requireBrokerActionAndTopic(env)
	case TypeTopicUpdate:
		if err := requireBrokerActionAndTopic(env); err != nil {
			return err
		}
		if env.CatalogEpoch == 0 || env.CatalogRevision == 0 {
			return protocolError(CodeInvalidMessage, "TOPIC_UPDATE requires catalog_epoch and catalog_revision")
		}
		return nil
	case TypeInterestUpdate:
		if err := requireRequestID(env); err != nil {
			return err
		}
		if err := requireBrokerAndTopic(env); err != nil {
			return err
		}
		if env.Present == nil || env.PeerEpoch == 0 || env.InterestVersion == 0 {
			return protocolError(CodeInvalidMessage, "INTEREST_UPDATE requires present, peer_epoch and interest_version")
		}
		return nil
	case TypeForwardPublication:
		if err := requireBrokerAndTopic(env); err != nil {
			return err
		}
		return requireMessageAndPayload(env)
	case TypePeerSync:
		return requireRequestID(env)
	case TypePeerHeartbeat:
		if err := requireRequestID(env); err != nil {
			return err
		}
		return requireBrokerID(env)
	default:
		return protocolError(CodeInvalidMessage, "unsupported message type %q", env.Type)
	}
}

func validateOptionalFields(env Envelope) error {
	for _, field := range []struct {
		name  string
		value string
	}{{"request_id", env.RequestID}, {"client_id", env.ClientID}, {"message_id", env.MessageID}, {"sync_id", env.SyncID}} {
		if field.value != "" && (!utf8.ValidString(field.value) || len(field.value) > maxIDBytes) {
			return protocolError(CodeInvalidMessage, "%s must be valid UTF-8 and at most %d bytes", field.name, maxIDBytes)
		}
	}
	if env.Topic != "" && (!utf8.ValidString(env.Topic) || len(env.Topic) > maxTopicBytes) {
		return protocolError(CodeInvalidMessage, "topic must be valid UTF-8 and at most %d bytes", maxTopicBytes)
	}
	if len(env.Payload) > 0 && !json.Valid(env.Payload) {
		return protocolError(CodeInvalidMessage, "payload is not valid JSON")
	}
	if len(env.Items) > 0 && !json.Valid(env.Items) {
		return protocolError(CodeInvalidMessage, "items is not valid JSON")
	}
	if env.Code != "" && !knownCode(env.Code) {
		return protocolError(CodeInvalidMessage, "unknown code %q", env.Code)
	}
	if env.Action != "" && env.Action != "create" && env.Action != "delete" && env.Action != "request" && env.Action != "chunk" && env.Action != "complete" {
		return protocolError(CodeInvalidMessage, "unknown action %q", env.Action)
	}
	if env.Status != "" && env.Status != "OK" && env.Status != "ERROR" {
		return protocolError(CodeInvalidMessage, "unknown ACK status %q", env.Status)
	}
	return nil
}

func requireRequestAndClient(env Envelope) error {
	if err := requireRequestID(env); err != nil {
		return err
	}
	return requireClientID(env)
}

func requireRequestClientAndTopic(env Envelope) error {
	if err := requireRequestAndClient(env); err != nil {
		return err
	}
	return requireTopic(env)
}

func requireClientAndTopic(env Envelope) error {
	if err := requireClientID(env); err != nil {
		return err
	}
	return requireTopic(env)
}

func requireBrokerActionAndTopic(env Envelope) error {
	if err := requireBrokerAndTopic(env); err != nil {
		return err
	}
	if env.Action != "create" && env.Action != "delete" {
		return protocolError(CodeInvalidMessage, "message requires action create or delete")
	}
	return nil
}

func requireBrokerAndTopic(env Envelope) error {
	if err := requireBrokerID(env); err != nil {
		return err
	}
	return requireTopic(env)
}

func requireRequestID(env Envelope) error {
	if env.RequestID == "" {
		return protocolError(CodeInvalidMessage, "request_id is required")
	}
	return nil
}

func requireClientID(env Envelope) error {
	if env.ClientID == "" {
		return protocolError(CodeInvalidMessage, "client_id is required")
	}
	return nil
}

func requireTopic(env Envelope) error {
	if env.Topic == "" {
		return protocolError(CodeInvalidMessage, "topic is required")
	}
	return nil
}

func requireMessageID(env Envelope) error {
	if env.MessageID == "" {
		return protocolError(CodeInvalidMessage, "message_id is required")
	}
	return nil
}

func requireMessageAndPayload(env Envelope) error {
	if err := requireMessageID(env); err != nil {
		return err
	}
	if len(env.Payload) == 0 {
		return protocolError(CodeInvalidMessage, "payload is required")
	}
	return nil
}

func requireBrokerID(env Envelope) error {
	if env.BrokerID != 1 && env.BrokerID != 2 {
		return protocolError(CodeInvalidMessage, "broker_id must be 1 or 2")
	}
	return nil
}

func knownType(value Type) bool {
	switch value {
	case TypeRegister, TypeCreateTopic, TypeDeleteTopic, TypeSubscribe, TypeUnsubscribe,
		TypePublish, TypeDeliver, TypeHeartbeat, TypeNoSubscribers, TypeRateLimited,
		TypeOK, TypeError, TypeACK, TypeTopicRequest, TypeTopicUpdate, TypeInterestUpdate,
		TypeForwardPublication, TypePeerSync, TypePeerHeartbeat:
		return true
	default:
		return false
	}
}

func knownCode(value Code) bool {
	switch value {
	case CodeInvalidMessage, CodeDatagramTooLarge, CodeNotRegistered, CodeTopicNotFound,
		CodeTopicAlreadyExists, CodeCoordinatorUnavailable, CodeRateLimited, CodeControlBusy,
		CodeSyncRequired, CodeInternalError:
		return true
	default:
		return false
	}
}
