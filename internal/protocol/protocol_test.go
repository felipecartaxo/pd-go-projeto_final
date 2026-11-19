package protocol

import (
	"bytes"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func validPublish(payload []byte) Envelope {
	return Envelope{
		Version:   Version1,
		Type:      TypePublish,
		RequestID: "req-1",
		ClientID:  "publisher-1",
		Topic:     "leilao/lances",
		MessageID: "message-1",
		Payload:   payload,
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	want := validPublish([]byte(`{"participante":"João","oferta":{"valor":120,"moeda":"BRL"}}`))

	encoded, err := Encode(want)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.Version != want.Version || got.Type != want.Type || got.RequestID != want.RequestID || got.ClientID != want.ClientID || got.Topic != want.Topic || got.MessageID != want.MessageID {
		t.Fatalf("Decode() envelope = %#v, want %#v", got, want)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Decode() payload = %s, want %s", got.Payload, want.Payload)
	}
}

func TestEncodeAcceptsEnvelopeExactlyAtMaximumSize(t *testing.T) {
	base := validPublish([]byte(`""`))
	var encoded []byte
	for payloadLength := 0; payloadLength <= MaxDatagramSize; payloadLength++ {
		candidate := base
		candidate.Payload = []byte(`"` + strings.Repeat("x", payloadLength) + `"`)
		var err error
		encoded, err = Encode(candidate)
		if err != nil {
			continue
		}
		if len(encoded) == MaxDatagramSize {
			return
		}
	}
	t.Fatalf("could not construct a valid envelope of exactly %d bytes; last size %d", MaxDatagramSize, len(encoded))
}

func TestEncodeRejectsEnvelopeLargerThanMaximumSize(t *testing.T) {
	env := validPublish([]byte(`"` + strings.Repeat("x", MaxDatagramSize) + `"`))
	_, err := Encode(env)
	if !IsCode(err, CodeDatagramTooLarge) {
		t.Fatalf("Encode() error = %v, want %s", err, CodeDatagramTooLarge)
	}
}

func TestDecodeRejectsDatagramLargerThanMaximumSize(t *testing.T) {
	_, err := Decode([]byte(strings.Repeat(" ", MaxDatagramSize+1)))
	if !IsCode(err, CodeDatagramTooLarge) {
		t.Fatalf("Decode() error = %v, want %s", err, CodeDatagramTooLarge)
	}
}

func TestDecodeRejectsMalformedJSON(t *testing.T) {
	_, err := Decode([]byte(`{"v":1,"type":"PUBLISH",`))
	if !IsCode(err, CodeInvalidMessage) {
		t.Fatalf("Decode() error = %v, want %s", err, CodeInvalidMessage)
	}
}

func TestDecodeRejectsMissingRequiredField(t *testing.T) {
	_, err := Decode([]byte(`{"v":1,"type":"PUBLISH","request_id":"r","client_id":"c","topic":"t","payload":{}}`))
	if !IsCode(err, CodeInvalidMessage) {
		t.Fatalf("Decode() error = %v, want %s", err, CodeInvalidMessage)
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	_, err := Decode([]byte(`{"v":2,"type":"REGISTER","request_id":"r","client_id":"c"}`))
	if !IsCode(err, CodeInvalidMessage) {
		t.Fatalf("Decode() error = %v, want %s", err, CodeInvalidMessage)
	}
}

func TestEncodedEnvelopeTravelsAsOneUDPDatagram(t *testing.T) {
	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := net.ListenUDP("udp", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	want := validPublish([]byte(`{"evento":"lance"}`))
	encoded, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.WriteToUDP(encoded, receiver.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, MaxDatagramSize)
	n, _, err := receiver.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != want.MessageID || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("received envelope = %#v, want %#v", got, want)
	}
}

func TestDecodeSupportsConcurrentCallers(t *testing.T) {
	data, err := Encode(validPublish([]byte(`{"bid":1}`)))
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	const iterations = 200
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for range iterations {
				envelope, decodeErr := Decode(data)
				if decodeErr != nil || envelope.MessageID != "message-1" {
					t.Errorf("Decode() = %#v, %v", envelope, decodeErr)
					return
				}
			}
		}()
	}
	group.Wait()
}
