package control

import (
	"testing"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/internal/protocol"
)

func TestReplyCacheExpiresAndBoundsEntries(t *testing.T) {
	now := time.Unix(100, 0)
	cache := NewReplyCache(1, time.Second, func() time.Time { return now })
	first := ReplyKey{Type: protocol.TypeRegister, Origin: "127.0.0.1:1", RequestID: "first"}
	second := ReplyKey{Type: protocol.TypeRegister, Origin: "127.0.0.1:1", RequestID: "second"}
	response := protocol.Envelope{Version: protocol.Version1, Type: protocol.TypeOK, RequestID: "first", ClientID: "client"}

	cache.Put(first, response)
	cache.Put(second, response)
	if _, ok := cache.Get(first); ok {
		t.Fatal("oldest reply survived cache capacity eviction")
	}
	if _, ok := cache.Get(second); !ok {
		t.Fatal("newest reply was not cached")
	}
	now = now.Add(time.Second)
	if _, ok := cache.Get(second); ok {
		t.Fatal("reply survived cache TTL")
	}
	if len(cache.order) != 0 {
		t.Fatalf("expired reply left %d ordering entries", len(cache.order))
	}
}
