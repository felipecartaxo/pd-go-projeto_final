package main

import (
	"os"
	"path/filepath"
	"testing"
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
