package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteResultJSONExportsEachMetric(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	want := result{
		Scenario:    "fairness",
		StartedAt:   time.Date(2026, time.November, 21, 12, 0, 0, 0, time.UTC),
		DurationMS:  15000,
		Publishers:  2,
		Subscribers: 20,
		Offered:     900,
		Sent:        800,
		Received:    700,
		RateLimited: 100,
		Note:        "test result",
	}

	if err := writeResult(path, want); err != nil {
		t.Fatalf("writeResult() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	for field, expected := range map[string]uint64{
		"publishers":   2,
		"subscribers":  20,
		"offered":      900,
		"sent":         800,
		"received":     700,
		"rate_limited": 100,
	} {
		value, ok := got[field]
		if !ok {
			t.Errorf("JSON result missing %q", field)
			continue
		}
		var actual uint64
		if err := json.Unmarshal(value, &actual); err != nil {
			t.Errorf("JSON result %q = %s; expected number: %v", field, value, err)
			continue
		}
		if actual != expected {
			t.Errorf("JSON result %q = %d, want %d", field, actual, expected)
		}
	}
}
