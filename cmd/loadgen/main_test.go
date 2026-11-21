package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteResultJSONExportsEachMetric(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	want := result{
		Scenario:    "fairness",
		RunID:       "run-001",
		StartedAt:   time.Date(2026, time.November, 21, 12, 0, 0, 0, time.UTC),
		DurationMS:  15000,
		Publishers:  2,
		Subscribers: 20,
		Topic:       "leilao/lances",
		Offered:     900,
		Sent:        800,
		Received:    700,
		RateLimited: 100,
		GoVersion:   "go1.26.1",
		GOOS:        "windows",
		GOARCH:      "amd64",
		CPUCount:    8,
		GOMAXPROCS:  1,
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

func TestWriteResultCSVIncludesEnvironmentAndRunIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.csv")
	want := result{
		Scenario:    "fanout",
		RunID:       "run-001",
		StartedAt:   time.Date(2026, time.November, 21, 12, 0, 0, 0, time.UTC),
		DurationMS:  30000,
		Publishers:  1,
		Subscribers: 20,
		Topic:       "loadgen/fanout-manual",
		Offered:     1499,
		Sent:        1499,
		Received:    29980,
		RateLimited: 0,
		GoVersion:   "go1.26.1",
		GOOS:        "windows",
		GOARCH:      "amd64",
		CPUCount:    8,
		GOMAXPROCS:  1,
		Note:        "test result",
	}

	if err := writeResult(path, want); err != nil {
		t.Fatalf("writeResult() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	text := string(data)
	for _, field := range []string{
		"run_id",
		"topic",
		"go_version",
		"goos",
		"goarch",
		"cpu_count",
		"gomaxprocs",
		"run-001",
		"loadgen/fanout-manual",
		"go1.26.1",
		"windows",
		"amd64",
	} {
		if !strings.Contains(text, field) {
			t.Errorf("CSV result missing %q:\n%s", field, text)
		}
	}
}
