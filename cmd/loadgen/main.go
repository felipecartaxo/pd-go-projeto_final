package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/felipecartaxo/pd-go-projeto_final/pkg/pubsub"
)

type result struct {
	Scenario    string    `json:"scenario"`
	RunID       string    `json:"run_id"`
	StartedAt   time.Time `json:"started_at"`
	DurationMS  int64     `json:"duration_ms"`
	Publishers  int       `json:"publishers"`
	Subscribers int       `json:"subscribers"`
	Topic       string    `json:"topic"`
	Offered     uint64    `json:"offered"`
	Sent        uint64    `json:"sent"`
	Received    uint64    `json:"received"`
	RateLimited uint64    `json:"rate_limited"`
	GoVersion   string    `json:"go_version"`
	GOOS        string    `json:"goos"`
	GOARCH      string    `json:"goarch"`
	CPUCount    int       `json:"cpu_count"`
	GOMAXPROCS  int       `json:"gomaxprocs"`
	Note        string    `json:"note"`
}

func main() {
	scenario := flag.String("scenario", "fanout", "fanout|burst|fairness|failover")
	duration := flag.Duration("duration", 15*time.Second, "duration")
	publishers := flag.Int("publishers", 1, "publishers")
	subscribers := flag.Int("subscribers", 1, "subscribers")
	brokers := flag.String("brokers", "127.0.0.1:9001,127.0.0.1:9002", "B1,B2")
	output := flag.String("output", "results.json", ".json or .csv")
	topic := flag.String("topic", "loadgen/events", "topic")
	gomaxprocs := flag.Int("gomaxprocs", 0, "maximum Go worker threads; 0 keeps the runtime default")
	runID := flag.String("run-id", "", "optional identifier for comparing repeated executions")
	flag.Parse()
	if *gomaxprocs < 0 {
		log.Fatal("-gomaxprocs must be zero or positive")
	}
	if *gomaxprocs > 0 {
		runtime.GOMAXPROCS(*gomaxprocs)
	}
	parts := strings.Split(*brokers, ",")
	if len(parts) != 2 {
		log.Fatal("-brokers must be B1,B2")
	}
	if *publishers < 1 || *subscribers < 1 || *duration <= 0 {
		log.Fatal("positive duration, publishers and subscribers required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	started := time.Now()
	executionID := *runID
	if executionID == "" {
		executionID = fmt.Sprintf("run-%d", started.UnixNano())
	}
	cfg := func(id string, prefer int) pubsub.Config {
		return pubsub.Config{Brokers: [2]string{parts[0], parts[1]}, ClientID: id, PreferredBroker: prefer}
	}
	admin, err := pubsub.NewClient(cfg("loadgen-admin", 1))
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()
	if err = admin.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if err = admin.CreateTopic(ctx, *topic); err != nil {
		log.Printf("create topic: %v (continuing if it already exists)", err)
	}
	var received, limited uint64
	var clients []*pubsub.Client
	var wg sync.WaitGroup
	for i := 0; i < *subscribers; i++ {
		c, e := pubsub.NewClient(cfg(fmt.Sprintf("loadgen-sub-%d", i), 1+i%2))
		if e != nil {
			log.Fatal(e)
		}
		if e = c.Connect(ctx); e != nil {
			log.Fatal(e)
		}
		ch, e := c.Subscribe(ctx, *topic)
		if e != nil {
			log.Fatal(e)
		}
		clients = append(clients, c)
		wg.Add(1)
		go func(c *pubsub.Client, ch <-chan pubsub.Message) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-ch:
					if !ok {
						return
					}
					atomic.AddUint64(&received, 1)
				case ev := <-c.Events():
					if ev.Type == pubsub.EventRateLimited {
						atomic.AddUint64(&limited, 1)
					}
				}
			}
		}(c, ch)
	}
	var offered, sent uint64
	for i := 0; i < *publishers; i++ {
		c, e := pubsub.NewClient(cfg(fmt.Sprintf("loadgen-pub-%d", i), 1))
		if e != nil {
			log.Fatal(e)
		}
		if e = c.Connect(ctx); e != nil {
			log.Fatal(e)
		}
		clients = append(clients, c)
		wg.Add(1)
		go func(i int, c *pubsub.Client) {
			defer wg.Done()
			interval := 20 * time.Millisecond
			if (*scenario == "burst" || *scenario == "fairness") && i == 0 {
				interval = time.Millisecond
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				atomic.AddUint64(&offered, 1)
				if _, e := c.Publish(ctx, *topic, json.RawMessage(`{"loadgen":true}`)); e == nil {
					atomic.AddUint64(&sent, 1)
				}
			}
		}(i, c)
	}
	<-ctx.Done()
	for _, c := range clients {
		_ = c.Close()
	}
	wg.Wait()
	r := result{
		Scenario:    *scenario,
		RunID:       executionID,
		StartedAt:   started,
		DurationMS:  time.Since(started).Milliseconds(),
		Publishers:  *publishers,
		Subscribers: *subscribers,
		Topic:       *topic,
		Offered:     offered,
		Sent:        sent,
		Received:    received,
		RateLimited: limited,
		GoVersion:   runtime.Version(),
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		CPUCount:    runtime.NumCPU(),
		GOMAXPROCS:  runtime.GOMAXPROCS(0),
		Note:        "sent is a local UDP send attempt; received is observed DELIVER; failover requires stopping/restarting B1 externally",
	}
	if err := writeResult(*output, r); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s: offered=%d sent=%d received=%d rate_limited=%d\n", *output, r.Offered, r.Sent, r.Received, r.RateLimited)
}
func writeResult(path string, r result) error {
	f, e := os.Create(path)
	if e != nil {
		return e
	}
	defer f.Close()
	if strings.HasSuffix(path, ".csv") {
		w := csv.NewWriter(f)
		defer w.Flush()
		if e = w.Write([]string{"scenario", "run_id", "started_at", "duration_ms", "publishers", "subscribers", "topic", "offered", "sent", "received", "rate_limited", "go_version", "goos", "goarch", "cpu_count", "gomaxprocs", "note"}); e != nil {
			return e
		}
		return w.Write([]string{r.Scenario, r.RunID, r.StartedAt.Format(time.RFC3339Nano), fmt.Sprint(r.DurationMS), fmt.Sprint(r.Publishers), fmt.Sprint(r.Subscribers), r.Topic, fmt.Sprint(r.Offered), fmt.Sprint(r.Sent), fmt.Sprint(r.Received), fmt.Sprint(r.RateLimited), r.GoVersion, r.GOOS, r.GOARCH, fmt.Sprint(r.CPUCount), fmt.Sprint(r.GOMAXPROCS), r.Note})
	}
	return json.NewEncoder(f).Encode(r)
}
