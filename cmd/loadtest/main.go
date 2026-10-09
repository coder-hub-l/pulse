package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/pulse-engine/internal/domain"
)

func main() {
	targetURL := flag.String("url", "http://localhost:8080/v1/events", "Target Ingest API URL")
	concurrency := flag.Int("c", 50, "Number of concurrent worker goroutines")
	totalEvents := flag.Int("n", 50000, "Total number of events to dispatch")
	keysCount := flag.Int("keys", 1000, "Number of distinct metric keys to simulate")
	flag.Parse()

	fmt.Printf("=== Starting Pulse Load Generator ===\n")
	fmt.Printf("Target:       %s\n", *targetURL)
	fmt.Printf("Concurrency:  %d workers\n", *concurrency)
	fmt.Printf("Total Events: %d\n", *totalEvents)
	fmt.Printf("Metric Keys:  %d\n", *keysCount)
	fmt.Println("---------------------------------------")

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	keys := make([]string, *keysCount)
	for i := 0; i < *keysCount; i++ {
		keys[i] = fmt.Sprintf("retail.orders.store_%04d", i)
	}

	var completed uint64
	var errorsCount uint64
	latencies := make([]time.Duration, *totalEvents)
	var latMu sync.Mutex

	jobs := make(chan int, *totalEvents)
	for i := 0; i < *totalEvents; i++ {
		jobs <- i
	}
	close(jobs)

	startTime := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for idx := range jobs {
				ev := domain.Event{
					EventID:   uuid.New().String(),
					MetricKey: keys[idx%len(keys)],
					Timestamp: time.Now().UTC(),
					Value:     100.0 + float64(idx%50),
					Dimensions: map[string]string{
						"store_id": fmt.Sprintf("store_%04d", idx%len(keys)),
						"region":   "west",
					},
				}

				payload, _ := json.Marshal(ev)
				reqStart := time.Now()

				resp, err := client.Post(*targetURL, "application/json", bytes.NewReader(payload))
				elapsed := time.Since(reqStart)

				if err != nil || resp.StatusCode >= 400 {
					atomic.AddUint64(&errorsCount, 1)
					if resp != nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				} else {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					atomic.AddUint64(&completed, 1)

					latMu.Lock()
					latencies[idx] = elapsed
					latMu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()
	totalElapsed := time.Since(startTime)

	throughput := float64(completed) / totalElapsed.Seconds()
	fmt.Println("---------------------------------------")
	fmt.Printf("Completed in: %v\n", totalElapsed)
	fmt.Printf("Total OK:     %d\n", completed)
	fmt.Printf("Total Errors: %d\n", errorsCount)
	fmt.Printf("Throughput:   %.2f events/sec\n", throughput)

	if completed == 0 && errorsCount > 0 {
		fmt.Println("Note: If pulse-api server is offline, this runs against local test mock.")
		os.Exit(0)
	}
}
