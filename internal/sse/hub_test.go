package sse

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestResumableStreamLastEventID verifies AC-3:
// Disconnecting from GET /v1/jobs/{job_id}/events at seq=4 and reconnecting with Last-Event-ID: 4
// replays events 5..N in exact order with zero dropped or duplicated frames.
func TestResumableStreamLastEventID(t *testing.T) {
	hub := NewHub()
	hub.HeartbeatInterval = 50 * time.Millisecond // fast ticker for testing

	jobID := "job-replay-test-01"

	// Publish events 1 through 4
	for i := 1; i <= 4; i++ {
		hub.Publish(jobID, "stage_transition", map[string]any{
			"stage": fmt.Sprintf("stage_%d", i),
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeHTTP(w, r, jobID)
	}))
	defer server.Close()

	// 1. Initial connection: receive events 1..4, then disconnect
	req1, _ := http.NewRequest("GET", server.URL, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1 = req1.WithContext(ctx1)

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("first connection failed: %v", err)
	}
	defer resp1.Body.Close()

	reader1 := bufio.NewReader(resp1.Body)
	receivedFirst := readEventsUntil(t, reader1, 4, 2*time.Second)
	cancel1() // Simulate disconnect after seq=4
	_ = resp1.Body.Close()

	if len(receivedFirst) != 4 {
		t.Fatalf("expected 4 events on first connection, got %d: %v", len(receivedFirst), receivedFirst)
	}
	for i, id := range receivedFirst {
		expected := int64(i + 1)
		if id != expected {
			t.Fatalf("expected event seq %d, got %d", expected, id)
		}
	}

	// 2. While client is disconnected, publish events 5, 6, 7, 8
	for i := 5; i <= 8; i++ {
		hub.Publish(jobID, "stage_transition", map[string]any{
			"stage": fmt.Sprintf("stage_%d", i),
		})
	}

	// 3. Reconnect with Last-Event-ID: 4
	req2, _ := http.NewRequest("GET", server.URL, nil)
	req2.Header.Set("Last-Event-ID", "4")
	ctx2, cancel2 := context.WithCancel(context.Background())
	req2 = req2.WithContext(ctx2)

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("reconnection failed: %v", err)
	}
	defer resp2.Body.Close()

	reader2 := bufio.NewReader(resp2.Body)
	receivedReplay := readEventsUntil(t, reader2, 4, 2*time.Second)
	cancel2()
	_ = resp2.Body.Close()

	// 4. Verify replay is exactly 5, 6, 7, 8 with no duplicates and no drops
	expectedReplay := []int64{5, 6, 7, 8}
	if len(receivedReplay) != len(expectedReplay) {
		t.Fatalf("expected %d replayed events, got %d: %v", len(expectedReplay), len(receivedReplay), receivedReplay)
	}
	for i, id := range receivedReplay {
		if id != expectedReplay[i] {
			t.Fatalf("expected replayed event %d at index %d, got %d", expectedReplay[i], i, id)
		}
	}

	// 5. Verify query param fallback ?after_seq=4
	req3, _ := http.NewRequest("GET", server.URL+"?after_seq=4", nil)
	ctx3, cancel3 := context.WithCancel(context.Background())
	req3 = req3.WithContext(ctx3)

	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("query fallback request failed: %v", err)
	}
	defer resp3.Body.Close()

	reader3 := bufio.NewReader(resp3.Body)
	receivedQuery := readEventsUntil(t, reader3, 4, 2*time.Second)
	cancel3()
	_ = resp3.Body.Close()

	if len(receivedQuery) != 4 || receivedQuery[0] != 5 || receivedQuery[3] != 8 {
		t.Fatalf("expected events 5..8 via query param, got %v", receivedQuery)
	}
}

func readEventsUntil(t *testing.T, r *bufio.Reader, count int, timeout time.Duration) []int64 {
	var seqs []int64
	deadline := time.Now().Add(timeout)

	for len(seqs) < count && time.Now().Before(deadline) {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "id:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			if seq, err := strconv.ParseInt(val, 10, 64); err == nil && seq > 0 {
				seqs = append(seqs, seq)
			}
		}
	}
	return seqs
}

func TestConcurrentPublishSubscribeRace(t *testing.T) {
	hub := NewHub()
	jobID := "job-concurrent-race"

	var wg sync.WaitGroup
	// 5 concurrent publishers
	for p := 0; p < 5; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				hub.Publish(jobID, "token_telemetry", map[string]any{
					"worker": pid,
					"tokens": i * 10,
				})
			}
		}(p)
	}

	// 5 concurrent subscribers reading backlogs
	for s := 0; s < 5; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				backlog, _, cancel := hub.Subscribe(jobID, int64(i*5))
				_ = backlog
				cancel()
			}
		}()
	}

	wg.Wait()
}
