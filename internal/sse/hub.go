// Package sse implements cursor-resumable SSE streaming with Last-Event-ID replay
// and JSON heartbeat frames for iazio-harness-api and iazio-web (ADR-0118).
package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Event is one typed harness execution event with monotonic sequence ordering per job.
type Event struct {
	Seq       int64     `json:"seq"`
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	JobID     string    `json:"job_id"`
	Data      any       `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// HeartbeatFrame represents the schema defined in ai/contracts/json-heartbeat.schema.json.
type HeartbeatFrame struct {
	Heartbeat HeartbeatData `json:"heartbeat"`
}

// HeartbeatData holds progress, stage, and token metrics.
type HeartbeatData struct {
	Phase        string  `json:"phase"`
	Completed    int     `json:"completed"`
	Total        int     `json:"total"`
	Running      int     `json:"running"`
	ElapsedS     float64 `json:"elapsed_s"`
	NextPollS    float64 `json:"next_poll_s"`
	CheckpointID string  `json:"checkpoint_id,omitempty"`
	Tokens       int     `json:"tokens,omitempty"`
}

// Hub manages event queues and active SSE subscriptions per job.
type Hub struct {
	mu                sync.RWMutex
	events            map[string][]Event
	seqs              map[string]int64
	subscribers       map[string][]chan Event
	HeartbeatInterval time.Duration
	Now               func() time.Time
}

// NewHub constructs an in-memory event hub for resumable streaming.
func NewHub() *Hub {
	return &Hub{
		events:            make(map[string][]Event),
		seqs:              make(map[string]int64),
		subscribers:       make(map[string][]chan Event),
		HeartbeatInterval: 5 * time.Second,
		Now:               time.Now,
	}
}

// Publish persists an event with the next monotonic sequence ID and broadcasts it to live subscribers.
func (h *Hub) Publish(jobID, eventType string, data any) Event {
	h.mu.Lock()
	defer h.mu.Unlock()

	seq := h.seqs[jobID] + 1
	h.seqs[jobID] = seq

	now := time.Now().UTC()
	if h.Now != nil {
		now = h.Now().UTC()
	}

	ev := Event{
		Seq:       seq,
		ID:        strconv.FormatInt(seq, 10),
		Type:      eventType,
		JobID:     jobID,
		Data:      data,
		Timestamp: now,
	}

	h.events[jobID] = append(h.events[jobID], ev)

	// Fan out to active subscribers
	for _, ch := range h.subscribers[jobID] {
		select {
		case ch <- ev:
		default:
			// Non-blocking drop if channel buffer is saturated
		}
	}

	return ev
}

// Subscribe returns historical events strictly after afterSeq and a channel for live frames.
func (h *Hub) Subscribe(jobID string, afterSeq int64) ([]Event, <-chan Event, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var backlog []Event
	all := h.events[jobID]
	for _, ev := range all {
		if ev.Seq > afterSeq {
			backlog = append(backlog, ev)
		}
	}

	ch := make(chan Event, 128)
	h.subscribers[jobID] = append(h.subscribers[jobID], ch)

	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		subs := h.subscribers[jobID]
		for i, s := range subs {
			if s == ch {
				h.subscribers[jobID] = append(subs[:i], subs[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return backlog, ch, cancel
}

// ParseAfterSeq extracts the requested starting sequence from Last-Event-ID header or query parameters.
func ParseAfterSeq(r *http.Request) int64 {
	lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if lastEventID != "" {
		if n, err := strconv.ParseInt(lastEventID, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	if querySeq := r.URL.Query().Get("after_seq"); querySeq != "" {
		if n, err := strconv.ParseInt(querySeq, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	if queryAfter := r.URL.Query().Get("after"); queryAfter != "" {
		if n, err := strconv.ParseInt(queryAfter, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return 0
}

// ServeHTTP handles the SSE streaming endpoint GET /v1/jobs/{job_id}/events.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request, jobID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Send an initial ping comment to establish reverse proxy tunnels
	_, _ = fmt.Fprint(w, ": ping\n\n")
	flusher.Flush()

	afterSeq := ParseAfterSeq(r)
	backlog, liveCh, cancel := h.Subscribe(jobID, afterSeq)
	defer cancel()

	// 1. Replay backlog frames (missed events after afterSeq)
	for _, ev := range backlog {
		writeSSEEvent(w, ev)
	}
	flusher.Flush()

	// 2. Stream live frames and 5s heartbeat ticks
	hbInterval := h.HeartbeatInterval
	if hbInterval <= 0 {
		hbInterval = 5 * time.Second
	}
	hbTicker := time.NewTicker(hbInterval)
	defer hbTicker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-r.Context().Done():
			return

		case ev, ok := <-liveCh:
			if !ok {
				return
			}
			writeSSEEvent(w, ev)
			flusher.Flush()

		case <-hbTicker.C:
			elapsed := time.Since(startTime).Seconds()
			hb := HeartbeatFrame{
				Heartbeat: HeartbeatData{
					Phase:     "monitoring",
					Completed: 1,
					Total:     1,
					Running:   1,
					ElapsedS:  elapsed,
					NextPollS: hbInterval.Seconds(),
				},
			}
			hbEv := Event{
				Seq:       0, // Heartbeats carry standard ping without consuming monotonic event seq
				Type:      "heartbeat",
				JobID:     jobID,
				Data:      hb,
				Timestamp: time.Now().UTC(),
			}
			writeSSEEvent(w, hbEv)
			flusher.Flush()
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, ev Event) {
	dataBytes, err := json.Marshal(ev.Data)
	if err != nil {
		dataBytes = []byte(`{}`)
	}
	if ev.Seq > 0 {
		_, _ = fmt.Fprintf(w, "id: %d\n", ev.Seq)
	}
	if ev.Type != "" {
		_, _ = fmt.Fprintf(w, "event: %s\n", ev.Type)
	}
	_, _ = fmt.Fprintf(w, "data: %s\n\n", string(dataBytes))
}
