package handlers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

// CreatePromptRequest is the payload accepted by POST /api/v1/prompts.
type CreatePromptRequest struct {
	Repo        string `json:"repo"`
	Title       string `json:"title"`
	Prompt      string `json:"prompt"`
	TargetAgent string `json:"target_agent,omitempty"`
	Enqueue     bool   `json:"enqueue,omitempty"`
}

// CreatePromptResponse is the response emitted by POST /api/v1/prompts.
type CreatePromptResponse struct {
	PromptID         string `json:"prompt_id"`
	Repo             string `json:"repo"`
	HarnessJobID     string `json:"harness_job_id,omitempty"`
	Status           string `json:"status"`
	CreatedAt        string `json:"created_at"`
	IdempotentReplay bool   `json:"idempotent_replay"`
}

// PromptsHandler handles prompt creation and dispatch.
type PromptsHandler struct {
	store       codebase.Store
	queue       dispatch.Queue
	idempotency *IdempotencyStore
}

// NewPromptsHandler constructs a new PromptsHandler.
func NewPromptsHandler(store codebase.Store, queue dispatch.Queue, idempotency *IdempotencyStore) *PromptsHandler {
	if idempotency == nil {
		idempotency = NewIdempotencyStore(24 * time.Hour)
	}
	return &PromptsHandler{
		store:       store,
		queue:       queue,
		idempotency: idempotency,
	}
}

func (h *PromptsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	defer r.Body.Close()

	payloadHash := ComputePayloadHash(bodyBytes)
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))

	if idempotencyKey != "" {
		cachedResp, _, replay, conflict := h.idempotency.Check(idempotencyKey, payloadHash)
		if conflict {
			writeJSONError(w, http.StatusConflict, "idempotency key conflict: payload mismatch")
			return
		}
		if replay && cachedResp != nil {
			var replayObj CreatePromptResponse
			if err := json.Unmarshal(cachedResp, &replayObj); err == nil {
				replayObj.IdempotentReplay = true
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(replayObj)
				return
			}
		}
	}

	var req CreatePromptRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "malformed JSON request body")
		return
	}

	req.Repo = strings.TrimSpace(req.Repo)
	req.Title = strings.TrimSpace(req.Title)
	req.Prompt = strings.TrimSpace(req.Prompt)

	if req.Repo == "" {
		writeJSONError(w, http.StatusUnprocessableEntity, "repo must not be empty")
		return
	}
	if req.Title == "" {
		writeJSONError(w, http.StatusUnprocessableEntity, "title must not be empty")
		return
	}
	if req.Prompt == "" {
		writeJSONError(w, http.StatusUnprocessableEntity, "prompt must not be empty")
		return
	}

	// Verify repo exists in the registered fleet repository table
	if req.Repo != "." {
		_, found, err := h.store.GetRepository(r.Context(), req.Repo)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to lookup repository")
			return
		}
		if !found {
			writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf("unknown repository %q", req.Repo))
			return
		}
	}

	promptID := generateUUIDv4()
	status := "created"
	harnessJobID := ""

	if req.Enqueue && h.queue != nil {
		job, err := h.queue.Enqueue(r.Context(), &dispatch.Job{
			Type:        dispatch.JobTypePromptExecute,
			Repo:        req.Repo,
			TargetAgent: req.TargetAgent,
			Payload:     bodyBytes,
		})
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue prompt: %v", err))
			return
		}
		status = "queued"
		harnessJobID = job.ID
	}

	resp := CreatePromptResponse{
		PromptID:         promptID,
		Repo:             req.Repo,
		HarnessJobID:     harnessJobID,
		Status:           status,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		IdempotentReplay: false,
	}

	respBytes, _ := json.Marshal(resp)
	if idempotencyKey != "" {
		h.idempotency.Store(idempotencyKey, payloadHash, http.StatusCreated, respBytes)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(respBytes)
}

func generateUUIDv4() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
