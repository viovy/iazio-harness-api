package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

// ClaimJobRequest is sent by an iazio-agent daemon to claim pending jobs.
type ClaimJobRequest struct {
	HostID   string   `json:"host_id,omitempty"`
	JobTypes []string `json:"job_types,omitempty"`
}

// CompleteJobRequest is sent by an iazio-agent daemon to report job outcome.
type CompleteJobRequest struct {
	FencingToken          int64           `json:"fencing_token,omitempty"`
	Result                json.RawMessage `json:"result,omitempty"`
	Error                 string          `json:"error,omitempty"`
	FinalTurnCount        int             `json:"final_turn_count,omitempty"`
	FinalTranscriptSHA256 string          `json:"final_transcript_sha256,omitempty"`
}

// HeartbeatJobRequest is sent by an iazio-agent daemon to extend its execution lease.
type HeartbeatJobRequest struct {
	FencingToken int64  `json:"fencing_token"`
	LeaseToken   string `json:"lease_token,omitempty"`
	StoryID      string `json:"story_id,omitempty"`
}

// AgentJobsHandler manages agent job claims and completion reports.
type AgentJobsHandler struct {
	queue dispatch.Queue
}

// NewAgentJobsHandler constructs a new AgentJobsHandler.
func NewAgentJobsHandler(queue dispatch.Queue) *AgentJobsHandler {
	return &AgentJobsHandler{queue: queue}
}

// ClaimHandler handles POST /api/v1/agent/jobs/claim.
func (h *AgentJobsHandler) ClaimHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req ClaimJobRequest
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}

	if h.queue == nil {
		writeJSONError(w, http.StatusInternalServerError, "dispatch queue not available")
		return
	}

	job, err := h.queue.Claim(r.Context(), req.HostID, req.JobTypes)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if job == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(job)
}

// CompleteHandler handles POST /api/v1/agent/jobs/{job_id}/complete.
func (h *AgentJobsHandler) CompleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := r.PathValue("id")
	if jobID == "" {
		path := r.URL.Path
		prefix := "/api/v1/agent/jobs/"
		if !strings.HasPrefix(path, prefix) {
			prefix = "/v1/agent/jobs/"
		}
		suffix := "/complete"
		if strings.HasPrefix(path, prefix) && strings.HasSuffix(path, suffix) {
			jobID = strings.TrimPrefix(path, prefix)
			jobID = strings.TrimSuffix(jobID, suffix)
			jobID = strings.TrimSpace(jobID)
		}
	}

	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing job_id")
		return
	}

	var req CompleteJobRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if h.queue == nil {
		writeJSONError(w, http.StatusInternalServerError, "dispatch queue not available")
		return
	}

	err := h.queue.CompleteWithFencing(r.Context(), jobID, req.FencingToken, req.Result, req.Error)
	if err != nil {
		if err == dispatch.ErrJobNotFound {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}
		if err == dispatch.ErrJobAlreadyCompleted {
			writeJSONError(w, http.StatusConflict, "job already completed")
			return
		}
		if err == dispatch.ErrLeaseFenced {
			writeJSONError(w, http.StatusConflict, "LEASE_FENCED: stale fencing token presented")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if req.FinalTurnCount > 0 || req.FinalTranscriptSHA256 != "" {
		h.queue.SetFinalHarvest(jobID, req.FinalTurnCount, req.FinalTranscriptSHA256)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// HeartbeatHandler handles POST /api/v1/agent/jobs/{id}/heartbeat.
func (h *AgentJobsHandler) HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := r.PathValue("id")
	if jobID == "" {
		path := r.URL.Path
		prefix := "/api/v1/agent/jobs/"
		if !strings.HasPrefix(path, prefix) {
			prefix = "/v1/agent/jobs/"
		}
		suffix := "/heartbeat"
		if strings.HasPrefix(path, prefix) && strings.HasSuffix(path, suffix) {
			jobID = strings.TrimPrefix(path, prefix)
			jobID = strings.TrimSuffix(jobID, suffix)
			jobID = strings.TrimSpace(jobID)
		}
	}

	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing job_id")
		return
	}

	var req HeartbeatJobRequest
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}

	if h.queue == nil {
		writeJSONError(w, http.StatusInternalServerError, "dispatch queue not available")
		return
	}

	var job *dispatch.Job
	var err error
	if hbWithStory, ok := h.queue.(interface {
		HeartbeatWithStory(ctx context.Context, jobID string, fencingToken int64, storyID string) (*dispatch.Job, error)
	}); ok && strings.TrimSpace(req.StoryID) != "" {
		job, err = hbWithStory.HeartbeatWithStory(r.Context(), jobID, req.FencingToken, strings.TrimSpace(req.StoryID))
	} else {
		job, err = h.queue.Heartbeat(r.Context(), jobID, req.FencingToken)
	}
	if err != nil {
		if err == dispatch.ErrJobNotFound {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}
		if err == dispatch.ErrLeaseFenced {
			writeJSONError(w, http.StatusConflict, "LEASE_FENCED: stale fencing token presented")
			return
		}
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":           "ok",
		"job_id":           job.ID,
		"fencing_token":    job.FencingToken,
		"lease_expires_at": job.LeaseExpiresAt,
	})
}
