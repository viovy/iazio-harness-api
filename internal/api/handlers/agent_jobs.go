package handlers

import (
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
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
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

	path := r.URL.Path
	prefix := "/api/v1/agent/jobs/"
	suffix := "/complete"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		writeJSONError(w, http.StatusBadRequest, "invalid complete path")
		return
	}

	jobID := strings.TrimPrefix(path, prefix)
	jobID = strings.TrimSuffix(jobID, suffix)
	jobID = strings.TrimSpace(jobID)

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

	err := h.queue.Complete(r.Context(), jobID, req.Result, req.Error)
	if err != nil {
		if err == dispatch.ErrJobNotFound {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}
		if err == dispatch.ErrJobAlreadyCompleted {
			writeJSONError(w, http.StatusConflict, "job already completed")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
