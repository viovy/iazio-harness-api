package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func TestJobLifecycleHandler_ConversationProvenanceEndpoints(t *testing.T) {
	ctx := context.Background()
	q := dispatch.NewInMemoryQueue()
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	eng := control.NewEngine(func() time.Time { return now })
	h := NewJobLifecycleHandler(q)
	h.SetEngine(eng)
	h.SetWebhookHTTPClient(http.DefaultClient)

	var nilHandler *JobLifecycleHandler
	if _, ok := nilHandler.ResolveConversationProvenance(ctx, "c1"); ok {
		t.Fatal("expected false on nil handler")
	}

	parentJob, err := q.Enqueue(ctx, &dispatch.Job{
		ID:                   "job-prov-parent",
		Type:                 dispatch.JobTypePromptExecute,
		Repo:                 "iazio-harvester-api",
		TargetHost:           "core-node",
		HostID:               "core-node",
		TargetAgent:          "opencode",
		StoryID:              "STORY-GOV-5200",
		WorktreePath:         "/work/iazio-harvester-api",
		ActiveConversationID: "conv-prov-parent",
	})
	if err != nil {
		t.Fatalf("enqueue parent: %v", err)
	}

	_, err = q.Enqueue(ctx, &dispatch.Job{
		ID:                   "job-prov-child",
		Type:                 dispatch.JobTypePromptExecute,
		DependsOnJobID:       parentJob.ID,
		TargetAgent:          "claude",
		NormalizedPrompt:     "Phase 4 review-scheme run STORY-GOV-5200",
		ActiveConversationID: "conv-prov-child",
	})
	if err != nil {
		t.Fatalf("enqueue child: %v", err)
	}

	// Single GET lookup
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversation/conv-prov-child", nil)
	rr := httptest.NewRecorder()
	h.GetJobByConversation(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var prov ConversationJobProvenance
	if err := json.NewDecoder(rr.Body).Decode(&prov); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !prov.IsChildReviewSession || prov.ParentJobID != parentJob.ID || prov.StoryID != "STORY-GOV-5200" || prov.HostID != "core-node" {
		t.Fatalf("unexpected provenance: %+v", prov)
	}

	// Method not allowed, missing ID, not found
	reqPost := httptest.NewRequest(http.MethodPost, "/v1/jobs/by-conversation/conv-prov-child", nil)
	rrPost := httptest.NewRecorder()
	h.GetJobByConversation(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rrPost.Code)
	}

	reqEmpty := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversation/", nil)
	rrEmpty := httptest.NewRecorder()
	h.GetJobByConversation(rrEmpty, reqEmpty)
	if rrEmpty.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rrEmpty.Code)
	}

	reqMiss := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversation/conv-missing", nil)
	rrMiss := httptest.NewRecorder()
	h.GetJobByConversation(rrMiss, reqMiss)
	if rrMiss.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rrMiss.Code)
	}

	// Batch POST lookup
	batchBody, _ := json.Marshal(BatchConversationProvenanceRequest{
		ConversationIDs: []string{"conv-prov-parent", "conv-prov-child", "conv-prov-parent", "unknown"},
	})
	reqBatch := httptest.NewRequest(http.MethodPost, "/v1/jobs/by-conversations", bytes.NewReader(batchBody))
	rrBatch := httptest.NewRecorder()
	h.GetJobsByConversations(rrBatch, reqBatch)
	if rrBatch.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rrBatch.Code, rrBatch.Body.String())
	}
	var batchResp BatchConversationProvenanceResponse
	if err := json.NewDecoder(rrBatch.Body).Decode(&batchResp); err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if batchResp.Count != 2 {
		t.Fatalf("expected 2 results, got %d", batchResp.Count)
	}

	// Method not allowed & invalid JSON on batch
	reqBatchGet := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversations", nil)
	rrBatchGet := httptest.NewRecorder()
	h.GetJobsByConversations(rrBatchGet, reqBatchGet)
	if rrBatchGet.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rrBatchGet.Code)
	}

	reqBatchBad := httptest.NewRequest(http.MethodPost, "/v1/jobs/by-conversations", bytes.NewBufferString("{bad"))
	rrBatchBad := httptest.NewRecorder()
	h.GetJobsByConversations(rrBatchBad, reqBatchBad)
	if rrBatchBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rrBatchBad.Code)
	}
}
