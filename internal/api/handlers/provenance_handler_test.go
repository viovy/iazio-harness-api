package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/api"
	"github.com/viovy/iazio-harness-api/internal/api/handlers"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/httpserver"
)

func TestConversationJobProvenance_EndpointsAndEngineFallback(t *testing.T) {
	ctx := context.Background()
	q := dispatch.NewInMemoryQueue()
	eng := control.NewEngine(time.Now)

	// 1. Seed root ASE orchestrator job in Queue
	rootPayload, _ := json.Marshal(map[string]any{
		"story_id":     "STORY-GOV-5200",
		"prompt":       "/autonomous-story-execution STORY-GOV-5200",
		"target_agent": "agy",
		"model":        "gemini-2.5-pro",
	})
	rootJob, err := q.Enqueue(ctx, &dispatch.Job{
		Type:                 dispatch.JobTypeStoryExecute,
		Repo:                 "iazio-harness-api",
		WorktreePath:         "/home/romeo/work/workspace/iazio-harness-api",
		HostID:               "pc1",
		TargetAgent:          "agy",
		StoryID:              "STORY-GOV-5200",
		NormalizedPrompt:     "/autonomous-story-execution STORY-GOV-5200",
		EffectiveAction:      "/autonomous-story-execution",
		ConversationID:       "conv-root-ase-1",
		ActiveConversationID: "conv-root-ase-1",
		ConversationIDs:      []string{"conv-root-ase-1"},
		Payload:              rootPayload,
	})
	if err != nil {
		t.Fatalf("Enqueue rootJob: %v", err)
	}

	// 2. Seed child Phase 4 review session in Queue (depending on rootJob, inheriting story_id)
	reviewPayload, _ := json.Marshal(map[string]any{
		"parent_job_id":    rootJob.ID,
		"action":           "review-scheme",
		"role":             "adversarial-reviewer",
		"prompt":           "Execute Phase 4 review-scheme adversarial-reviewer and output docs/reviews/2026-11-08-STORY-GOV-5200-iter-1.md",
		"llm_model":        "deepseek-reasoner",
		"review_iteration": 1,
		"conversation_ids": []string{"conv-phase4-review-1"},
	})
	childReviewJob, err := q.Enqueue(ctx, &dispatch.Job{
		Type:           dispatch.JobTypePromptExecute,
		Repo:           "iazio-harness-api",
		TargetAgent:    "opencode",
		DependsOnJobID: rootJob.ID,
		Payload:        reviewPayload,
	})
	if err != nil {
		t.Fatalf("Enqueue childReviewJob: %v", err)
	}

	// 3. Seed historical root job + secondary Phase 4 review conversation in control.Engine history
	eng.SetHistory("debian\x00/home/romeo/work/workspace", []control.HistoryRow{
		{
			JobID:                 "job-hist-900",
			ScheduleID:            "sch-900",
			StoryID:               "STORY-GOV-5199",
			PromptTitle:           "Execute story STORY-GOV-5199: Telemetry lineage",
			Engine:                "claude-code",
			Status:                "COMPLETED",
			Model:                 "claude-sonnet-4-20250514",
			InitialConversationID: "conv-hist-root-900",
			ActiveConversationID:  "conv-hist-root-900",
			ConversationIDs:       []string{"conv-hist-root-900", "conv-hist-review-900"},
			CreatedAt:             time.Now().Add(-10 * time.Minute),
			ConversationLineage: []control.ConversationLineage{
				{
					ConversationID: "conv-hist-root-900",
					Role:           "initial_active",
					ASEPhase:       "PHASE_2_TDD",
				},
				{
					ConversationID: "conv-hist-review-900",
					Role:           "secondary_review",
					ASEPhase:       "PHASE_4_REVIEW_SCHEME",
					Reason:         "spawned adversarial-reviewer for docs/reviews/ validation",
				},
			},
		},
	})

	srv := &httpserver.Server{
		Engine: eng,
		Queue:  q,
	}
	handler := srv.Handler()

	// Test A: GET /v1/jobs/by-conversation/{conversation_id} for Root ASE orchestrator job
	reqRoot := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversation/conv-root-ase-1", nil)
	recRoot := httptest.NewRecorder()
	handler.ServeHTTP(recRoot, reqRoot)
	if recRoot.Code != http.StatusOK {
		t.Fatalf("GET /v1/jobs/by-conversation/conv-root-ase-1 status = %d, body = %s", recRoot.Code, recRoot.Body.String())
	}
	var gotRoot handlers.ConversationJobProvenance
	if err := json.Unmarshal(recRoot.Body.Bytes(), &gotRoot); err != nil {
		t.Fatalf("unmarshal root provenance: %v", err)
	}
	if gotRoot.JobID != rootJob.ID || gotRoot.StoryID != "STORY-GOV-5200" || gotRoot.Harness != "agy" || gotRoot.Model != "gemini-2.5-pro" || gotRoot.ModelAuthor != "google" {
		t.Errorf("unexpected root provenance: %+v", gotRoot)
	}
	if gotRoot.IsChildReviewSession || gotRoot.RootJobID != rootJob.ID {
		t.Errorf("expected IsChildReviewSession=false and RootJobID=%s, got %+v", rootJob.ID, gotRoot)
	}

	// Test B: GET /api/v1/jobs/by-conversation/{conversation_id} for Child Phase 4 review session
	reqChild := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/by-conversation/conv-phase4-review-1", nil)
	recChild := httptest.NewRecorder()
	handler.ServeHTTP(recChild, reqChild)
	if recChild.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/jobs/by-conversation/conv-phase4-review-1 status = %d, body = %s", recChild.Code, recChild.Body.String())
	}
	var gotChild handlers.ConversationJobProvenance
	if err := json.Unmarshal(recChild.Body.Bytes(), &gotChild); err != nil {
		t.Fatalf("unmarshal child provenance: %v", err)
	}
	if gotChild.JobID != childReviewJob.ID {
		t.Errorf("child JobID = %q, want %q", gotChild.JobID, childReviewJob.ID)
	}
	if gotChild.ParentJobID != rootJob.ID || gotChild.RootJobID != rootJob.ID {
		t.Errorf("child ParentJobID=%q RootJobID=%q, want both %q", gotChild.ParentJobID, gotChild.RootJobID, rootJob.ID)
	}
	if gotChild.StoryID != "STORY-GOV-5200" {
		t.Errorf("child inherited StoryID = %q, want STORY-GOV-5200", gotChild.StoryID)
	}
	if !gotChild.IsChildReviewSession {
		t.Errorf("expected IsChildReviewSession=true for child review session")
	}
	if gotChild.Harness != "opencode" || gotChild.Model != "deepseek-reasoner" || gotChild.ModelAuthor != "deepseek" {
		t.Errorf("unexpected child harness/model/author: %+v", gotChild)
	}

	// Test C: Fallback lookup from control.Engine history rows (both root and lineage secondary review session)
	reqHistRoot := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversation/conv-hist-root-900", nil)
	recHistRoot := httptest.NewRecorder()
	handler.ServeHTTP(recHistRoot, reqHistRoot)
	if recHistRoot.Code != http.StatusOK {
		t.Fatalf("GET /v1/jobs/by-conversation/conv-hist-root-900 status = %d, body = %s", recHistRoot.Code, recHistRoot.Body.String())
	}
	var gotHistRoot handlers.ConversationJobProvenance
	_ = json.Unmarshal(recHistRoot.Body.Bytes(), &gotHistRoot)
	if gotHistRoot.JobID != "job-hist-900" || gotHistRoot.StoryID != "STORY-GOV-5199" || gotHistRoot.Harness != "claude" || gotHistRoot.ModelAuthor != "anthropic" || gotHistRoot.IsChildReviewSession {
		t.Errorf("unexpected engine history root provenance: %+v", gotHistRoot)
	}

	reqHistRev := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversation/conv-hist-review-900", nil)
	recHistRev := httptest.NewRecorder()
	handler.ServeHTTP(recHistRev, reqHistRev)
	if recHistRev.Code != http.StatusOK {
		t.Fatalf("GET /v1/jobs/by-conversation/conv-hist-review-900 status = %d, body = %s", recHistRev.Code, recHistRev.Body.String())
	}
	var gotHistRev handlers.ConversationJobProvenance
	_ = json.Unmarshal(recHistRev.Body.Bytes(), &gotHistRev)
	if gotHistRev.JobID != "job-hist-900" || gotHistRev.StoryID != "STORY-GOV-5199" || !gotHistRev.IsChildReviewSession || gotHistRev.ParentJobID != "job-hist-900" || gotHistRev.RootJobID != "job-hist-900" {
		t.Errorf("unexpected engine history review lineage provenance: %+v", gotHistRev)
	}

	// Test D: 404 on unknown conversation ID
	req404 := httptest.NewRequest(http.MethodGet, "/v1/jobs/by-conversation/conv-does-not-exist", nil)
	rec404 := httptest.NewRecorder()
	handler.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for unknown conversation ID, got %d", rec404.Code)
	}

	// Test E: Batch lookup POST /v1/jobs/by-conversations and POST /api/v1/jobs/by-conversations
	batchReqBody, _ := json.Marshal(map[string]any{
		"conversation_ids": []string{
			"conv-root-ase-1",
			"conv-phase4-review-1",
			"conv-hist-review-900",
			"conv-unknown-999",
		},
	})
	for _, endpoint := range []string{"/v1/jobs/by-conversations", "/api/v1/jobs/by-conversations"} {
		reqBatch := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(batchReqBody))
		recBatch := httptest.NewRecorder()
		handler.ServeHTTP(recBatch, reqBatch)
		if recBatch.Code != http.StatusOK {
			t.Fatalf("POST %s status = %d, body = %s", endpoint, recBatch.Code, recBatch.Body.String())
		}
		var batchResp handlers.BatchConversationProvenanceResponse
		if err := json.Unmarshal(recBatch.Body.Bytes(), &batchResp); err != nil {
			t.Fatalf("unmarshal batch response from %s: %v", endpoint, err)
		}
		if batchResp.Count != 3 || len(batchResp.Results) != 3 || len(batchResp.Items) != 3 {
			t.Fatalf("expected 3 matched conversations in batch response from %s, got count=%d results=%d items=%d",
				endpoint, batchResp.Count, len(batchResp.Results), len(batchResp.Items))
		}
		if !batchResp.Items["conv-phase4-review-1"].IsChildReviewSession {
			t.Errorf("expected conv-phase4-review-1 in items to have IsChildReviewSession=true")
		}
		if batchResp.Items["conv-hist-review-900"].StoryID != "STORY-GOV-5199" {
			t.Errorf("expected conv-hist-review-900 StoryID=STORY-GOV-5199, got %q", batchResp.Items["conv-hist-review-900"].StoryID)
		}
	}

	// Test F: Verify api.NewRouter also serves both GET and POST provenance endpoints
	router := api.NewRouter(api.RouterConfig{
		Queue:  q,
		Engine: eng,
	})
	recRouter := httptest.NewRecorder()
	router.ServeHTTP(recRouter, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/by-conversation/conv-phase4-review-1", nil))
	if recRouter.Code != http.StatusOK {
		t.Fatalf("api.NewRouter GET /api/v1/jobs/by-conversation/conv-phase4-review-1 status = %d", recRouter.Code)
	}
}
